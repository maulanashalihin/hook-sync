//go:build sqlite_preupdate_hook

// cmd/hookserver: demo binary using the hook/ library (preupdate_hook capture).
// Pebble-backed by default; --in-memory switches to in-memory slice (no persistence).
//
// Build: go build -tags sqlite_preupdate_hook -o hook-sync-hookserver ./cmd/hookserver
// Usage: hook-sync-hookserver -id node1 -db node1.db -listen :9001 -peer http://localhost:9002
//        hook-sync-hookserver -id node1 -db node1.db -listen :9001 -peer http://localhost:9002 -in-memory

package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"log"
	"time"

	"hook-sync/hook"
	"hook-sync/hooksync"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

func main() {
	id := flag.String("id", "", "node ID")
	dbPath := flag.String("db", "", "SQLite DB path")
	listen := flag.String("listen", "", "HTTP listen address")
	batchSize := flag.Int("batch-size", 10000, "max changes per ship")
	batchMs := flag.Int("batch-ms", 50, "ship interval ms")
	peerURL := flag.String("peer", "", "peer URL")
	inMemory := flag.Bool("in-memory", false, "use in-memory capture (no Pebble, no persistence)")
	flag.Parse()

	if *id == "" || *dbPath == "" || *listen == "" {
		log.Fatal("usage: hook-sync-hookserver -id node1 -db node1.db -listen :9001 -peer http://localhost:9002 [-in-memory]")
	}

	nodeID := *id

	// Create items table first (hook.Open introspects it for column names)
	dsn := *dbPath + "?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS items (
		id TEXT PRIMARY KEY, name TEXT, value INTEGER,
		created_at INTEGER, updated_at INTEGER, node_id TEXT
	)`)
	if err != nil {
		log.Fatalf("create items table: %v", err)
	}
	db.Close()

	var peers []string
	if *peerURL != "" {
		peers = append(peers, *peerURL)
	}

	cfg := hooksync.Config{
		ID:        nodeID,
		Peers:     peers,
		BatchMs:   *batchMs,
		BatchSize: *batchSize,
	}

	var mgr *hook.Manager
	if *inMemory {
		mgr, err = hook.OpenInMemory(*dbPath, cfg, []string{"items"})
	} else {
		mgr, err = hook.Open(*dbPath, cfg, []string{"items"})
	}
	if err != nil {
		log.Fatalf("hook open: %v", err)
	}
	defer mgr.Stop()

	db = mgr.DB()

	mode := "hookpebble"
	if *inMemory {
		mode = "hookmem"
	}

	app := fiber.New(fiber.Config{DisableStartupMessage: true, BodyLimit: 16 * 1024 * 1024})

	// POST /sync — receive changes from peer (ACK-based)
	app.Post("/sync", func(c *fiber.Ctx) error {
		var req hooksync.SyncRequest
		json.Unmarshal(c.Body(), &req)
		applied := mgr.ApplyChanges(req.Changes)
		return c.JSON(fiber.Map{"applied": applied, "ack": req.BatchID})
	})

	// GET /api/items/:id
	app.Get("/api/items/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")
		var itemID, name, nodeID string
		var value, createdAt, updatedAt int64
		err := db.QueryRow("SELECT id, name, value, created_at, updated_at, node_id FROM items WHERE id = ?", id).
			Scan(&itemID, &name, &value, &createdAt, &updatedAt, &nodeID)
		if err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "not found"})
		}
		return c.JSON(map[string]any{
			"id": itemID, "name": name, "value": value,
			"created_at": createdAt, "updated_at": updatedAt, "node_id": nodeID,
		})
	})

	// GET /api/items
	app.Get("/api/items", func(c *fiber.Ctx) error {
		rows, _ := db.Query("SELECT id, name, value, created_at, updated_at, node_id FROM items ORDER BY created_at DESC LIMIT 100")
		defer rows.Close()
		items := []map[string]any{}
		for rows.Next() {
			var id string
			var createdAt, updatedAt int64
			var namePtr, nodeIDPtr sql.NullString
			var valuePtr sql.NullInt64
			rows.Scan(&id, &namePtr, &valuePtr, &createdAt, &updatedAt, &nodeIDPtr)
			items = append(items, map[string]any{
				"id": id, "name": namePtr.String, "value": valuePtr.Int64,
				"created_at": createdAt, "updated_at": updatedAt, "node_id": nodeIDPtr.String,
			})
		}
		return c.JSON(items)
	})

	// POST /api/items
	app.Post("/api/items", func(c *fiber.Ctx) error {
		var body struct {
			Name  string `json:"name"`
			Value int    `json:"value"`
		}
		c.BodyParser(&body)
		id, _ := uuid.NewV7()
		now := time.Now().UnixMilli()
		db.Exec("INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
			id.String(), body.Name, body.Value, now, now, nodeID)
		return c.JSON(fiber.Map{"id": id.String(), "name": body.Name, "value": body.Value, "created_at": now, "node_id": nodeID})
	})

	// POST /api/items/batch
	app.Post("/api/items/batch", func(c *fiber.Ctx) error {
		var items []struct {
			Name  string `json:"name"`
			Value int    `json:"value"`
		}
		c.BodyParser(&items)
		tx, _ := db.Begin()
		now := time.Now().UnixMilli()
		for _, item := range items {
			id, _ := uuid.NewV7()
			if _, err := tx.Exec("INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
				id.String(), item.Name, item.Value, now, now, nodeID); err != nil {
				tx.Rollback()
				return c.Status(500).JSON(fiber.Map{"error": err.Error()})
			}
		}
		tx.Commit()
		return c.JSON(fiber.Map{"created": len(items)})
	})

	// PUT /api/items/:id
	app.Put("/api/items/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")
		var body struct {
			Name  string `json:"name"`
			Value int    `json:"value"`
		}
		c.BodyParser(&body)
		now := time.Now().UnixMilli()
		db.Exec("UPDATE items SET name=?, value=?, updated_at=? WHERE id=?", body.Name, body.Value, now, id)
		return c.JSON(fiber.Map{"id": id, "name": body.Name, "value": body.Value, "updated_at": now})
	})

	// DELETE /api/items/:id
	app.Delete("/api/items/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")
		db.Exec("DELETE FROM items WHERE id=?", id)
		return c.JSON(fiber.Map{"deleted": id})
	})

	// GET /health
	app.Get("/health", func(c *fiber.Ctx) error {
		h := mgr.Health()
		return c.JSON(h)
	})

	log.Printf("[%s] %s on %s, peer=%s", nodeID, mode, *listen, *peerURL)
	log.Fatal(app.Listen(*listen))
}
