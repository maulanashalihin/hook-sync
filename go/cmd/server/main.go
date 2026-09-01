package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"log"
	"time"

	"hook-sync/hooksync"
	"hook-sync/trigger"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	var (
		id        = flag.String("id", "", "node ID (e.g. node1)")
		dbPath    = flag.String("db", "", "SQLite DB path")
		listen    = flag.String("listen", "", "HTTP listen address (e.g. :9001)")
		batchSize = flag.Int("batch-size", 10000, "max changes per ship batch")
		batchMs   = flag.Int("batch-ms", 50, "batch ship interval in milliseconds")
		noTrigger = flag.Bool("no-trigger", false, "disable triggers and sync (baseline for bench)")
		peerURL   = flag.String("peer", "", "peer URL (e.g. http://localhost:9002)")
	)
	flag.Parse()

	if *id == "" || *dbPath == "" || *listen == "" {
		log.Fatal("usage: hook-sync -id node1 -db node1.db -listen :9001 -peer http://localhost:9002")
	}

	nodeID := *id

	dsn := *dbPath + "?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)

	// Create items table (application schema)
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS items (
		id TEXT PRIMARY KEY, name TEXT, value INTEGER,
		created_at INTEGER, updated_at INTEGER, node_id TEXT
	)`)
	if err != nil {
		log.Fatalf("create items table: %v", err)
	}

	// Attach trigger-based sync (unless baseline mode)
	var mgr *trigger.Manager
	if !*noTrigger {
		var peers []string
		if *peerURL != "" {
			peers = append(peers, *peerURL)
		}
		mgr, err = trigger.Attach(db, hooksync.Config{
			ID:        nodeID,
			Peers:     peers,
			BatchMs:   *batchMs,
			BatchSize: *batchSize,
		}, []string{"items"})
		if err != nil {
			log.Fatalf("trigger attach: %v", err)
		}
		defer mgr.Stop()
	}

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
		BodyLimit:             16 * 1024 * 1024,
	})

	// POST /sync — receive changes from peer (ACK-based)
	if mgr != nil {
		app.Post("/sync", func(c *fiber.Ctx) error {
			var req hooksync.SyncRequest
			if err := json.Unmarshal(c.Body(), &req); err != nil {
				return c.Status(400).JSON(fiber.Map{"error": err.Error()})
			}
			applied := mgr.ApplyChanges(req.Changes)
			return c.JSON(fiber.Map{"applied": applied, "ack": req.BatchID})
		})
	}

	// GET /api/items — list all items
	app.Get("/api/items", func(c *fiber.Ctx) error {
		rows, err := db.Query("SELECT id, name, value, created_at, updated_at, node_id FROM items ORDER BY created_at DESC LIMIT 100")
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		defer rows.Close()

		items := []map[string]any{}
		for rows.Next() {
			var id string
			var createdAt, updatedAt int64
			var namePtr, nodeIDPtr sql.NullString
			var valuePtr sql.NullInt64
			rows.Scan(&id, &namePtr, &valuePtr, &createdAt, &updatedAt, &nodeIDPtr)
			items = append(items, map[string]any{
				"id":         id,
				"name":       namePtr.String,
				"value":      valuePtr.Int64,
				"created_at": createdAt,
				"updated_at": updatedAt,
				"node_id":    nodeIDPtr.String,
			})
		}
		return c.JSON(items)
	})

	// GET /api/items/:id — get single item
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

	// POST /api/items — create item (local write)
	app.Post("/api/items", func(c *fiber.Ctx) error {
		var body struct {
			Name  string `json:"name"`
			Value int    `json:"value"`
		}
		if err := c.BodyParser(&body); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		}

		id, _ := uuid.NewV7()
		idStr := id.String()
		now := time.Now().UnixMilli()
		_, err := db.Exec(
			"INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
			idStr, body.Name, body.Value, now, now, nodeID,
		)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(map[string]any{
			"id": idStr, "name": body.Name, "value": body.Value,
			"created_at": now, "node_id": nodeID,
		})
	})

	// POST /api/items/batch — create multiple items in one transaction
	app.Post("/api/items/batch", func(c *fiber.Ctx) error {
		var items []struct {
			Name  string `json:"name"`
			Value int    `json:"value"`
		}
		if err := c.BodyParser(&items); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		}

		tx, err := db.Begin()
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		now := time.Now().UnixMilli()
		for _, item := range items {
			id, _ := uuid.NewV7()
			_, err := tx.Exec(
				"INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
				id.String(), item.Name, item.Value, now, now, nodeID,
			)
			if err != nil {
				tx.Rollback()
				return c.Status(500).JSON(fiber.Map{"error": err.Error()})
			}
		}
		if err := tx.Commit(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"created": len(items)})
	})

	// PUT /api/items/:id — update item (local write)
	app.Put("/api/items/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")
		var body struct {
			Name  string `json:"name"`
			Value int    `json:"value"`
		}
		if err := c.BodyParser(&body); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		}
		now := time.Now().UnixMilli()
		_, err := db.Exec(
			"UPDATE items SET name=?, value=?, updated_at=? WHERE id=?",
			body.Name, body.Value, now, id,
		)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(map[string]any{
			"id": id, "name": body.Name, "value": body.Value, "updated_at": now,
		})
	})

	// DELETE /api/items/:id — delete item (local write)
	app.Delete("/api/items/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")
		_, err := db.Exec("DELETE FROM items WHERE id=?", id)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(map[string]any{"deleted": id})
	})

	// GET /health
	app.Get("/health", func(c *fiber.Ctx) error {
		if mgr != nil {
			h := mgr.Health()
			return c.JSON(h)
		}
		// Baseline mode (no trigger)
		var count int
		db.QueryRow("SELECT COUNT(*) FROM items").Scan(&count)
		return c.JSON(fiber.Map{
			"ok": true, "node_id": nodeID, "item_count": count,
			"pending_changes": 0, "dead_letter": 0,
		})
	})

	log.Printf("[%s] listening on %s, peer=%s", *id, *listen, *peerURL)
	log.Fatal(app.Listen(*listen))
}
