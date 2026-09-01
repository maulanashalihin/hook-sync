package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"log"
	"strings"
	"time"

	"hook-sync/hooksync"
	"hook-sync/trigger"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

// peerList is a repeatable --peer flag
type peerList []string

func (p *peerList) String() string { return strings.Join(*p, ",") }
func (p *peerList) Set(v string) error {
	*p = append(*p, v)
	return nil
}

func main() {
	var (
		id        = flag.String("id", "", "node ID (e.g. nodeA)")
		dbPath    = flag.String("db", "", "SQLite DB path")
		listen    = flag.String("listen", "", "HTTP listen address (e.g. :9001)")
		batchMs   = flag.Int("batch-ms", 50, "batch ship interval in milliseconds")
		batchSize = flag.Int("batch-size", 10000, "max changes per ship batch")
		peers     peerList
	)
	flag.Var(&peers, "peer", "peer URL (repeatable, e.g. http://localhost:9002)")
	flag.Parse()

	if *id == "" || *dbPath == "" || *listen == "" {
		log.Fatal("usage: hook-sync-mesh-go -id nodeA -db a.db -listen :9001 -peer http://localhost:9002 -peer http://localhost:9003")
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

	// Attach trigger-based sync with multi-peer
	mgr, err := trigger.Attach(db, hooksync.Config{
		ID:        nodeID,
		Peers:     peers,
		BatchMs:   *batchMs,
		BatchSize: *batchSize,
	}, []string{"items"})
	if err != nil {
		log.Fatalf("trigger attach: %v", err)
	}
	defer mgr.Stop()

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
		BodyLimit:             16 * 1024 * 1024,
	})

	// POST /sync — receive changes from peer (ACK-based)
	app.Post("/sync", func(c *fiber.Ctx) error {
		var req hooksync.SyncRequest
		if err := json.Unmarshal(c.Body(), &req); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		}
		applied := mgr.ApplyChanges(req.Changes)
		return c.JSON(fiber.Map{"applied": applied, "ack": req.BatchID})
	})

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

	// GET /health — includes per-peer watermark state
	app.Get("/health", func(c *fiber.Ctx) error {
		h := mgr.Health()

		// Add per-peer watermark state from _peer_state
		rows, err := db.Query("SELECT peer_url, last_acked FROM _peer_state")
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		peerStates := []map[string]any{}
		for rows.Next() {
			var url string
			var acked int64
			rows.Scan(&url, &acked)
			peerStates = append(peerStates, map[string]any{"peer_url": url, "last_acked": acked})
		}
		rows.Close()

		return c.JSON(fiber.Map{
			"ok":              h.OK,
			"node_id":         h.NodeID,
			"item_count":      h.ItemCount,
			"pending_changes": h.PendingChanges,
			"dead_letter":     h.DeadLetter,
			"peers":           peerStates,
		})
	})

	log.Printf("[%s] listening on %s, peers=[%s]", nodeID, *listen, strings.Join(peers, ", "))
	log.Fatal(app.Listen(*listen))
}
