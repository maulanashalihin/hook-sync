package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	sqlite3 "github.com/mattn/go-sqlite3"
)

// Change represents a single row-level change captured by preupdate hook
type Change struct {
	Op    string         `json:"op"`    // INSERT, UPDATE, DELETE
	Table string         `json:"table"`
	Row   map[string]any `json:"row"`   // new row values (for INSERT/UPDATE)
	OldID string         `json:"old_id"` // for DELETE
}

// Node is a single sync node
type Node struct {
	ID       string
	DBPath   string
	Listen   string
	PeerURL  string
	db       *sql.DB
	conn     *sqlite3.SQLiteConn
	changeCh chan Change
	batchInterval time.Duration
}

var syncing bool
var syncMu sync.Mutex

func main() {
	var (
		id      = flag.String("id", "", "node ID (e.g. node1)")
		dbPath  = flag.String("db", "", "SQLite DB path")
		listen  = flag.String("listen", "", "HTTP listen address (e.g. :9001)")
		batchMs = flag.Int("batch-ms", 50, "batch ship interval in milliseconds")
		peerURL = flag.String("peer", "", "peer URL (e.g. http://localhost:9002)")
	)
	flag.Parse()

	if *id == "" || *dbPath == "" || *listen == "" {
		log.Fatal("usage: hook-sync -id node1 -db node1.db -listen :9001 -peer http://localhost:9002")
	}

	node := &Node{
		ID:       *id,
		DBPath:   *dbPath,
		Listen:   *listen,
		batchInterval: time.Duration(*batchMs) * time.Millisecond,
		PeerURL:  *peerURL,
		changeCh: make(chan Change, 10000),
	}

	node.initDB()
	node.setupSchema()
	go node.batchShip()
	node.startHTTP()

	log.Printf("[%s] listening on %s, peer=%s", *id, *listen, *peerURL)
	select {} // block forever
}

func (n *Node) initDB() {
	dsn := n.DBPath + "?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	n.db = db
	db.SetMaxOpenConns(1)

	// Get raw conn for preupdate hook registration
	rawConn, err := db.Conn(context.Background())
	if err != nil {
		log.Fatalf("get conn: %v", err)
	}

	err = rawConn.Raw(func(driverConn any) error {
		conn, ok := driverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return fmt.Errorf("not a sqlite3 conn")
		}
		n.conn = conn
		conn.RegisterPreUpdateHook(func(d sqlite3.SQLitePreUpdateData) {
			n.captureChange(d)
		})
		return nil
	})
	if err != nil {
		log.Fatalf("register hook: %v", err)
	}

	// Release conn back to pool — hook persists on underlying SQLiteConn.
	// With SetMaxOpenConns(1), all subsequent db.Exec uses this same conn.
	rawConn.Close()
}

func (n *Node) setupSchema() {
	_, err := n.db.Exec(`
		CREATE TABLE IF NOT EXISTS items (
			id TEXT PRIMARY KEY,
			name TEXT,
			value INTEGER,
			created_at INTEGER,
			updated_at INTEGER,
			node_id TEXT
		);
	`)
	if err != nil {
		log.Fatalf("setup schema: %v", err)
	}
}

func (n *Node) captureChange(d sqlite3.SQLitePreUpdateData) {
	log.Printf("[%s] HOOK FIRED: op=%d table=%s", n.ID, d.Op, d.TableName)
	// Skip changes applied by sync (avoid infinite loop)
	syncMu.Lock()
	isSyncing := syncing
	syncMu.Unlock()
	if isSyncing {
		return
	}

	table := d.TableName
	if table == "sqlite_sequence" {
		return
	}

	var op string
	switch d.Op {
	case sqlite3.SQLITE_INSERT:
		op = "INSERT"
	case sqlite3.SQLITE_UPDATE:
		op = "UPDATE"
	case sqlite3.SQLITE_DELETE:
		op = "DELETE"
	default:
		return
	}

	change := Change{Op: op, Table: table}

	log.Printf("[%s] HOOK op=%s table=%s, capturing row...", n.ID, op, table)

	if op == "DELETE" {
		colCount := d.Count()
		oldRow := make([]any, colCount)
		if err := d.Old(oldRow...); err == nil && len(oldRow) > 0 {
			// d.Old returns []byte for TEXT columns; convert to string
			if b, ok := oldRow[0].([]byte); ok {
				change.OldID = string(b)
			} else if id, ok := oldRow[0].(string); ok {
				change.OldID = id
			}
		}
	} else {
		// INSERT or UPDATE — get new row values
		colCount := d.Count()
		newRow := make([]any, colCount)
		if err := d.New(newRow...); err != nil {
			log.Printf("[%s] HOOK d.New error: %v", n.ID, err)
		} else {
			log.Printf("[%s] HOOK d.New returned %d values: %v", n.ID, len(newRow), newRow)
			change.Row = make(map[string]any)
			cols := []string{"id", "name", "value", "created_at", "updated_at", "node_id"}
			for i, val := range newRow {
				if i < len(cols) {
					// d.New returns []byte for TEXT columns; convert to string
					if b, ok := val.([]byte); ok {
						change.Row[cols[i]] = string(b)
					} else {
						change.Row[cols[i]] = val
					}
				}
			}
		}
	}

	select {
	case n.changeCh <- change:
		log.Printf("[%s] HOOK pushed to channel, op=%s row=%v", n.ID, op, change.Row)
	default:
		log.Printf("[%s] WARNING: change channel full, dropping change", n.ID)
	}
}


// batchShip collects changes and ships every batchInterval (default 50ms)
// Drains channel completely before shipping to prevent changes being left behind
func (n *Node) batchShip() {
	batch := make([]Change, 0, 100)
	ticker := time.NewTicker(n.batchInterval)
	defer ticker.Stop()

	for {
		select {
		case c := <-n.changeCh:
			batch = append(batch, c)
			// Drain remaining changes from channel
			drain := true
			for drain {
				select {
				case c2 := <-n.changeCh:
					batch = append(batch, c2)
				default:
					drain = false
				}
			}
			if len(batch) >= 100 {
				n.ship(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			// Drain all pending changes from channel
			drain := true
			for drain {
				select {
				case c := <-n.changeCh:
					batch = append(batch, c)
				default:
					drain = false
				}
			}
			if len(batch) > 0 {
				n.ship(batch)
				batch = batch[:0]
			}
		}
	}
}

func (n *Node) ship(changes []Change) {
	if n.PeerURL == "" || len(changes) == 0 {
		return
	}
	log.Printf("[%s] SHIP %d changes to %s", n.ID, len(changes), n.PeerURL)

	data, err := json.Marshal(changes)
	if err != nil {
		log.Printf("[%s] marshal error: %v", n.ID, err)
		return
	}

	url := n.PeerURL + "/sync"
	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Node-Id", n.ID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("[%s] ship error to %s: %v", n.ID, n.PeerURL, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("[%s] ship failed: %d %s", n.ID, resp.StatusCode, string(body))
	}
}

func (n *Node) startHTTP() {
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
		BodyLimit:             16 * 1024 * 1024,
	})

	// POST /sync — receive changes from peer
	app.Post("/sync", func(c *fiber.Ctx) error {
		var changes []Change
		if err := json.Unmarshal(c.Body(), &changes); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		}
		applied := n.applyChanges(changes)
		return c.JSON(fiber.Map{"applied": applied})
	})

	// GET /api/items — list all items
	app.Get("/api/items", func(c *fiber.Ctx) error {
		rows, err := n.db.Query("SELECT id, name, value, created_at, updated_at, node_id FROM items ORDER BY created_at DESC LIMIT 100")
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
		err := n.db.QueryRow("SELECT id, name, value, created_at, updated_at, node_id FROM items WHERE id = ?", id).
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
		_, err := n.db.Exec(
			"INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
			idStr, body.Name, body.Value, now, now, n.ID,
		)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(map[string]any{
			"id": idStr, "name": body.Name, "value": body.Value,
			"created_at": now, "node_id": n.ID,
		})
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
		_, err := n.db.Exec(
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
		_, err := n.db.Exec("DELETE FROM items WHERE id=?", id)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(map[string]any{"deleted": id})
	})

	// GET /health
	app.Get("/health", func(c *fiber.Ctx) error {
		var count int
		n.db.QueryRow("SELECT COUNT(*) FROM items").Scan(&count)
		return c.JSON(fiber.Map{
			"ok": true, "node_id": n.ID, "item_count": count,
		})
	})

	go app.Listen(n.Listen)
}


// applyChanges applies received changes from peer in a single transaction.
// Transaction holds the connection for the entire batch, preventing local writes
// from interleaving while syncing flag is set (which would cause hook to drop changes).
func (n *Node) applyChanges(changes []Change) int {
	syncMu.Lock()
	syncing = true
	syncMu.Unlock()
	defer func() {
		syncMu.Lock()
		syncing = false
		syncMu.Unlock()
	}()

	tx, err := n.db.Begin()
	if err != nil {
		log.Printf("[%s] begin tx error: %v", n.ID, err)
		return 0
	}
	defer tx.Rollback()

	applied := 0
	for _, c := range changes {
		switch c.Op {
		case "INSERT", "UPDATE":
			if c.Row == nil {
				continue
			}
			id, _ := c.Row["id"].(string)
			if id == "" {
				continue
			}
			name, _ := c.Row["name"].(string)
			value := toInt64(c.Row["value"])
			createdAt := toInt64(c.Row["created_at"])
			updatedAt := toInt64(c.Row["updated_at"])
			nodeID, _ := c.Row["node_id"].(string)

			_, err := tx.Exec(
				"INSERT OR REPLACE INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
				id, name, value, createdAt, updatedAt, nodeID,
			)
			if err != nil {
				log.Printf("[%s] apply error: %v", n.ID, err)
				continue
			}
			applied++

		case "DELETE":
			if c.OldID == "" {
				continue
			}
			_, err := tx.Exec("DELETE FROM items WHERE id = ?", c.OldID)
			if err != nil {
				log.Printf("[%s] delete error: %v", n.ID, err)
				continue
			}
			applied++
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("[%s] commit error: %v", n.ID, err)
		return 0
	}
	return applied
}

// toInt64 converts any numeric value from JSON unmarshal to int64
func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case float64:
		return int64(n)
	case int:
		return int64(n)
	default:
		return 0
	}
}
