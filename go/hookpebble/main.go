//go:build sqlite_preupdate_hook

package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/mattn/go-sqlite3"
)

type Change struct {
	Op    string         `json:"op"`
	Table string         `json:"table"`
	Row   map[string]any `json:"row"`
	OldID string         `json:"old_id"`
}

type SyncRequest struct {
	BatchID int64    `json:"batch_id"`
	Changes []Change `json:"changes"`
}

type SyncResponse struct {
	Applied int   `json:"applied"`
	Ack     int64 `json:"ack"`
}

type pendingChange struct {
	seq     int64
	rowID   string
	op      string
	rowData string
}

type Node struct {
	ID            string
	DBPath        string
	Listen        string
	PeerURL       string
	db            *sql.DB
	pdb           *pebble.DB
	batchInterval time.Duration
	batchSize     int
	syncing       atomic.Bool
	seqCounter    int64
	pending       []pendingChange
}

func main() {
	id := flag.String("id", "", "node ID")
	dbPath := flag.String("db", "", "SQLite DB path")
	listen := flag.String("listen", "", "HTTP listen address")
	batchSize := flag.Int("batch-size", 10000, "max changes per ship")
	batchMs := flag.Int("batch-ms", 50, "ship interval ms")
	peerURL := flag.String("peer", "", "peer URL")
	flag.Parse()

	if *id == "" || *dbPath == "" || *listen == "" {
		log.Fatal("usage: hook-sync-hookpebble -id node1 -db node1.db -listen :9001 -peer http://localhost:9002")
	}

	n := &Node{
		ID: *id, DBPath: *dbPath, Listen: *listen, PeerURL: *peerURL,
		batchSize: *batchSize, batchInterval: time.Duration(*batchMs) * time.Millisecond,
	}

	// Pebble first, then SQLite (hook needs pdb)
	pdb, err := pebble.Open(n.DBPath+".pebble", &pebble.Options{})
	if err != nil {
		log.Fatalf("pebble: %v", err)
	}
	n.pdb = pdb

	driverName := "sqlite3_hp_" + n.ID
	sql.Register(driverName, &sqlite3.SQLiteDriver{
		ConnectHook: func(c *sqlite3.SQLiteConn) error {
			// preupdate_hook: collect in-memory (fast, no I/O in C callback)
			c.RegisterPreUpdateHook(func(data sqlite3.SQLitePreUpdateData) {
				if n.syncing.Load() || data.TableName != "items" {
					return
				}
				seq := atomic.AddInt64(&n.seqCounter, 1)

				var row []any
				var rowID string
				if data.Op == sqlite3.SQLITE_DELETE {
					row = make([]any, data.Count())
					data.Old(row...)
				} else {
					row = make([]any, data.Count())
					data.New(row...)
				}
				if id, ok := row[0].([]byte); ok {
					rowID = string(id)
				}

				op := "INSERT"
				switch data.Op {
				case sqlite3.SQLITE_UPDATE:
					op = "UPDATE"
				case sqlite3.SQLITE_DELETE:
					op = "DELETE"
				}

				cols := []string{"id", "name", "value", "created_at", "updated_at", "node_id"}
				obj := map[string]any{}
				for i, col := range cols {
					if i >= len(row) {
						break
					}
					switch v := row[i].(type) {
					case []byte:
						obj[col] = string(v)
					case int64:
						obj[col] = v
					default:
						obj[col] = nil
					}
				}
				rowJSON, _ := json.Marshal(obj)

				n.pending = append(n.pending, pendingChange{
					seq: seq, rowID: rowID, op: op, rowData: string(rowJSON),
				})
			})

			// commit_hook: flush all pending as 1 Pebble batch (1 I/O per transaction)
			c.RegisterCommitHook(func() int {
				if len(n.pending) > 0 && n.pdb != nil {
					batch := n.pdb.NewBatch()
					for _, pc := range n.pending {
						key := fmt.Sprintf("seq:%020d", pc.seq)
						val := fmt.Sprintf(`{"op":"%s","row_id":"%s","row_data":%s}`, pc.op, pc.rowID, pc.rowData)
						batch.Set([]byte(key), []byte(val), nil)
					}
					batch.Commit(nil)
					batch.Close()
				}
				n.pending = n.pending[:0]
				return 0
			})

			// rollback_hook: discard pending (no false positives)
			c.RegisterRollbackHook(func() {
				n.pending = n.pending[:0]
			})
			return nil
		},
	})

	db, err := sql.Open(driverName, n.DBPath+"?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000")
	if err != nil {
		log.Fatalf("sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	n.db = db

	db.Exec(`CREATE TABLE IF NOT EXISTS items (
		id TEXT PRIMARY KEY, name TEXT, value INTEGER,
		created_at INTEGER, updated_at INTEGER, node_id TEXT
	)`)

	if n.PeerURL != "" {
		go n.shipLoop()
	}

	app := fiber.New(fiber.Config{DisableStartupMessage: true, BodyLimit: 16 * 1024 * 1024})

	app.Post("/sync", func(c *fiber.Ctx) error {
		var req SyncRequest
		json.Unmarshal(c.Body(), &req)
		applied := n.applyChanges(req.Changes)
		return c.JSON(fiber.Map{"applied": applied, "ack": req.BatchID})
	})

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

	app.Get("/api/items", func(c *fiber.Ctx) error {
		rows, _ := n.db.Query("SELECT id, name, value, created_at, updated_at, node_id FROM items ORDER BY created_at DESC LIMIT 100")
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

	app.Post("/api/items", func(c *fiber.Ctx) error {
		var body struct {
			Name  string `json:"name"`
			Value int    `json:"value"`
		}
		c.BodyParser(&body)
		id, _ := uuid.NewV7()
		now := time.Now().UnixMilli()
		n.db.Exec("INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
			id.String(), body.Name, body.Value, now, now, n.ID)
		return c.JSON(fiber.Map{"id": id.String(), "name": body.Name, "value": body.Value, "created_at": now, "node_id": n.ID})
	})

	app.Post("/api/items/batch", func(c *fiber.Ctx) error {
		var items []struct {
			Name  string `json:"name"`
			Value int    `json:"value"`
		}
		c.BodyParser(&items)
		tx, _ := n.db.Begin()
		now := time.Now().UnixMilli()
		for _, item := range items {
			id, _ := uuid.NewV7()
			if _, err := tx.Exec("INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
				id.String(), item.Name, item.Value, now, now, n.ID); err != nil {
				tx.Rollback()
				return c.Status(500).JSON(fiber.Map{"error": err.Error()})
			}
		}
		tx.Commit()
		return c.JSON(fiber.Map{"created": len(items)})
	})

	app.Put("/api/items/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")
		var body struct {
			Name  string `json:"name"`
			Value int    `json:"value"`
		}
		c.BodyParser(&body)
		now := time.Now().UnixMilli()
		n.db.Exec("UPDATE items SET name=?, value=?, updated_at=? WHERE id=?", body.Name, body.Value, now, id)
		return c.JSON(fiber.Map{"id": id, "name": body.Name, "value": body.Value, "updated_at": now})
	})

	app.Delete("/api/items/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")
		n.db.Exec("DELETE FROM items WHERE id=?", id)
		return c.JSON(fiber.Map{"deleted": id})
	})

	app.Get("/health", func(c *fiber.Ctx) error {
		var count int
		n.db.QueryRow("SELECT COUNT(*) FROM items").Scan(&count)
		pending := 0
		iter, _ := n.pdb.NewIter(&pebble.IterOptions{LowerBound: []byte("seq:"), UpperBound: []byte("seq~")})
		for iter.First(); iter.Valid(); iter.Next() {
			pending++
		}
		iter.Close()
		return c.JSON(fiber.Map{"ok": true, "node_id": n.ID, "item_count": count, "pending_changes": pending, "dead_letter": 0, "mode": "hookpebble"})
	})

	log.Printf("[%s] hookpebble on %s, peer=%s", *id, *listen, *peerURL)
	app.Listen(*listen)
}

func (n *Node) shipLoop() {
	ticker := time.NewTicker(n.batchInterval)
	defer ticker.Stop()
	for range ticker.C {
		n.drainAndShip()
	}
}

func (n *Node) drainAndShip() {
	iter, err := n.pdb.NewIter(&pebble.IterOptions{LowerBound: []byte("seq:"), UpperBound: []byte("seq~")})
	if err != nil {
		return
	}

	var changes []Change
	var keys [][]byte
	var lastSeq int64

	for iter.First(); iter.Valid() && len(changes) < n.batchSize; iter.Next() {
		key := make([]byte, len(iter.Key()))
		copy(key, iter.Key())
		val := make([]byte, len(iter.Value()))
		copy(val, iter.Value())

		var pc struct {
			Op      string          `json:"op"`
			RowID   string          `json:"row_id"`
			RowData json.RawMessage `json:"row_data"`
		}
		if json.Unmarshal(val, &pc) != nil {
			continue
		}

		c := Change{Op: pc.Op, Table: "items"}
		c.OldID = pc.RowID
		json.Unmarshal([]byte(pc.RowData), &c.Row)
		changes = append(changes, c)
		keys = append(keys, key)

		var seq int64
		fmt.Sscanf(string(key), "seq:%020d", &seq)
		if seq > lastSeq {
			lastSeq = seq
		}
	}
	iter.Close()

	if len(changes) == 0 {
		return
	}

	data, _ := json.Marshal(SyncRequest{BatchID: lastSeq, Changes: changes})
	resp, err := http.Post(n.PeerURL+"/sync", "application/json", bytes.NewReader(data))
	if err != nil {
		log.Printf("[%s] ship error: %v", n.ID, err)
		return
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var sr SyncResponse
	json.Unmarshal(body, &sr)

	if sr.Ack == lastSeq {
		batch := n.pdb.NewBatch()
		for _, k := range keys {
			batch.Delete(k, nil)
		}
		batch.Commit(nil)
		batch.Close()
		log.Printf("[%s] shipped %d changes", n.ID, len(changes))
	}
}

func (n *Node) applyChanges(changes []Change) int {
	n.syncing.Store(true)
	defer n.syncing.Store(false)

	tx, err := n.db.Begin()
	if err != nil {
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

			var existing int64
			if tx.QueryRow("SELECT updated_at FROM items WHERE id = ?", id).Scan(&existing) == nil {
				if existing > updatedAt {
					continue
				}
			}
			if _, err := tx.Exec("INSERT OR REPLACE INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
				id, name, value, createdAt, updatedAt, nodeID); err != nil {
				continue
			}
			applied++
		case "DELETE":
			if c.OldID == "" {
				continue
			}
			if c.Row != nil {
				delUpdated := toInt64(c.Row["updated_at"])
				var existing int64
				if tx.QueryRow("SELECT updated_at FROM items WHERE id = ?", c.OldID).Scan(&existing) == nil {
					if existing > delUpdated {
						continue
					}
				}
			}
			tx.Exec("DELETE FROM items WHERE id = ?", c.OldID)
			applied++
		}
	}

	if tx.Commit() != nil {
		return 0
	}
	return applied
}

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
