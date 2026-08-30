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
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
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

type Node struct {
	ID            string
	DBPath        string
	Listen        string
	PeerURL       string
	db            *sql.DB
	batchInterval time.Duration
}

func main() {
	var (
		id      = flag.String("id", "", "node ID")
		dbPath  = flag.String("db", "", "SQLite DB path")
		listen  = flag.String("listen", "", "HTTP listen address")
		batchMs = flag.Int("batch-ms", 50, "batch ship interval ms")
		peerURL = flag.String("peer", "", "peer URL")
	)
	flag.Parse()

	if *id == "" || *dbPath == "" || *listen == "" {
		log.Fatal("usage: hook-sync-multi -id node1 -db node1.db -listen :9001 -peer http://localhost:9002")
	}

	node := &Node{
		ID:            *id,
		DBPath:        *dbPath,
		Listen:        *listen,
		batchInterval: time.Duration(*batchMs) * time.Millisecond,
		PeerURL:       *peerURL,
	}

	node.initDB()
	node.setupSchema()
	go node.batchShip()
	node.startHTTP()

	log.Printf("[%s] listening on %s, peer=%s", *id, *listen, *peerURL)
	select{}
}

func (n *Node) initDB() {
	dsn := n.DBPath + "?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	n.db = db
	db.SetMaxOpenConns(1)
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

		CREATE TABLE IF NOT EXISTS categories (
			id TEXT PRIMARY KEY,
			name TEXT,
			parent_id TEXT,
			created_at INTEGER,
			node_id TEXT
		);

		CREATE TABLE IF NOT EXISTS _meta (
			key TEXT PRIMARY KEY,
			value INTEGER
		);
		INSERT OR IGNORE INTO _meta(key, value) VALUES('syncing', 0);

		CREATE TABLE IF NOT EXISTS _changes (
			change_id INTEGER PRIMARY KEY AUTOINCREMENT,
			table_name TEXT,
			op TEXT,
			row_id TEXT,
			row_data TEXT
		);

		CREATE TABLE IF NOT EXISTS _dead_letter (
			dead_id INTEGER PRIMARY KEY AUTOINCREMENT,
			table_name TEXT,
			op TEXT,
			row_id TEXT,
			row_data TEXT,
			failed_at INTEGER,
			retry_count INTEGER DEFAULT 0
		);

		-- items triggers
		CREATE TRIGGER IF NOT EXISTS items_ai AFTER INSERT ON items
		WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
		BEGIN
			INSERT INTO _changes(table_name, op, row_id, row_data) VALUES('items', 'INSERT', NEW.id,
				json_object('id', NEW.id, 'name', NEW.name, 'value', NEW.value,
					'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'node_id', NEW.node_id));
		END;

		CREATE TRIGGER IF NOT EXISTS items_au AFTER UPDATE ON items
		WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
		BEGIN
			INSERT INTO _changes(table_name, op, row_id, row_data) VALUES('items', 'UPDATE', NEW.id,
				json_object('id', NEW.id, 'name', NEW.name, 'value', NEW.value,
					'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'node_id', NEW.node_id));
		END;

		CREATE TRIGGER IF NOT EXISTS items_ad AFTER DELETE ON items
		WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
		BEGIN
			INSERT INTO _changes(table_name, op, row_id, row_data) VALUES('items', 'DELETE', OLD.id, NULL);
		END;

		-- categories triggers
		CREATE TRIGGER IF NOT EXISTS cat_ai AFTER INSERT ON categories
		WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
		BEGIN
			INSERT INTO _changes(table_name, op, row_id, row_data) VALUES('categories', 'INSERT', NEW.id,
				json_object('id', NEW.id, 'name', NEW.name, 'parent_id', NEW.parent_id,
					'created_at', NEW.created_at, 'node_id', NEW.node_id));
		END;

		CREATE TRIGGER IF NOT EXISTS cat_au AFTER UPDATE ON categories
		WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
		BEGIN
			INSERT INTO _changes(table_name, op, row_id, row_data) VALUES('categories', 'UPDATE', NEW.id,
				json_object('id', NEW.id, 'name', NEW.name, 'parent_id', NEW.parent_id,
					'created_at', NEW.created_at, 'node_id', NEW.node_id));
		END;

		CREATE TRIGGER IF NOT EXISTS cat_ad AFTER DELETE ON categories
		WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
		BEGIN
			INSERT INTO _changes(table_name, op, row_id, row_data) VALUES('categories', 'DELETE', OLD.id, NULL);
		END;
	`)
	if err != nil {
		log.Fatalf("setup schema: %v", err)
	}
}

func (n *Node) batchShip() {
	ticker := time.NewTicker(n.batchInterval)
	defer ticker.Stop()

	backoffs := []time.Duration{50, 100, 200, 400, 800}

	for range ticker.C {
		if n.PeerURL == "" {
			continue
		}

		rows, err := n.db.Query("SELECT change_id, table_name, op, row_id, row_data FROM _changes ORDER BY change_id LIMIT 100")
		if err != nil {
			log.Printf("[%s] query _changes error: %v", n.ID, err)
			continue
		}

		type changeRow struct {
			ChangeID  int64
			TableName string
			Op        string
			RowID     string
			RowData   string
		}
		var crs []changeRow
		for rows.Next() {
			var cr changeRow
			var rowData sql.NullString
			if err := rows.Scan(&cr.ChangeID, &cr.TableName, &cr.Op, &cr.RowID, &rowData); err != nil {
				continue
			}
			cr.RowData = rowData.String
			crs = append(crs, cr)
		}
		rows.Close()

		if len(crs) == 0 {
			continue
		}

		changes := make([]Change, 0, len(crs))
		var batchID int64
		for _, cr := range crs {
			if cr.ChangeID > batchID {
				batchID = cr.ChangeID
			}
			c := Change{Op: cr.Op, Table: cr.TableName}
			if cr.Op == "DELETE" {
				c.OldID = cr.RowID
			} else if cr.RowData != "" {
				json.Unmarshal([]byte(cr.RowData), &c.Row)
			}
			changes = append(changes, c)
		}

		acked := false
		for attempt := 0; attempt < 5; attempt++ {
			if attempt > 0 {
				time.Sleep(backoffs[attempt-1] * time.Millisecond)
			}
			resp, err := n.shipWithAck(batchID, changes)
			if err != nil {
				log.Printf("[%s] ship attempt %d error: %v", n.ID, attempt+1, err)
				continue
			}
			if resp.Ack == batchID {
				n.db.Exec("DELETE FROM _changes WHERE change_id <= ?", batchID)
				acked = true
				break
			}
		}

		if !acked {
			log.Printf("[%s] ship failed after 5 retries, moving to _dead_letter", n.ID)
			for _, cr := range crs {
				n.db.Exec("INSERT INTO _dead_letter(table_name, op, row_id, row_data, failed_at, retry_count) VALUES(?, ?, ?, ?, ?, ?)",
					cr.TableName, cr.Op, cr.RowID, cr.RowData, time.Now().UnixMilli(), 5)
			}
			n.db.Exec("DELETE FROM _changes WHERE change_id <= ?", batchID)
		}
	}
}

func (n *Node) shipWithAck(batchID int64, changes []Change) (*SyncResponse, error) {
	reqBody := SyncRequest{BatchID: batchID, Changes: changes}
	data, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", n.PeerURL+"/sync", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Node-Id", n.ID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ship failed: %d %s", resp.StatusCode, string(body))
	}

	var sr SyncResponse
	json.NewDecoder(resp.Body).Decode(&sr)
	return &sr, nil
}

func (n *Node) startHTTP() {
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
		BodyLimit:             16 * 1024 * 1024,
	})

	app.Post("/sync", func(c *fiber.Ctx) error {
		var req SyncRequest
		if err := json.Unmarshal(c.Body(), &req); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		}
		applied := n.applyChanges(req.Changes)
		return c.JSON(fiber.Map{"applied": applied, "ack": req.BatchID})
	})

	// --- items endpoints ---
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

	// --- categories endpoints ---
	app.Get("/api/categories", func(c *fiber.Ctx) error {
		rows, err := n.db.Query("SELECT id, name, parent_id, created_at, node_id FROM categories ORDER BY created_at DESC LIMIT 100")
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		defer rows.Close()
		cats := []map[string]any{}
		for rows.Next() {
			var id, name string
			var createdAt int64
			var parentIDPtr, nodeIDPtr sql.NullString
			rows.Scan(&id, &name, &parentIDPtr, &createdAt, &nodeIDPtr)
			cats = append(cats, map[string]any{
				"id": id, "name": name, "parent_id": parentIDPtr.String,
				"created_at": createdAt, "node_id": nodeIDPtr.String,
			})
		}
		return c.JSON(cats)
	})

	app.Post("/api/categories", func(c *fiber.Ctx) error {
		var body struct {
			Name     string `json:"name"`
			ParentID string `json:"parent_id"`
		}
		if err := c.BodyParser(&body); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		}
		id, _ := uuid.NewV7()
		idStr := id.String()
		now := time.Now().UnixMilli()
		var parentID interface{}
		if body.ParentID == "" {
			parentID = nil
		} else {
			parentID = body.ParentID
		}
		_, err := n.db.Exec(
			"INSERT INTO categories(id, name, parent_id, created_at, node_id) VALUES(?, ?, ?, ?, ?)",
			idStr, body.Name, parentID, now, n.ID,
		)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(map[string]any{
			"id": idStr, "name": body.Name, "parent_id": body.ParentID,
			"created_at": now, "node_id": n.ID,
		})
	})

	app.Get("/health", func(c *fiber.Ctx) error {
		var itemCount, catCount, pending, dead int
		n.db.QueryRow("SELECT COUNT(*) FROM items").Scan(&itemCount)
		n.db.QueryRow("SELECT COUNT(*) FROM categories").Scan(&catCount)
		n.db.QueryRow("SELECT COUNT(*) FROM _changes").Scan(&pending)
		n.db.QueryRow("SELECT COUNT(*) FROM _dead_letter").Scan(&dead)
		return c.JSON(fiber.Map{
			"ok": true, "node_id": n.ID,
			"items": itemCount, "categories": catCount,
			"pending_changes": pending, "dead_letter": dead,
		})
	})

	go app.Listen(n.Listen)
}

func (n *Node) applyChanges(changes []Change) int {
	tx, err := n.db.Begin()
	if err != nil {
		log.Printf("[%s] begin tx error: %v", n.ID, err)
		return 0
	}
	defer tx.Rollback()

	tx.Exec("UPDATE _meta SET value = 1 WHERE key = 'syncing'")

	applied := 0
	for _, c := range changes {
		switch c.Table {
		case "items":
			if c.Op == "INSERT" || c.Op == "UPDATE" {
				if c.Row == nil {
					continue
				}
				id, _ := c.Row["id"].(string)
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
					log.Printf("[%s] apply items error: %v", n.ID, err)
					continue
				}
				applied++
			} else if c.Op == "DELETE" {
				if c.OldID == "" {
					continue
				}
				tx.Exec("DELETE FROM items WHERE id = ?", c.OldID)
				applied++
			}

		case "categories":
			if c.Op == "INSERT" || c.Op == "UPDATE" {
				if c.Row == nil {
					continue
				}
				id, _ := c.Row["id"].(string)
				name, _ := c.Row["name"].(string)
				parentID, _ := c.Row["parent_id"].(string)
				createdAt := toInt64(c.Row["created_at"])
				nodeID, _ := c.Row["node_id"].(string)
				var pID interface{}
				if parentID == "" {
					pID = nil
				} else {
					pID = parentID
				}
				_, err := tx.Exec(
					"INSERT OR REPLACE INTO categories(id, name, parent_id, created_at, node_id) VALUES(?, ?, ?, ?, ?)",
					id, name, pID, createdAt, nodeID,
				)
				if err != nil {
					log.Printf("[%s] apply categories error: %v", n.ID, err)
					continue
				}
				applied++
			} else if c.Op == "DELETE" {
				if c.OldID == "" {
					continue
				}
				tx.Exec("DELETE FROM categories WHERE id = ?", c.OldID)
				applied++
			}
		}
	}

	tx.Exec("UPDATE _meta SET value = 0 WHERE key = 'syncing'")

	if err := tx.Commit(); err != nil {
		log.Printf("[%s] commit error: %v", n.ID, err)
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
