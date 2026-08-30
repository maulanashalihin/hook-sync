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
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

// Change represents a single row-level change captured by triggers
type Change struct {
	Op    string         `json:"op"`
	Table string         `json:"table"`
	Row   map[string]any `json:"row"`
	OldID string         `json:"old_id"`
}

// SyncRequest is the ACK-based sync payload sent to peer
type SyncRequest struct {
	BatchID int64    `json:"batch_id"`
	Changes []Change `json:"changes"`
}

// SyncResponse is the ACK from peer
type SyncResponse struct {
	Applied int   `json:"applied"`
	Ack     int64 `json:"ack"`
}

// peerList is a repeatable --peer flag
type peerList []string

func (p *peerList) String() string { return strings.Join(*p, ",") }
func (p *peerList) Set(v string) error {
	*p = append(*p, v)
	return nil
}

// Node is a single sync node
type Node struct {
	ID            string
	DBPath        string
	Listen        string
	Peers         []string
	db            *sql.DB
	batchInterval time.Duration
	batchSize     int
}

func main() {
	var (
		id      = flag.String("id", "", "node ID (e.g. nodeA)")
		dbPath  = flag.String("db", "", "SQLite DB path")
		listen  = flag.String("listen", "", "HTTP listen address (e.g. :9001)")
		batchMs = flag.Int("batch-ms", 50, "batch ship interval in milliseconds")
		batchSize = flag.Int("batch-size", 10000, "max changes per ship batch")
		peers   peerList
	)
	flag.Var(&peers, "peer", "peer URL (repeatable, e.g. http://localhost:9002)")
	flag.Parse()

	if *id == "" || *dbPath == "" || *listen == "" {
		log.Fatal("usage: hook-sync-mesh-go -id nodeA -db a.db -listen :9001 -peer http://localhost:9002 -peer http://localhost:9003")
	}

	node := &Node{
		ID:            *id,
		DBPath:        *dbPath,
		Listen:        *listen,
		Peers:         peers,
		batchInterval: time.Duration(*batchMs) * time.Millisecond,
		batchSize:     *batchSize,
	}

	node.initDB()
	node.setupSchema()
	go node.batchShip()
	node.startHTTP()

	log.Printf("[%s] listening on %s, peers=[%s]", *id, *listen, strings.Join(peers, ", "))
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

		CREATE TABLE IF NOT EXISTS _meta (
			key TEXT PRIMARY KEY,
			value INTEGER
		);
		INSERT OR IGNORE INTO _meta(key, value) VALUES('syncing', 0);

		CREATE TABLE IF NOT EXISTS _changes (
			change_id INTEGER PRIMARY KEY AUTOINCREMENT,
			op TEXT,
			row_id TEXT,
			row_data TEXT
		);

		CREATE TABLE IF NOT EXISTS _dead_letter (
			dead_id INTEGER PRIMARY KEY AUTOINCREMENT,
			op TEXT,
			row_id TEXT,
			row_data TEXT,
			failed_at INTEGER,
			retry_count INTEGER DEFAULT 0
		);

		CREATE TABLE IF NOT EXISTS _peer_state (
			peer_url TEXT PRIMARY KEY,
			last_acked INTEGER DEFAULT 0
		);

		CREATE TRIGGER IF NOT EXISTS items_ai AFTER INSERT ON items
		WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
		BEGIN
			INSERT INTO _changes(op, row_id, row_data) VALUES('INSERT', NEW.id,
				json_object('id', NEW.id, 'name', NEW.name, 'value', NEW.value,
					'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'node_id', NEW.node_id));
		END;

		CREATE TRIGGER IF NOT EXISTS items_au AFTER UPDATE ON items
		WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
		BEGIN
			INSERT INTO _changes(op, row_id, row_data) VALUES('UPDATE', NEW.id,
				json_object('id', NEW.id, 'name', NEW.name, 'value', NEW.value,
					'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'node_id', NEW.node_id));
		END;

		CREATE TRIGGER IF NOT EXISTS items_ad AFTER DELETE ON items
		WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
		BEGIN
			INSERT INTO _changes(op, row_id, row_data) VALUES('DELETE', OLD.id,
				json_object('id', OLD.id, 'name', OLD.name, 'value', OLD.value,
					'created_at', OLD.created_at, 'updated_at', OLD.updated_at, 'node_id', OLD.node_id));
		END;
	`)
	if err != nil {
		log.Fatalf("setup schema: %v", err)
	}

	// Init peer state rows for configured peers
	for _, peer := range n.Peers {
		n.db.Exec("INSERT OR IGNORE INTO _peer_state(peer_url, last_acked) VALUES(?, 0)", peer)
	}
}

// peerState holds the watermark for a single peer
type peerState struct {
	URL       string
	LastAcked int64
}

// batchShip polls _changes every batchInterval. Ships to each peer independently
// using per-peer watermarks. Deletes from _changes only when ALL peers have ACKed.
func (n *Node) batchShip() {
	ticker := time.NewTicker(n.batchInterval)
	defer ticker.Stop()

	backoffs := []time.Duration{50, 100, 200, 400, 800}

	for range ticker.C {
		if len(n.Peers) == 0 {
			continue
		}

		// Drain mode: ship until no peer has a full batch pending
		for {
			// Read current peer states
			rows, err := n.db.Query("SELECT peer_url, last_acked FROM _peer_state")
			if err != nil {
				log.Printf("[%s] query _peer_state error: %v", n.ID, err)
				break
			}
			var peers []peerState
			for rows.Next() {
				var ps peerState
				rows.Scan(&ps.URL, &ps.LastAcked)
				peers = append(peers, ps)
			}
			rows.Close()

			// Ship to all peers concurrently
			var wg sync.WaitGroup
			var mu sync.Mutex
			maxShipped := 0
			for _, ps := range peers {
				wg.Add(1)
				go func(ps peerState) {
					defer wg.Done()
					shipped := n.shipToPeer(ps, backoffs)
					mu.Lock()
					if shipped > maxShipped {
						maxShipped = shipped
					}
					mu.Unlock()
				}(ps)
			}
			wg.Wait()

			// If no peer shipped a full batch, done draining
			if maxShipped < n.batchSize {
				break
			}
		}

		// Cleanup: delete changes that ALL peers have ACKed
		var minAck sql.NullInt64
		n.db.QueryRow("SELECT MIN(last_acked) FROM _peer_state").Scan(&minAck)
		if minAck.Valid && minAck.Int64 > 0 {
			n.db.Exec("DELETE FROM _changes WHERE change_id <= ?", minAck.Int64)
		}
	}
}

// shipToPeer ships pending changes (change_id > lastAcked) to a single peer.
// Retries with exponential backoff. On failure, logs and returns — changes
// stay in _changes until the peer comes back and ACKs.
func (n *Node) shipToPeer(ps peerState, backoffs []time.Duration) int {
	rows, err := n.db.Query("SELECT change_id, op, row_id, row_data FROM _changes WHERE change_id > ? ORDER BY change_id LIMIT ?", ps.LastAcked, n.batchSize)
	if err != nil {
		log.Printf("[%s] query _changes error: %v", n.ID, err)
		return 0
	}

	type changeRow struct {
		ChangeID int64
		Op       string
		RowID    string
		RowData  string
	}
	var crs []changeRow
	for rows.Next() {
		var cr changeRow
		var rowData sql.NullString
		rows.Scan(&cr.ChangeID, &cr.Op, &cr.RowID, &rowData)
		cr.RowData = rowData.String
		crs = append(crs, cr)
	}
	rows.Close()

	if len(crs) == 0 {
		return 0
	}

	// Build changes payload
	changes := make([]Change, 0, len(crs))
	var batchID int64
	for _, cr := range crs {
		if cr.ChangeID > batchID {
			batchID = cr.ChangeID
		}
		c := Change{Op: cr.Op, Table: "items"}
		if cr.Op == "DELETE" {
			c.OldID = cr.RowID
			if cr.RowData != "" {
				json.Unmarshal([]byte(cr.RowData), &c.Row)
			}
		} else if cr.RowData != "" {
			json.Unmarshal([]byte(cr.RowData), &c.Row)
		}
		changes = append(changes, c)
	}

	// Ship with retry
	acked := false
	connError := false
	for attempt := range backoffs {
		if attempt > 0 {
			time.Sleep(backoffs[attempt-1] * time.Millisecond)
		}
		resp, err := n.shipWithAck(batchID, changes, ps.URL)
		if err != nil {
			log.Printf("[%s] ship attempt %d to %s error: %v", n.ID, attempt+1, ps.URL, err)
			connError = true
			continue
		}
		connError = false
		if resp.Ack == batchID {
			n.db.Exec("UPDATE _peer_state SET last_acked = ? WHERE peer_url = ?", batchID, ps.URL)
			acked = true
			break
		}
		log.Printf("[%s] ship attempt %d to %s ACK mismatch: got %d want %d", n.ID, attempt+1, ps.URL, resp.Ack, batchID)
	}

	if !acked {
		if connError {
			// Peer not reachable — keep changes in _changes, try again next tick
			log.Printf("[%s] peer %s unreachable, keeping %d changes for next tick", n.ID, ps.URL, len(crs))
			return 0
		}
		// ACK mismatch — dead letter and advance watermark so this peer moves on
		log.Printf("[%s] ship to %s failed after %d retries (ACK mismatch), moving to _dead_letter", n.ID, ps.URL, len(backoffs))
		for _, cr := range crs {
			n.db.Exec("INSERT INTO _dead_letter(op, row_id, row_data, failed_at, retry_count) VALUES(?, ?, ?, ?, ?)",
				cr.Op, cr.RowID, cr.RowData, time.Now().UnixMilli(), len(backoffs))
		}
		n.db.Exec("UPDATE _peer_state SET last_acked = ? WHERE peer_url = ?", batchID, ps.URL)
	}
	return len(crs)

}

func (n *Node) shipWithAck(batchID int64, changes []Change, peerURL string) (*SyncResponse, error) {
	reqBody := SyncRequest{BatchID: batchID, Changes: changes}
	data, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}

	url := peerURL + "/sync"
	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
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
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &sr, nil
}

func (n *Node) startHTTP() {
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
		BodyLimit:             16 * 1024 * 1024,
	})

	// POST /sync — receive changes from peer (ACK-based)
	app.Post("/sync", func(c *fiber.Ctx) error {
		var req SyncRequest
		if err := json.Unmarshal(c.Body(), &req); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		}
		applied := n.applyChanges(req.Changes)
		return c.JSON(fiber.Map{"applied": applied, "ack": req.BatchID})
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

	// GET /health — includes per-peer watermark state
	app.Get("/health", func(c *fiber.Ctx) error {
		var count int
		n.db.QueryRow("SELECT COUNT(*) FROM items").Scan(&count)
		var pendingChanges int
		n.db.QueryRow("SELECT COUNT(*) FROM _changes").Scan(&pendingChanges)
		var deadLetter int
		n.db.QueryRow("SELECT COUNT(*) FROM _dead_letter").Scan(&deadLetter)

		rows, err := n.db.Query("SELECT peer_url, last_acked FROM _peer_state")
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		peers := []map[string]any{}
		for rows.Next() {
			var url string
			var acked int64
			rows.Scan(&url, &acked)
			peers = append(peers, map[string]any{"peer_url": url, "last_acked": acked})
		}
		rows.Close()

		return c.JSON(fiber.Map{
			"ok": true, "node_id": n.ID, "item_count": count,
			"pending_changes": pendingChanges, "dead_letter": deadLetter,
			"peers": peers,
		})
	})

	go app.Listen(n.Listen)
}

// applyChanges applies received changes from peer in a single transaction.
// Sets syncing flag in _meta so triggers don't capture sync-applied changes.
func (n *Node) applyChanges(changes []Change) int {
	tx, err := n.db.Begin()
	if err != nil {
		log.Printf("[%s] begin tx error: %v", n.ID, err)
		return 0
	}
	defer tx.Rollback()

	// Set syncing flag
	tx.Exec("UPDATE _meta SET value = 1 WHERE key = 'syncing'")

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

			// Last-write-wins: skip if existing row is newer than incoming
			var existingUpdatedAt int64
			if err := tx.QueryRow("SELECT updated_at FROM items WHERE id = ?", id).Scan(&existingUpdatedAt); err == nil {
				if existingUpdatedAt > updatedAt {
					continue
				}
			}

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
			// Last-write-wins: skip delete if row was updated after deletion
			if c.Row != nil {
				deleteUpdatedAt := toInt64(c.Row["updated_at"])
				var existingUpdatedAt int64
				if err := tx.QueryRow("SELECT updated_at FROM items WHERE id = ?", c.OldID).Scan(&existingUpdatedAt); err == nil {
					if existingUpdatedAt > deleteUpdatedAt {
						continue // row was updated after delete, keep the update
					}
				}
			}
			_, err := tx.Exec("DELETE FROM items WHERE id = ?", c.OldID)
			if err != nil {
				log.Printf("[%s] delete error: %v", n.ID, err)
				continue
			}
			applied++
		}
	}

	// Clear syncing flag before commit so triggers see the final state
	tx.Exec("UPDATE _meta SET value = 0 WHERE key = 'syncing'")

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
