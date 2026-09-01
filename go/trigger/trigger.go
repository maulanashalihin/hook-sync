// Package trigger provides trigger-based change capture for hook-sync.
// It attaches to an existing *sql.DB, creates sync tables + triggers, and
// runs a background ship loop. User picks capture mode at import time:
//
//	import "hook-sync/trigger"
//
//	db, _ := sql.Open("sqlite3", "app.db")
//	mgr, _ := trigger.Attach(db, hooksync.Config{
//	    ID: "node1", Peers: []string{"http://peer:9002"},
//	}, []string{"items"})
//	// Writes to db now replicate automatically.
//	defer mgr.Stop()
package trigger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"hook-sync/hooksync"
)

// Manager manages trigger-based change capture and sync.
type Manager struct {
	ID            string
	db            *sql.DB
	peers         []string
	batchInterval time.Duration
	batchSize     int
	tables        []string
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

// Attach sets up trigger-based change capture on an existing *sql.DB.
// It creates _meta, _changes, _dead_letter, _peer_state tables, generates
// triggers for each table via schema introspection, and starts the background
// ship loop. The db must be opened with SQLite WAL mode for best performance.
func Attach(db *sql.DB, config hooksync.Config, tables []string) (*Manager, error) {
	batchMs := config.BatchMs
	if batchMs <= 0 {
		batchMs = 50
	}
	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 10000
	}

	m := &Manager{
		ID:            config.ID,
		db:            db,
		peers:         config.Peers,
		batchInterval: time.Duration(batchMs) * time.Millisecond,
		batchSize:     batchSize,
		tables:        tables,
		stopCh:        make(chan struct{}),
	}

	if err := m.setupSyncTables(); err != nil {
		return nil, fmt.Errorf("setup sync tables: %w", err)
	}

	for _, table := range tables {
		if err := m.generateTriggers(table); err != nil {
			return nil, fmt.Errorf("generate triggers for %s: %w", table, err)
		}
	}

	m.initPeerState()

	m.wg.Add(1)
	go m.shipLoop()

	return m, nil
}

// Stop shuts down the background ship loop and waits for it to finish.
func (m *Manager) Stop() {
	close(m.stopCh)
	m.wg.Wait()
}

// ServeHTTP implements http.Handler for the /sync endpoint.
// Receives a SyncRequest, applies changes with LWW conflict resolution,
// and returns a SyncResponse with the ACK batch_id.
func (m *Manager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req hooksync.SyncRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	applied := m.ApplyChanges(req.Changes)
	json.NewEncoder(w).Encode(hooksync.SyncResponse{Applied: applied, Ack: req.BatchID})
}

// ApplyChanges applies received changes in a single transaction.
// Sets the syncing flag so triggers don't re-capture sync-applied changes.
func (m *Manager) ApplyChanges(changes []hooksync.Change) int {
	tx, err := m.db.Begin()
	if err != nil {
		log.Printf("[%s] begin tx error: %v", m.ID, err)
		return 0
	}
	defer tx.Rollback()

	tx.Exec("UPDATE _meta SET value = 1 WHERE key = 'syncing'")

	applied := 0
	for _, c := range changes {
		if hooksync.ApplyChange(tx, c) {
			applied++
		}
	}

	tx.Exec("UPDATE _meta SET value = 0 WHERE key = 'syncing'")

	if err := tx.Commit(); err != nil {
		log.Printf("[%s] commit error: %v", m.ID, err)
		return 0
	}
	return applied
}

// HealthStatus holds health information for monitoring.
type HealthStatus struct {
	OK             bool     `json:"ok"`
	NodeID         string   `json:"node_id"`
	ItemCount      int      `json:"item_count"`
	PendingChanges int      `json:"pending_changes"`
	DeadLetter     int      `json:"dead_letter"`
	Peers          []string `json:"peers"`
}

// Health returns current health status. itemCount is the total rows across
// all synced tables.
func (m *Manager) Health() HealthStatus {
	h := HealthStatus{OK: true, NodeID: m.ID, Peers: m.peers}

	var totalItems int
	for _, table := range m.tables {
		var count int
		m.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
		totalItems += count
	}
	h.ItemCount = totalItems

	m.db.QueryRow("SELECT COUNT(*) FROM _changes").Scan(&h.PendingChanges)
	m.db.QueryRow("SELECT COUNT(*) FROM _dead_letter").Scan(&h.DeadLetter)
	return h
}

// setupSyncTables creates the internal sync tables.
func (m *Manager) setupSyncTables() error {
	_, err := m.db.Exec(`
		CREATE TABLE IF NOT EXISTS _meta (
			key TEXT PRIMARY KEY,
			value INTEGER
		);
		INSERT OR IGNORE INTO _meta(key, value) VALUES('syncing', 0);

		CREATE TABLE IF NOT EXISTS _changes (
			change_id INTEGER PRIMARY KEY AUTOINCREMENT,
			op TEXT,
			table_name TEXT,
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
	`)
	return err
}

// generateTriggers introspects a table's schema via PRAGMA table_info and
// creates INSERT/UPDATE/DELETE triggers that capture changes to _changes.
// The trigger uses json_object() to serialize the full row, which is
// table-agnostic — no hardcoded column names.
func (m *Manager) generateTriggers(table string) error {
	rows, err := m.db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return fmt.Errorf("introspect %s: %w", table, err)
	}

	type colInfo struct {
		name string
	}
	var cols []colInfo
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			rows.Close()
			return err
		}
		cols = append(cols, colInfo{name: name})
	}
	rows.Close()

	if len(cols) == 0 {
		return fmt.Errorf("table %s not found or has no columns", table)
	}

	// Build json_object args for NEW and OLD row
	newArgs := make([]string, len(cols))
	oldArgs := make([]string, len(cols))
	for i, c := range cols {
		newArgs[i] = fmt.Sprintf("'%s', NEW.%s", c.name, c.name)
		oldArgs[i] = fmt.Sprintf("'%s', OLD.%s", c.name, c.name)
	}
	newJSON := strings.Join(newArgs, ", ")
	oldJSON := strings.Join(oldArgs, ", ")
	sql := fmt.Sprintf(`
		CREATE TRIGGER IF NOT EXISTS %s_ai AFTER INSERT ON %s
		WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
		BEGIN
			INSERT INTO _changes(op, table_name, row_id, row_data) VALUES('INSERT', '%s', NEW.id,
				json_object(%s));
		END;

		CREATE TRIGGER IF NOT EXISTS %s_au AFTER UPDATE ON %s
		WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
		BEGIN
			INSERT INTO _changes(op, table_name, row_id, row_data) VALUES('UPDATE', '%s', NEW.id,
				json_object(%s));
		END;

		CREATE TRIGGER IF NOT EXISTS %s_ad AFTER DELETE ON %s
		WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
		BEGIN
			INSERT INTO _changes(op, table_name, row_id, row_data) VALUES('DELETE', '%s', OLD.id,
				json_object(%s));
		END;
	`, table, table, table, newJSON,
		table, table, table, newJSON,
		table, table, table, oldJSON)

	_, err = m.db.Exec(sql)
	return err
}

func (m *Manager) initPeerState() {
	for _, peer := range m.peers {
		m.db.Exec("INSERT OR IGNORE INTO _peer_state(peer_url, last_acked) VALUES(?, 0)", peer)
	}
}

// peerState holds the watermark for a single peer.
type peerState struct {
	URL       string
	LastAcked int64
}

// shipLoop polls _changes every batchInterval and ships to all peers
// concurrently using per-peer watermarks. Changes are deleted from _changes
// only when ALL peers have ACKed.
func (m *Manager) shipLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.batchInterval)
	defer ticker.Stop()

	backoffs := []time.Duration{50, 100, 200, 400, 800}

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
		}

		if len(m.peers) == 0 {
			continue
		}

		// Drain mode: ship until no peer has a full batch pending
		for {
			rows, err := m.db.Query("SELECT peer_url, last_acked FROM _peer_state")
			if err != nil {
				log.Printf("[%s] query _peer_state error: %v", m.ID, err)
				break
			}
			var peers []peerState
			for rows.Next() {
				var ps peerState
				rows.Scan(&ps.URL, &ps.LastAcked)
				peers = append(peers, ps)
			}
			rows.Close()

			var wg sync.WaitGroup
			var mu sync.Mutex
			maxShipped := 0
			for _, ps := range peers {
				wg.Add(1)
				go func(ps peerState) {
					defer wg.Done()
					shipped := m.shipToPeer(ps, backoffs)
					mu.Lock()
					if shipped > maxShipped {
						maxShipped = shipped
					}
					mu.Unlock()
				}(ps)
			}
			wg.Wait()

			if maxShipped < m.batchSize {
				break
			}
		}

		// Cleanup: delete changes that ALL peers have ACKed
		var minAck sql.NullInt64
		m.db.QueryRow("SELECT MIN(last_acked) FROM _peer_state").Scan(&minAck)
		if minAck.Valid && minAck.Int64 > 0 {
			m.db.Exec("DELETE FROM _changes WHERE change_id <= ?", minAck.Int64)
		}
	}
}

// shipToPeer ships pending changes (change_id > lastAcked) to a single peer.
// Retries with exponential backoff. On connection error, changes stay in
// _changes for next tick. On ACK mismatch, moves to _dead_letter and advances
// watermark so this peer moves on.
func (m *Manager) shipToPeer(ps peerState, backoffs []time.Duration) int {
	rows, err := m.db.Query("SELECT change_id, op, table_name, row_id, row_data FROM _changes WHERE change_id > ? ORDER BY change_id LIMIT ?",
		ps.LastAcked, m.batchSize)
	if err != nil {
		log.Printf("[%s] query _changes error: %v", m.ID, err)
		return 0
	}

	type changeRow struct {
		ChangeID  int64
		Op        string
		TableName string
		RowID     string
		RowData   string
	}
	var crs []changeRow
	for rows.Next() {
		var cr changeRow
		var rowData sql.NullString
		rows.Scan(&cr.ChangeID, &cr.Op, &cr.TableName, &cr.RowID, &rowData)
		cr.RowData = rowData.String
		crs = append(crs, cr)
	}
	rows.Close()

	if len(crs) == 0 {
		return 0
	}



	changes := make([]hooksync.Change, 0, len(crs))
	var batchID int64
	for _, cr := range crs {
		if cr.ChangeID > batchID {
			batchID = cr.ChangeID
		}
		c := hooksync.Change{Op: cr.Op, Table: cr.TableName}
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
		resp, err := hooksync.ShipWithAck(m.ID, batchID, changes, ps.URL)
		if err != nil {
			log.Printf("[%s] ship attempt %d to %s error: %v", m.ID, attempt+1, ps.URL, err)
			connError = true
			continue
		}
		connError = false
		if resp.Ack == batchID {
			m.db.Exec("UPDATE _peer_state SET last_acked = ? WHERE peer_url = ?", batchID, ps.URL)
			acked = true
			break
		}
		log.Printf("[%s] ship attempt %d to %s ACK mismatch: got %d want %d", m.ID, attempt+1, ps.URL, resp.Ack, batchID)
	}

	if !acked {
		if connError {
			log.Printf("[%s] peer %s unreachable, keeping %d changes for next tick", m.ID, ps.URL, len(crs))
			return 0
		}
		log.Printf("[%s] ship to %s failed after %d retries (ACK mismatch), moving to _dead_letter", m.ID, ps.URL, len(backoffs))
		for _, cr := range crs {
			m.db.Exec("INSERT INTO _dead_letter(op, row_id, row_data, failed_at, retry_count) VALUES(?, ?, ?, ?, ?)",
				cr.Op, cr.RowID, cr.RowData, time.Now().UnixMilli(), len(backoffs))
		}
		m.db.Exec("UPDATE _peer_state SET last_acked = ? WHERE peer_url = ?", batchID, ps.URL)
	}
	return len(crs)
}
