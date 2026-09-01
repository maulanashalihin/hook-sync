//go:build sqlite_preupdate_hook

// Package hook provides preupdate-hook-based change capture for hook-sync.
// It opens a SQLite database with a custom driver that registers preupdate,
// commit, and rollback hooks. Changes are captured in-memory during the
// transaction, flushed to Pebble on commit, and discarded on rollback.
//
// User picks capture mode at import time:
//
//	import "hook-sync/hook"
//
//	mgr, _ := hook.Open("app.db", hooksync.Config{
//	    ID: "node1", Peers: []string{"http://peer:9002"},
//	}, []string{"items"})
//	defer mgr.Stop()
//
// Build with: go build -tags sqlite_preupdate_hook
package hook

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"hook-sync/hooksync"

	"github.com/cockroachdb/pebble"
	"github.com/mattn/go-sqlite3"
)

// pendingChange is an in-memory captured change waiting for commit flush.
type pendingChange struct {
	seq       int64
	rowID     string
	op        string
	tableName string
	rowData   string
}

// Manager manages hook-based change capture and sync.
type Manager struct {
	ID            string
	db            *sql.DB
	pdb           *pebble.DB
	peers         []string
	batchInterval time.Duration
	batchSize     int
	tables        []string
	tableCols     map[string][]string // table -> column names (from schema introspection)
	syncing       atomic.Bool
	seqCounter    int64
	pending       []pendingChange
	mu            sync.Mutex // protects pending for in-memory mode
	inMemory      bool
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

// Open creates a Pebble-backed hook capture manager. Changes are captured
// via preupdate hooks, flushed to Pebble on commit, and shipped to peers
// from Pebble. The Pebble DB is opened at path+".pebble".
func Open(path string, config hooksync.Config, tables []string) (*Manager, error) {
	return openManager(path, config, tables, false)
}

// OpenInMemory creates an in-memory hook capture manager. Changes are
// captured via preupdate hooks and held in a mutex-protected slice. No
// Pebble dependency. If the node crashes before shipping, pending changes
// are lost. Useful for benchmarking Pebble overhead.
func OpenInMemory(path string, config hooksync.Config, tables []string) (*Manager, error) {
	return openManager(path, config, tables, true)
}

func openManager(path string, config hooksync.Config, tables []string, inMemory bool) (*Manager, error) {
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
		peers:         config.Peers,
		batchInterval: time.Duration(batchMs) * time.Millisecond,
		batchSize:     batchSize,
		tables:        tables,
		tableCols:     make(map[string][]string),
		inMemory:      inMemory,
		stopCh:        make(chan struct{}),
	}

	// Open Pebble first (hook needs pdb reference)
	if !inMemory {
		pdb, err := pebble.Open(path+".pebble", &pebble.Options{})
		if err != nil {
			return nil, fmt.Errorf("pebble open: %w", err)
		}
		m.pdb = pdb
	}

	// Register custom SQLite driver with hooks
	driverName := "sqlite3_hook_" + config.ID
	sql.Register(driverName, &sqlite3.SQLiteDriver{
		ConnectHook: func(c *sqlite3.SQLiteConn) error {
			c.RegisterPreUpdateHook(func(data sqlite3.SQLitePreUpdateData) {
				if m.syncing.Load() {
					return
				}
				// Filter: only capture configured tables
				tableName := data.TableName
				if !m.isSyncedTable(tableName) {
					return
				}

				seq := atomic.AddInt64(&m.seqCounter, 1)

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

				// Build row map using introspected column names
				cols := m.tableCols[tableName]
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

				pc := pendingChange{
					seq: seq, rowID: rowID, op: op,
					tableName: tableName, rowData: string(rowJSON),
				}

				if inMemory {
					m.mu.Lock()
					m.pending = append(m.pending, pc)
					m.mu.Unlock()
				} else {
					// Pebble mode: collect in-memory, flush on commit
					m.pending = append(m.pending, pc)
				}
			})

			if !inMemory {
				// commit_hook: flush all pending as 1 Pebble batch
				c.RegisterCommitHook(func() int {
					if len(m.pending) > 0 && m.pdb != nil {
						batch := m.pdb.NewBatch()
						for _, pc := range m.pending {
							key := fmt.Sprintf("seq:%020d", pc.seq)
							val := fmt.Sprintf(`{"op":"%s","table":"%s","row_id":"%s","row_data":%s}`,
								pc.op, pc.tableName, pc.rowID, pc.rowData)
							batch.Set([]byte(key), []byte(val), nil)
						}
						batch.Commit(nil)
						batch.Close()
					}
					m.pending = m.pending[:0]
					return 0
				})

				// rollback_hook: discard pending
				c.RegisterRollbackHook(func() {
					m.pending = m.pending[:0]
				})
			}
			return nil
		},
	})

	db, err := sql.Open(driverName, path+"?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("sqlite open: %w", err)
	}
	db.SetMaxOpenConns(1)
	m.db = db

	// Introspect schema for each table
	for _, table := range tables {
		cols, err := introspectColumns(db, table)
		if err != nil {
			return nil, fmt.Errorf("introspect %s: %w", table, err)
		}
		m.tableCols[table] = cols
	}

	// Init per-peer watermarks in Pebble (or in-memory)
	for _, peer := range m.peers {
		if !inMemory {
			key := fmt.Sprintf("peer_state:%s", peer)
			if val, closer, err := m.pdb.Get([]byte(key)); err != nil || val == nil {
				m.pdb.Set([]byte(key), []byte("0"), nil)
			} else {
				closer.Close()
			}
		}
	}

	if len(m.peers) > 0 {
		m.wg.Add(1)
		go m.shipLoop()
	}

	return m, nil
}

// DB returns the underlying *sql.DB for application use (CRUD endpoints).
func (m *Manager) DB() *sql.DB { return m.db }

// Stop shuts down the background ship loop and closes Pebble.
func (m *Manager) Stop() {
	close(m.stopCh)
	m.wg.Wait()
	if m.pdb != nil {
		m.pdb.Close()
	}
}

// ServeHTTP implements http.Handler for the /sync endpoint.
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
// Sets the syncing flag so hooks don't re-capture sync-applied changes.
func (m *Manager) ApplyChanges(changes []hooksync.Change) int {
	m.syncing.Store(true)
	defer m.syncing.Store(false)

	tx, err := m.db.Begin()
	if err != nil {
		log.Printf("[%s] begin tx error: %v", m.ID, err)
		return 0
	}
	defer tx.Rollback()

	applied := 0
	for _, c := range changes {
		if hooksync.ApplyChange(tx, c) {
			applied++
		}
	}

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
	Mode           string   `json:"mode"`
}

// Health returns current health status.
func (m *Manager) Health() HealthStatus {
	h := HealthStatus{OK: true, NodeID: m.ID, Peers: m.peers}
	if m.inMemory {
		h.Mode = "hookmem"
	} else {
		h.Mode = "hookpebble"
	}

	var totalItems int
	for _, table := range m.tables {
		var count int
		m.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
		totalItems += count
	}
	h.ItemCount = totalItems

	if m.inMemory {
		m.mu.Lock()
		h.PendingChanges = len(m.pending)
		m.mu.Unlock()
	} else if m.pdb != nil {
		iter, _ := m.pdb.NewIter(&pebble.IterOptions{
			LowerBound: []byte("seq:"),
			UpperBound: []byte("seq~"),
		})
		for iter.First(); iter.Valid(); iter.Next() {
			h.PendingChanges++
		}
		iter.Close()
	}
	return h
}

func (m *Manager) isSyncedTable(name string) bool {
	for _, t := range m.tables {
		if t == name {
			return true
		}
	}
	return false
}

// shipLoop polls for pending changes and ships to all peers.
func (m *Manager) shipLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.batchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
		}

		if len(m.peers) == 0 {
			continue
		}

		for _, peer := range m.peers {
			m.shipToPeer(peer)
		}
	}
}

// shipToPeer ships pending changes to a single peer.
func (m *Manager) shipToPeer(peerURL string) {
	var changes []hooksync.Change
	var keys [][]byte
	var lastSeq int64

	if m.inMemory {
		m.mu.Lock()
		if len(m.pending) == 0 {
			m.mu.Unlock()
			return
		}
		for _, pc := range m.pending {
			c := hooksync.Change{Op: pc.op, Table: pc.tableName}
			c.OldID = pc.rowID
			json.Unmarshal([]byte(pc.rowData), &c.Row)
			changes = append(changes, c)
			if pc.seq > lastSeq {
				lastSeq = pc.seq
			}
		}
		m.pending = m.pending[:0]
		m.mu.Unlock()
	} else {
		iter, err := m.pdb.NewIter(&pebble.IterOptions{
			LowerBound: []byte("seq:"),
			UpperBound: []byte("seq~"),
		})
		if err != nil {
			return
		}

		for iter.First(); iter.Valid() && len(changes) < m.batchSize; iter.Next() {
			key := make([]byte, len(iter.Key()))
			copy(key, iter.Key())
			val := make([]byte, len(iter.Value()))
			copy(val, iter.Value())

			var pc struct {
				Op        string          `json:"op"`
				Table     string          `json:"table"`
				RowID     string          `json:"row_id"`
				RowData   json.RawMessage `json:"row_data"`
			}
			if json.Unmarshal(val, &pc) != nil {
				continue
			}

			c := hooksync.Change{Op: pc.Op, Table: pc.Table}
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
	}

	// Ship with retry
	backoffs := []time.Duration{50, 100, 200, 400, 800}
	acked := false
	for attempt := range backoffs {
		if attempt > 0 {
			time.Sleep(backoffs[attempt-1] * time.Millisecond)
		}

		data, _ := json.Marshal(hooksync.SyncRequest{BatchID: lastSeq, Changes: changes})
		resp, err := http.Post(peerURL+"/sync", "application/json", bytes.NewReader(data))
		if err != nil {
			log.Printf("[%s] ship attempt %d to %s error: %v", m.ID, attempt+1, peerURL, err)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var sr hooksync.SyncResponse
		json.Unmarshal(body, &sr)

		if sr.Ack == lastSeq {
			acked = true
			if !m.inMemory && m.pdb != nil {
				batch := m.pdb.NewBatch()
				for _, k := range keys {
					batch.Delete(k, nil)
				}
				batch.Commit(nil)
				batch.Close()
			}
			log.Printf("[%s] shipped %d changes to %s", m.ID, len(changes), peerURL)
			break
		}
		log.Printf("[%s] ship attempt %d to %s ACK mismatch: got %d want %d", m.ID, attempt+1, peerURL, sr.Ack, lastSeq)
	}

	if !acked && m.inMemory {
		// Re-queue changes for next tick
		m.mu.Lock()
		for _, c := range changes {
			seq := atomic.AddInt64(&m.seqCounter, 1)
			rowJSON, _ := json.Marshal(c.Row)
			m.pending = append(m.pending, pendingChange{
				seq: seq, rowID: c.OldID, op: c.Op,
				tableName: c.Table, rowData: string(rowJSON),
			})
		}
		m.mu.Unlock()
	}
}

// introspectColumns returns column names for a table via PRAGMA table_info.
func introspectColumns(db *sql.DB, table string) ([]string, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols = append(cols, name)
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("table %s not found or has no columns", table)
	}
	return cols, nil
}
