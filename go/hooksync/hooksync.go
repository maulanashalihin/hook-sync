package hooksync

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// validTable checks that a table name contains only alphanumeric characters
// and underscores. This prevents SQL injection via table names, which cannot
// be parameterized in SQLite. Table names come from the wire protocol between
// trusted peers, but validation is defense-in-depth.
func validTable(name string) bool {
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_') {
			return false
		}
	}
	return len(name) > 0
}

// Change represents a single row-level change captured by triggers or hooks.
type Change struct {
	Op    string         `json:"op"`     // INSERT, UPDATE, DELETE
	Table string         `json:"table"`  // target table name
	Row   map[string]any `json:"row"`    // full row values (for INSERT/UPDATE; OLD row for DELETE)
	OldID string         `json:"old_id"` // row ID for DELETE
}

// SyncRequest is the ACK-based sync payload sent to a peer.
type SyncRequest struct {
	BatchID int64    `json:"batch_id"`
	Changes []Change `json:"changes"`
}

// SyncResponse is the ACK from the receiving peer.
type SyncResponse struct {
	Applied int   `json:"applied"`
	Ack     int64 `json:"ack"`
}

// Config holds sync configuration shared by all capture modes.
type Config struct {
	ID        string   // node identifier (e.g. "node1")
	Peers     []string // peer URLs (e.g. ["http://localhost:9002"])
	BatchMs   int      // ship interval in milliseconds (default 50)
	BatchSize int      // max changes per ship batch (default 10000)
}

// ShipWithAck sends a batch of changes to a peer and returns the ACK response.
// The peer must implement POST /sync per the wire protocol (PROTOCOL.md).
func ShipWithAck(nodeID string, batchID int64, changes []Change, peerURL string) (*SyncResponse, error) {
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
	req.Header.Set("X-Node-Id", nodeID)

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

// ApplyChange applies a single change to a transaction with last-write-wins
// conflict resolution. It is table-agnostic: column names and values come from
// c.Row. Every table MUST have `id` (TEXT PRIMARY KEY) and `updated_at`
// (INTEGER millisecond timestamp) columns per the wire protocol.
//
// Returns true if the change was applied, false if skipped (LWW) or errored.
// The caller is responsible for transaction management and syncing flag.
func ApplyChange(tx *sql.Tx, c Change) bool {
	switch c.Op {
	case "INSERT", "UPDATE":
		return applyUpsert(tx, c)
	case "DELETE":
		return applyDelete(tx, c)
	}
	return false
}

func applyUpsert(tx *sql.Tx, c Change) bool {
	if c.Row == nil || !validTable(c.Table) {
		return false
	}
	id, _ := c.Row["id"].(string)
	if id == "" {
		return false
	}
	updatedAt := ToInt64(c.Row["updated_at"])

	// Last-write-wins: skip if existing row is newer than incoming
	var existingUpdatedAt int64
	if err := tx.QueryRow(
		fmt.Sprintf("SELECT updated_at FROM %s WHERE id = ?", c.Table), id,
	).Scan(&existingUpdatedAt); err == nil {
		if existingUpdatedAt > updatedAt {
			return false
		}
	}

	// Build dynamic INSERT OR REPLACE from Row keys
	cols := make([]string, 0, len(c.Row))
	vals := make([]any, 0, len(c.Row))
	placeholders := make([]string, 0, len(c.Row))
	for col, val := range c.Row {
		cols = append(cols, col)
		vals = append(vals, val)
		placeholders = append(placeholders, "?")
	}

	query := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
		c.Table, strings.Join(cols, ", "), strings.Join(placeholders, ", "))
	_, err := tx.Exec(query, vals...)
	return err == nil
}

func applyDelete(tx *sql.Tx, c Change) bool {
	if c.OldID == "" || !validTable(c.Table) {
		return false
	}
	// Last-write-wins: skip delete if row was updated after deletion
	if c.Row != nil {
		deleteUpdatedAt := ToInt64(c.Row["updated_at"])
		var existingUpdatedAt int64
		if err := tx.QueryRow(
			fmt.Sprintf("SELECT updated_at FROM %s WHERE id = ?", c.Table), c.OldID,
		).Scan(&existingUpdatedAt); err == nil {
			if existingUpdatedAt > deleteUpdatedAt {
				return false // row was updated after delete, keep the update
			}
		}
	}
	_, err := tx.Exec(fmt.Sprintf("DELETE FROM %s WHERE id = ?", c.Table), c.OldID)
	return err == nil
}

// ToInt64 converts any numeric value from JSON unmarshal to int64.
func ToInt64(v any) int64 {
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
