package bench

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	sqlite3 "github.com/mattn/go-sqlite3"
)

// Direct Go benchmark — bypasses HTTP/curl overhead
// Measures pure SQLite write throughput with preupdate hook active

func main() {
	// Open DB with hook
	dsn := "bench.db?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	// Register preupdate hook
	rawConn, err := db.Conn(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	hookCount := 0
	var hookMu sync.Mutex

	err = rawConn.Raw(func(driverConn any) error {
		conn, ok := driverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return fmt.Errorf("not sqlite3 conn")
		}
		conn.RegisterPreUpdateHook(func(d sqlite3.SQLitePreUpdateData) {
			hookMu.Lock()
			hookCount++
			hookMu.Unlock()
		})
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	rawConn.Close()

	// Setup schema
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS items (
		id TEXT PRIMARY KEY, name TEXT, value INTEGER,
		created_at INTEGER, updated_at INTEGER, node_id TEXT
	)`)
	if err != nil {
		log.Fatal(err)
	}

	// Clean slate
	db.Exec("DELETE FROM items")

	// Benchmark: N sequential writes
	for _, n := range []int{100, 1000, 10000} {
		// Reset
		db.Exec("DELETE FROM items")
		hookMu.Lock()
		hookCount = 0
		hookMu.Unlock()

		start := time.Now()
		for i := 0; i < n; i++ {
			id, _ := uuid.NewV7()
			now := time.Now().UnixMilli()
			_, err := db.Exec(
				"INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
				id, fmt.Sprintf("item-%d", i), i, now, now, "bench",
			)
			if err != nil {
				log.Fatal(err)
			}
		}
		elapsed := time.Since(start)
		qps := float64(n) / elapsed.Seconds()

		hookMu.Lock()
		hc := hookCount
		hookMu.Unlock()

		fmt.Printf("Sequential  %5d writes: %8.1fms  %8.0f QPS  (hooks fired: %d)\n", n, elapsed.Seconds()*1000, qps, hc)
	}

	fmt.Println()

	// Benchmark: N writes in a single transaction
	for _, n := range []int{100, 1000, 10000} {
		db.Exec("DELETE FROM items")
		hookMu.Lock()
		hookCount = 0
		hookMu.Unlock()

		start := time.Now()
		tx, _ := db.Begin()
		for i := 0; i < n; i++ {
			id, _ := uuid.NewV7()
			now := time.Now().UnixMilli()
			tx.Exec(
				"INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
				id, fmt.Sprintf("item-%d", i), i, now, now, "bench",
			)
		}
		tx.Commit()
		elapsed := time.Since(start)
		qps := float64(n) / elapsed.Seconds()

		hookMu.Lock()
		hc := hookCount
		hookMu.Unlock()

		fmt.Printf("Transaction %5d writes: %8.1fms  %8.0f QPS  (hooks fired: %d)\n", n, elapsed.Seconds()*1000, qps, hc)
	}

	// Cleanup
	db.Exec("DELETE FROM items")
}
