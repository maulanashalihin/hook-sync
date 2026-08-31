//go:build sqlite_preupdate_hook

// Reproduce: hook → pdb.Set → concurrent ticker iterator (like drainShip).
// Tests if iterator sees data written from CGO callback when reading concurrently.

package main

import (
	"database/sql"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/mattn/go-sqlite3"
)

func main() {
	os.Remove("/tmp/test-ticker.db")
	os.RemoveAll("/tmp/test-ticker.pebble")

	pdb, err := pebble.Open("/tmp/test-ticker.pebble", &pebble.Options{})
	if err != nil {
		fmt.Println("FAIL: pebble open:", err)
		return
	}
	defer pdb.Close()

	var seqCounter int64
	var syncing atomic.Bool

	sql.Register("sqlite3_ticker_test", &sqlite3.SQLiteDriver{
		ConnectHook: func(c *sqlite3.SQLiteConn) error {
			c.RegisterPreUpdateHook(func(data sqlite3.SQLitePreUpdateData) {
				if syncing.Load() || data.TableName != "items" {
					return
				}
				seq := atomic.AddInt64(&seqCounter, 1)
				newRow := make([]any, data.Count())
				if err := data.New(newRow...); err != nil {
					return
				}
				id, _ := newRow[0].([]byte)
				key := fmt.Sprintf("seq:%020d", seq)
				val := fmt.Sprintf(`{"id":"%s"}`, string(id))
				_ = pdb.Set([]byte(key), []byte(val), nil)
				fmt.Printf("  hook: set %s\n", key)
			})
			return nil
		},
	})

	db, err := sql.Open("sqlite3_ticker_test", "/tmp/test-ticker.db?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000")
	if err != nil {
		fmt.Println("FAIL: sql.Open:", err)
		return
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	db.Exec(`CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, value INTEGER, created_at INTEGER, updated_at INTEGER, node_id TEXT)`)

	// Ticker goroutine — reads Pebble every 100ms, like drainShip
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				// Try Get
				_, closer, gerr := pdb.Get([]byte("seq:00000000000000000001"))
				getStatus := "miss"
				if gerr == nil {
					getStatus = "HIT"
					closer.Close()
				}

				// Try iterator
				iter, _ := pdb.NewIter(&pebble.IterOptions{
					LowerBound: []byte("seq:"),
					UpperBound: []byte("seq~"),
				})
				count := 0
				for iter.First(); iter.Valid(); iter.Next() {
					count++
				}
				iter.Close()

				fmt.Printf("  ticker: Get=%s iter=%d\n", getStatus, count)
			}
		}
	}()

	// Write 3 rows
	for i := 0; i < 3; i++ {
		fmt.Printf(">>> INSERT %d\n", i)
		db.Exec("INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
			fmt.Sprintf("item-%d", i), fmt.Sprintf("n-%d", i), i, 0, 0, "test")
	}

	time.Sleep(1 * time.Second)
	close(stop)
	time.Sleep(200 * time.Millisecond)

	// Final read from main goroutine
	iter, _ := pdb.NewIter(&pebble.IterOptions{
		LowerBound: []byte("seq:"),
		UpperBound: []byte("seq~"),
	})
	final := 0
	for iter.First(); iter.Valid(); iter.Next() {
		final++
	}
	iter.Close()
	fmt.Printf("\n=== FINAL: iterator found %d keys ===\n", final)
}
