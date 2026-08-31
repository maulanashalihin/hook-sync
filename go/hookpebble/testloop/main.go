//go:build sqlite_preupdate_hook

// testloop: same as teststeps but with 50ms ticker loop instead of one-shot sleep.
// Proves hook → pdb.Set → iterator works in a continuous loop.

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
	os.Remove("/tmp/testloop.db")
	os.RemoveAll("/tmp/testloop.pebble")

	// Step 0: Open Pebble
	pdb, err := pebble.Open("/tmp/testloop.pebble", &pebble.Options{})
	if err != nil {
		fmt.Println("STEP 0 FAIL: pebble open:", err)
		return
	}
	defer pdb.Close()
	fmt.Println("STEP 0 OK: pebble opened")

	var seqCounter int64
	var syncing atomic.Bool

	// Step 1: Open SQLite with hook
	sql.Register("sqlite3_testloop", &sqlite3.SQLiteDriver{
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

	db, err := sql.Open("sqlite3_testloop", "/tmp/testloop.db?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000")
	if err != nil {
		fmt.Println("STEP 1 FAIL: sql.Open:", err)
		return
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	_, err = db.Exec(`CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, value INTEGER, created_at INTEGER, updated_at INTEGER, node_id TEXT)`)
	if err != nil {
		fmt.Println("STEP 1c FAIL: create table:", err)
		return
	}
	fmt.Println("STEP 1 OK: sqlite + hook ready")

	// Step 2: ticker loop reads Pebble every 50ms
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				iter, err := pdb.NewIter(&pebble.IterOptions{
					LowerBound: []byte("seq:"),
					UpperBound: []byte("seq~"),
				})
				if err != nil {
					fmt.Println("  loop: iter error:", err)
					continue
				}
				count := 0
				for iter.First(); iter.Valid(); iter.Next() {
					count++
				}
				iter.Close()
				if count > 0 {
					fmt.Printf("  loop: found %d keys\n", count)
				}
			}
		}
	}()

	// Step 3: INSERT 3 rows with 200ms gap
	for i := 0; i < 3; i++ {
		fmt.Printf(">>> INSERT %d\n", i)
		_, err := db.Exec(
			"INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
			fmt.Sprintf("item-%d", i), fmt.Sprintf("n-%d", i), i, 0, 0, "test",
		)
		if err != nil {
			fmt.Printf("  INSERT %d FAIL: %v\n", i, err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	time.Sleep(500 * time.Millisecond)
	close(stop)

	// Final check
	iter, _ := pdb.NewIter(&pebble.IterOptions{
		LowerBound: []byte("seq:"),
		UpperBound: []byte("seq~"),
	})
	final := 0
	for iter.First(); iter.Valid(); iter.Next() {
		final++
	}
	iter.Close()
	fmt.Printf("\n=== FINAL: %d keys ===\n", final)
}
