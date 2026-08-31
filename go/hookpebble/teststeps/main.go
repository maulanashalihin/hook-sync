//go:build sqlite_preupdate_hook

// Step-by-step test: hook fires → pdb.Set → iterator reads.
// No HTTP, no server. Just SQLite + Pebble + goroutine.
// Isolates which step breaks.

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
	os.Remove("/tmp/test-steps.db")
	os.RemoveAll("/tmp/test-steps.pebble")

	// --- Step 0: Open Pebble FIRST ---
	pdb, err := pebble.Open("/tmp/test-steps.pebble", &pebble.Options{})
	if err != nil {
		fmt.Println("STEP 0 FAIL: pebble open:", err)
		return
	}
	defer pdb.Close()
	fmt.Println("STEP 0 OK: pebble opened")

	var seqCounter int64
	var syncing atomic.Bool

	// --- Step 1: Open SQLite with preupdate_hook ---
	sql.Register("sqlite3_test_steps", &sqlite3.SQLiteDriver{
		ConnectHook: func(c *sqlite3.SQLiteConn) error {
			c.RegisterPreUpdateHook(func(data sqlite3.SQLitePreUpdateData) {
				if syncing.Load() || data.TableName != "items" {
					return
				}
				seq := atomic.AddInt64(&seqCounter, 1)

				newRow := make([]any, data.Count())
				if err := data.New(newRow...); err != nil {
					fmt.Println("STEP 1 FAIL: data.New:", err)
					return
				}
				id, _ := newRow[0].([]byte)
				name, _ := newRow[1].([]byte)
				val, _ := newRow[2].(int64)

				key := fmt.Sprintf("seq:%020d", seq)
				jsonVal := fmt.Sprintf(`{"id":"%s","name":"%s","value":%d}`, string(id), string(name), val)

				if err := pdb.Set([]byte(key), []byte(jsonVal), nil); err != nil {
					fmt.Println("STEP 2 FAIL: pdb.Set:", err)
					return
				}

				gotVal, closer, gerr := pdb.Get([]byte(key))
				if gerr != nil {
					fmt.Printf("STEP 2b FAIL: pdb.Get after Set: %v\n", gerr)
				} else {
					fmt.Printf("STEP 2b OK: Get(%s) = %s\n", key, string(gotVal))
					closer.Close()
				}
			})
			return nil
		},
	})

	db, err := sql.Open("sqlite3_test_steps", "/tmp/test-steps.db?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000")
	if err != nil {
		fmt.Println("STEP 1 FAIL: sql.Open:", err)
		return
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	fmt.Println("STEP 1 OK: sqlite opened with hook")

	_, err = db.Exec(`CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, value INTEGER, created_at INTEGER, updated_at INTEGER, node_id TEXT)`)
	if err != nil {
		fmt.Println("STEP 1c FAIL: create table:", err)
		return
	}
	fmt.Println("STEP 1c OK: table created")

	// --- Step 3: Background goroutine reads Pebble with iterator ---
	iterResult := make(chan int, 1)
	go func() {
		time.Sleep(500 * time.Millisecond)

		iter, err := pdb.NewIter(&pebble.IterOptions{
			LowerBound: []byte("seq:"),
			UpperBound: []byte("seq~"),
		})
		if err != nil {
			fmt.Println("STEP 3 FAIL: NewIter:", err)
			iterResult <- -1
			return
		}

		count := 0
		for iter.First(); iter.Valid(); iter.Next() {
			key := make([]byte, len(iter.Key()))
			copy(key, iter.Key())
			val := make([]byte, len(iter.Value()))
			copy(val, iter.Value())
			fmt.Printf("STEP 3: iterator found key=%s val=%s\n", string(key), string(val))
			count++
		}
		if err := iter.Error(); err != nil {
			fmt.Println("STEP 3 WARN: iter.Error():", err)
		}
		iter.Close()
		fmt.Printf("STEP 3: iterator total = %d\n", count)
		iterResult <- count
	}()

	// --- Step 2 trigger: INSERT rows ---
	for i := 0; i < 3; i++ {
		_, err := db.Exec(
			"INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
			fmt.Sprintf("item-%d", i), fmt.Sprintf("name-%d", i), i, 0, 0, "test",
		)
		if err != nil {
			fmt.Printf("STEP 2 FAIL: INSERT %d: %v\n", i, err)
			return
		}
		fmt.Printf("STEP 2 OK: INSERT %d done\n", i)
	}

	result := <-iterResult
	fmt.Printf("\n=== RESULT: iterator found %d keys ===\n", result)

	iter2, _ := pdb.NewIter(&pebble.IterOptions{
		LowerBound: []byte("seq:"),
		UpperBound: []byte("seq~"),
	})
	count2 := 0
	for iter2.First(); iter2.Valid(); iter2.Next() {
		count2++
	}
	iter2.Close()
	fmt.Printf("=== RESULT: main goroutine iterator found %d keys ===\n", count2)

	if result == 3 && count2 == 3 {
		fmt.Println("=== ALL STEPS PASS ===")
	} else {
		fmt.Println("=== MISMATCH: expected 3, got", result, "and", count2, "===")
	}
}
