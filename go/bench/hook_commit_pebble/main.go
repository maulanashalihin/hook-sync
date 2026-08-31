// go/bench/hook_commit_pebble/main.go
//
// Benchmark: preupdate_hook + commit/rollback + Pebble vs triggers
//
// Protocol baru: capture changes via preupdate_hook (in-memory),
// flush to Pebble only on commit, discard on rollback.
// Same-transaction safety without SQL triggers.
//
// 4 modes, 100K INSERTs per run, 10 runs:
//   1. baseline           — INSERT only, no capture
//   2. triggers           — INSERT + SQL trigger writes _changes (same txn)
//   3. preupdate_pebble   — preupdate_hook → in-memory → commit_hook → Pebble batch
//   4. preupdate_pebble_rb — same as 3, but with rollback every 1000 rows (test rollback safety)
//
// Build: go build -tags sqlite_preupdate_hook -o /tmp/hook-commit-pebble ./go/bench/hook_commit_pebble/main.go
// Run:   /tmp/hook-commit-pebble

package main

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
	"github.com/mattn/go-sqlite3"
)

const (
	totalItems = 100_000
	runs       = 10
)

type changeRecord struct {
	rowID   string
	rowData string
}

var (
	registered    = map[string]bool{}
	pending       []changeRecord // collected by preupdate_hook, flushed by commit_hook
	pebbleDB      *pebble.DB
	hookCount     int
	commitCount   int
	rollbackCount int
)

func openDB(mode string) *sql.DB {
	driverName := "sqlite3_" + mode
	if !registered[driverName] {
		sql.Register(driverName, &sqlite3.SQLiteDriver{
			ConnectHook: func(c *sqlite3.SQLiteConn) error {
				if mode == "preupdate_pebble" || mode == "preupdate_pebble_rb" {
					// preupdate_hook: collect changes in-memory (NOT to Pebble yet)
					c.RegisterPreUpdateHook(func(data sqlite3.SQLitePreUpdateData) {
						if data.Op == sqlite3.SQLITE_INSERT && data.TableName == "items" {
							newRow := make([]any, data.Count())
							if err := data.New(newRow...); err != nil {
								return
							}
							id, _ := newRow[0].([]byte)
							name, _ := newRow[1].([]byte)
							value, _ := newRow[2].(int64)
							createdAt, _ := newRow[3].(int64)
							updatedAt, _ := newRow[4].(int64)
							nodeID, _ := newRow[5].([]byte)

							rowData := fmt.Sprintf(
								`{"id":"%s","name":"%s","value":%d,"created_at":%d,"updated_at":%d,"node_id":"%s"}`,
								string(id), string(name), value, createdAt, updatedAt, string(nodeID),
							)
							pending = append(pending, changeRecord{rowID: string(id), rowData: rowData})
							hookCount++
						}
					})

					// commit_hook: flush pending → Pebble batch (only on successful commit)
					c.RegisterCommitHook(func() int {
						if len(pending) > 0 && pebbleDB != nil {
							batch := pebbleDB.NewBatch()
							for _, rec := range pending {
								_ = batch.Set([]byte("data:"+rec.rowID), []byte(rec.rowData), pebble.Sync)
							}
							_ = batch.Commit(pebble.Sync)
							batch.Close()
							commitCount += len(pending)
						}
						pending = pending[:0] // clear regardless
						return 0              // 0 = allow commit
					})

					// rollback_hook: discard pending (no false positives)
					c.RegisterRollbackHook(func() {
						rollbackCount += len(pending)
						pending = pending[:0]
					})
				}
				return nil
			},
		})
		registered[driverName] = true
	}

	db, err := sql.Open(driverName, "/tmp/bench-commit-pebble.db?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000")
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	return db
}

func setupSchema(mode string) *sql.DB {
	os.Remove("/tmp/bench-commit-pebble.db")
	os.Remove("/tmp/bench-commit-pebble.db-wal")
	os.Remove("/tmp/bench-commit-pebble.db-shm")

	db := openDB(mode)

	_, err := db.Exec(`CREATE TABLE items (
		id TEXT PRIMARY KEY, name TEXT, value INTEGER,
		created_at INTEGER, updated_at INTEGER, node_id TEXT
	)`)
	if err != nil {
		log.Fatal(err)
	}

	if mode == "triggers" {
		_, err = db.Exec(`CREATE TABLE _changes (
			change_id INTEGER PRIMARY KEY AUTOINCREMENT,
			op TEXT, row_id TEXT, row_data TEXT
		)`)
		if err != nil {
			log.Fatal(err)
		}

		_, err = db.Exec(`CREATE TRIGGER items_ai AFTER INSERT ON items
		BEGIN
			INSERT INTO _changes(op, row_id, row_data) VALUES('INSERT', NEW.id,
				json_object('id', NEW.id, 'name', NEW.name, 'value', NEW.value,
					'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'node_id', NEW.node_id));
		END`)
		if err != nil {
			log.Fatal(err)
		}
	}

	return db
}

func runBench(mode string) []float64 {
	var qpsList []float64

	for run := 0; run < runs; run++ {
		db := setupSchema(mode)
		hookCount = 0
		commitCount = 0
		rollbackCount = 0
		pending = pending[:0]

		if mode == "preupdate_pebble" || mode == "preupdate_pebble_rb" {
			os.RemoveAll("/tmp/bench-commit-pebble-store")
			pdb, err := pebble.Open("/tmp/bench-commit-pebble-store", &pebble.Options{})
			if err != nil {
				log.Fatal(err)
			}
			pebbleDB = pdb
		}

		start := time.Now()

		if mode == "preupdate_pebble_rb" {
			// Insert with periodic rollbacks to test safety
			// 100K items, but every 1000th item triggers a rollback + retry
			now := time.Now().UnixMilli()
			i := 0
			for i < totalItems {
				tx, err := db.Begin()
				if err != nil {
					log.Fatal(err)
				}
				batchEnd := i + 1000
				if batchEnd > totalItems {
					batchEnd = totalItems
				}
				for ; i < batchEnd; i++ {
					_, err := tx.Exec(
						"INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
						uuid.New().String(), fmt.Sprintf("item-%d", i), i, now, now, "bench",
					)
					if err != nil {
						tx.Rollback()
						log.Fatal(err)
					}
				}
				// Rollback every other batch (even batch index)
				if (i/1000)%2 == 0 {
					tx.Rollback()
				} else {
					if err := tx.Commit(); err != nil {
						log.Fatal(err)
					}
				}
			}
		} else {
			tx, err := db.Begin()
			if err != nil {
				log.Fatal(err)
			}

			now := time.Now().UnixMilli()
			for i := 0; i < totalItems; i++ {
				_, err := tx.Exec(
					"INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
					uuid.New().String(), fmt.Sprintf("item-%d", i), i, now, now, "bench",
				)
				if err != nil {
					tx.Rollback()
					log.Fatal(err)
				}
			}

			if err := tx.Commit(); err != nil {
				log.Fatal(err)
			}
		}

		elapsed := time.Since(start).Seconds()
		qps := float64(totalItems) / elapsed
		qpsList = append(qpsList, qps)

		// Verify
		var count int
		db.QueryRow("SELECT COUNT(*) FROM items").Scan(&count)

		if mode == "preupdate_pebble" {
			if count != totalItems {
				log.Fatalf("VERIFY FAIL: items=%d expected=%d", count, totalItems)
			}
			if hookCount != totalItems {
				log.Fatalf("VERIFY FAIL: hook=%d expected=%d", hookCount, totalItems)
			}
			if commitCount != totalItems {
				log.Fatalf("VERIFY FAIL: commit=%d expected=%d", commitCount, totalItems)
			}
			if rollbackCount != 0 {
				log.Fatalf("VERIFY FAIL: rollback=%d expected=0", rollbackCount)
			}
			// Verify Pebble has all records
			iter, _ := pebbleDB.NewIter(&pebble.IterOptions{
				LowerBound: []byte("data:"),
				UpperBound: []byte("data~"),
			})
			pcount := 0
			for iter.First(); iter.Valid(); iter.Next() {
				pcount++
			}
			iter.Close()
			if pcount != totalItems {
				log.Fatalf("VERIFY FAIL: pebble=%d expected=%d", pcount, totalItems)
			}
		}

		if mode == "preupdate_pebble_rb" {
			// Half the batches were rolled back
			expectedItems := totalItems / 2 // 50 batches committed, 50 rolled back
			if count != expectedItems {
				log.Fatalf("VERIFY FAIL: items=%d expected=%d (rollback)", count, expectedItems)
			}
			// hookCount = totalItems (hook fires for all, including rolled back)
			// commitCount = expectedItems (only committed batches flushed to Pebble)
			// rollbackCount = expectedItems (rolled back batches discarded)
			if commitCount != expectedItems {
				log.Fatalf("VERIFY FAIL: commit=%d expected=%d (rollback)", commitCount, expectedItems)
			}
			if rollbackCount != expectedItems {
				log.Fatalf("VERIFY FAIL: rollback=%d expected=%d", rollbackCount, expectedItems)
			}
			// Pebble should only have committed records
			iter, _ := pebbleDB.NewIter(&pebble.IterOptions{
				LowerBound: []byte("data:"),
				UpperBound: []byte("data~"),
			})
			pcount := 0
			for iter.First(); iter.Valid(); iter.Next() {
				pcount++
			}
			iter.Close()
			if pcount != expectedItems {
				log.Fatalf("VERIFY FAIL: pebble=%d expected=%d (rollback)", pcount, expectedItems)
			}
		}

		if mode == "triggers" {
			if count != totalItems {
				log.Fatalf("VERIFY FAIL: items=%d expected=%d", count, totalItems)
			}
			var cc int
			db.QueryRow("SELECT COUNT(*) FROM _changes").Scan(&cc)
			if cc != totalItems {
				log.Fatalf("VERIFY FAIL: _changes=%d expected=%d", cc, totalItems)
			}
		}

		if mode == "baseline" {
			if count != totalItems {
				log.Fatalf("VERIFY FAIL: items=%d expected=%d", count, totalItems)
			}
		}

		if pebbleDB != nil {
			pebbleDB.Close()
			pebbleDB = nil
		}
		db.Close()
	}

	return qpsList
}

func median(qps []float64) float64 {
	sort.Float64s(qps)
	return qps[len(qps)/2]
}
func minQ(qps []float64) float64 {
	sort.Float64s(qps)
	return qps[0]
}
func maxQ(qps []float64) float64 {
	sort.Float64s(qps)
	return qps[len(qps)-1]
}
func fmtQPS(q float64) string {
	return fmt.Sprintf("%.0f", math.Round(q))
}

func main() {
	fmt.Println("============================================")
	fmt.Println("Benchmark: preupdate_hook + commit/rollback + Pebble vs triggers")
	fmt.Printf("  %d INSERTs per run, %d runs, direct SQLite (no HTTP)\n", totalItems, runs)
	fmt.Println("  WAL mode, synchronous=NORMAL, single transaction")
	fmt.Println("============================================")
	fmt.Println()

	modes := []string{"baseline", "triggers", "preupdate_pebble", "preupdate_pebble_rb"}
	labels := map[string]string{
		"baseline":            "baseline",
		"triggers":            "triggers",
		"preupdate_pebble":    "hook+commit+pebble",
		"preupdate_pebble_rb": "hook+commit+pebble (rollback test)",
	}
	results := map[string][]float64{}

	for _, mode := range modes {
		fmt.Printf(">>> %s\n", labels[mode])
		qps := runBench(mode)
		results[mode] = qps

		allQPS := make([]string, len(qps))
		for i, q := range qps {
			allQPS[i] = fmtQPS(q)
		}
		fmt.Printf("  QPS: min=%s med=%s max=%s\n", fmtQPS(minQ(qps)), fmtQPS(median(qps)), fmtQPS(maxQ(qps)))
		fmt.Printf("  All: %s\n", strings.Join(allQPS, ", "))
		fmt.Println()
	}

	// Summary
	fmt.Println("============================================")
	fmt.Println("Summary")
	fmt.Println("============================================")
	fmt.Println()

	baseMed := median(results["baseline"])
	trigMed := median(results["triggers"])
	hookMed := median(results["preupdate_pebble"])
	rbMed := median(results["preupdate_pebble_rb"])

	fmt.Printf("| Mode                    | QPS median | vs baseline | Same-txn safe? |\n")
	fmt.Printf("|-------------------------|------------|-------------|----------------|\n")
	fmt.Printf("| baseline                | %10s |       —     | N/A            |\n", fmtQPS(baseMed))
	fmt.Printf("| triggers                | %10s | %+.0f%%       | Yes (SQL)      |\n", fmtQPS(trigMed), (trigMed/baseMed-1)*100)
	fmt.Printf("| hook+commit+pebble      | %10s | %+.0f%%       | Yes (commit hook) |\n", fmtQPS(hookMed), (hookMed/baseMed-1)*100)
	fmt.Printf("| hook+commit+pebble (rb) | %10s | %+.0f%%       | Yes (verified)  |\n", fmtQPS(rbMed), (rbMed/baseMed-1)*100)
	fmt.Println()

	trigOverhead := (baseMed - trigMed) / baseMed * 100
	hookOverhead := (baseMed - hookMed) / baseMed * 100
	fmt.Printf("Trigger overhead:           -%.1f%% (writes _changes in same txn)\n", trigOverhead)
	fmt.Printf("hook+commit+pebble overhead: -%.1f%% (in-memory collect + 1 Pebble batch at commit)\n", hookOverhead)
	fmt.Println()

	fmt.Printf(">>> Fair comparison (both persist + same-txn safe):\n")
	if hookMed >= trigMed {
		fmt.Printf("    hook+commit+pebble WINS by %.0f%% (%s vs %s QPS)\n",
			(hookMed/trigMed-1)*100, fmtQPS(hookMed), fmtQPS(trigMed))
	} else {
		fmt.Printf("    triggers WIN by %.0f%% (%s vs %s QPS)\n",
			(trigMed/hookMed-1)*100, fmtQPS(trigMed), fmtQPS(hookMed))
	}
	fmt.Println()

	fmt.Printf(">>> Rollback safety:\n")
	fmt.Printf("    rollback test QPS: %s (50%% rollbacks, Pebble only has committed records)\n", fmtQPS(rbMed))
	fmt.Printf("    commit_count == pebble_count == items_count (verified per run)\n")
	fmt.Printf("    rollback_count == discarded pending (no false positives in Pebble)\n")
}
