// go/bench/hook_vs_trigger/main.go
//
// Benchmark: preupdate_hook vs triggers vs baseline (direct SQLite, no HTTP)
//
// 4 modes, 100K INSERTs per run, 10 runs, report median QPS:
//   1. baseline          — INSERT only, no capture
//   2. triggers          — INSERT + SQL trigger writes _changes row (same transaction)
//   3. preupdate         — INSERT + Go callback captures to in-memory (no _changes write)
//   4. preupdate_pebble  — INSERT + Go callback → channel → batch write to Pebble (LSM)
//
// Build: go build -tags sqlite_preupdate_hook -o /tmp/hook-vs-trigger ./go/bench/hook_vs_trigger/
// Run:   /tmp/hook-vs-trigger

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
	registered = map[string]bool{}
	hookCount  int
	changeCh   chan changeRecord
	pebbleDB   *pebble.DB
)

func openDB(mode string) *sql.DB {
	driverName := "sqlite3_" + mode
	if !registered[driverName] {
		sql.Register(driverName, &sqlite3.SQLiteDriver{
			ConnectHook: func(c *sqlite3.SQLiteConn) error {
				if mode == "preupdate" || mode == "preupdate_pebble" {
					c.RegisterPreUpdateHook(func(data sqlite3.SQLitePreUpdateData) {
						if data.Op == sqlite3.SQLITE_INSERT && data.TableName == "items" {
							newRow := make([]any, data.Count())
							if err := data.New(newRow...); err != nil {
								return
							}
							id, _ := newRow[0].([]byte)
							name, _ := newRow[1].([]byte)
							value, _ := newRow[2].(int64)
							updatedAt, _ := newRow[3].(int64)
							nodeID, _ := newRow[4].([]byte)

							rowData := fmt.Sprintf(
								`{"id":"%s","name":"%s","value":%d,"updated_at":%d,"node_id":"%s"}`,
								string(id), string(name), value, updatedAt, string(nodeID),
							)

							hookCount++
							if mode == "preupdate_pebble" {
								changeCh <- changeRecord{rowID: string(id), rowData: rowData}
							}
						}
					})
				}
				return nil
			},
		})
		registered[driverName] = true
	}

	db, err := sql.Open(driverName, "/tmp/hook-bench.db?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000")
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	return db
}

func setupSchema(mode string) *sql.DB {
	os.Remove("/tmp/hook-bench.db")
	os.Remove("/tmp/hook-bench.db-wal")
	os.Remove("/tmp/hook-bench.db-shm")

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
			op TEXT, table_name TEXT, row_id TEXT, row_data TEXT
		)`)
		if err != nil {
			log.Fatal(err)
		}
	}

	if mode == "triggers" {
		_, err = db.Exec(`CREATE TRIGGER items_insert AFTER INSERT ON items
		BEGIN
			INSERT INTO _changes(op, table_name, row_id, row_data)
			VALUES ('INSERT', 'items', NEW.id,
				json_object('id', NEW.id, 'name', NEW.name, 'value', NEW.value,
					'updated_at', NEW.updated_at, 'node_id', NEW.node_id));
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

		if mode == "preupdate_pebble" {
			changeCh = make(chan changeRecord, totalItems)
			os.RemoveAll("/tmp/hook-bench-pebble")
			pdb, err := pebble.Open("/tmp/hook-bench-pebble", &pebble.Options{})
			if err != nil {
				log.Fatal(err)
			}
			pebbleDB = pdb
		}

		start := time.Now()

		tx, err := db.Begin()
		if err != nil {
			log.Fatal(err)
		}

		now := time.Now().UnixMilli()
		for i := 0; i < totalItems; i++ {
			id := uuid.New().String()
			_, err := tx.Exec(
				"INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
				id, fmt.Sprintf("item-%d", i), i, now, now, "bench",
			)
			if err != nil {
				tx.Rollback()
				log.Fatal(err)
			}
		}

		if err := tx.Commit(); err != nil {
			log.Fatal(err)
		}

		// For preupdate_pebble: flush channel → batch write to Pebble (LSM)
		if mode == "preupdate_pebble" {
			close(changeCh)
			batch := pebbleDB.NewBatch()
			count := 0
			for rec := range changeCh {
				_ = batch.Set([]byte("data:"+rec.rowID), []byte(rec.rowData), pebble.Sync)
				count++
			}
			if err := batch.Commit(pebble.Sync); err != nil {
				log.Fatal(err)
			}
			batch.Close()

			// Verify: count keys in Pebble
			iter, err := pebbleDB.NewIter(&pebble.IterOptions{
				LowerBound: []byte("data:"),
				UpperBound: []byte("data~"),
			})
			if err != nil {
				log.Fatal(err)
			}
			pcount := 0
			for iter.First(); iter.Valid(); iter.Next() {
				pcount++
			}
			iter.Close()
			if pcount != totalItems {
				log.Fatalf("  VERIFY FAIL: pebble count=%d expected=%d", pcount, totalItems)
			}
			pebbleDB.Close()
		}

		elapsed := time.Since(start).Seconds()
		qps := float64(totalItems) / elapsed
		qpsList = append(qpsList, qps)

		// Verify
		var count int
		db.QueryRow("SELECT COUNT(*) FROM items").Scan(&count)
		if count != totalItems {
			log.Fatalf("  VERIFY FAIL: items count=%d expected=%d", count, totalItems)
		}

		if mode == "triggers" {
			var cc int
			db.QueryRow("SELECT COUNT(*) FROM _changes").Scan(&cc)
			if cc != totalItems {
				log.Fatalf("  VERIFY FAIL: _changes count=%d expected=%d", cc, totalItems)
			}
		}
		if mode == "preupdate" && hookCount != totalItems {
			log.Fatalf("  VERIFY FAIL: hook count=%d expected=%d", hookCount, totalItems)
		}
		if mode == "preupdate_pebble" && hookCount != totalItems {
			log.Fatalf("  VERIFY FAIL: hook count=%d expected=%d", hookCount, totalItems)
		}

		db.Close()
	}

	return qpsList
}

func median(qps []float64) float64 {
	sort.Float64s(qps)
	n := len(qps)
	if n%2 == 0 {
		return (qps[n/2-1] + qps[n/2]) / 2
	}
	return qps[n/2]
}

func min(qps []float64) float64 {
	sort.Float64s(qps)
	return qps[0]
}

func max(qps []float64) float64 {
	sort.Float64s(qps)
	return qps[len(qps)-1]
}

func fmtQPS(q float64) string {
	return fmt.Sprintf("%.0f", math.Round(q))
}

func main() {
	fmt.Println("============================================")
	fmt.Printf("Benchmark: preupdate_hook vs triggers vs baseline\n")
	fmt.Printf("  %d INSERTs per run, %d runs, direct SQLite (no HTTP)\n", totalItems, runs)
	fmt.Printf("  WAL mode, synchronous=NORMAL, single transaction\n")
	fmt.Println("============================================")
	fmt.Println()

	modes := []string{"baseline", "triggers", "preupdate", "preupdate_pebble"}
	labels := map[string]string{
		"baseline":         "baseline",
		"triggers":         "triggers",
		"preupdate":        "preupdate (mem)",
		"preupdate_pebble": "preupdate+pebble",
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
		fmt.Printf("  QPS: min=%s med=%s max=%s\n", fmtQPS(min(qps)), fmtQPS(median(qps)), fmtQPS(max(qps)))
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
	hookMed := median(results["preupdate"])
	hookPebbleMed := median(results["preupdate_pebble"])

	fmt.Printf("| Mode              | QPS median | vs baseline | Capture target      |\n")
	fmt.Printf("|-------------------|------------|-------------|---------------------|\n")
	fmt.Printf("| baseline          | %10s |       —     | No capture          |\n", fmtQPS(baseMed))
	fmt.Printf("| triggers          | %10s | %+.0f%%       | _changes (same txn) |\n", fmtQPS(trigMed), (trigMed/baseMed-1)*100)
	fmt.Printf("| preupdate (mem)   | %10s | %+.0f%%       | In-memory (no DB)   |\n", fmtQPS(hookMed), (hookMed/baseMed-1)*100)
	fmt.Printf("| preupdate+pebble  | %10s | %+.0f%%       | Pebble LSM (batch)  |\n", fmtQPS(hookPebbleMed), (hookPebbleMed/baseMed-1)*100)
	fmt.Println()

	trigOverhead := (baseMed - trigMed) / baseMed * 100
	hookOverhead := (baseMed - hookMed) / baseMed * 100
	hookPebbleOverhead := (baseMed - hookPebbleMed) / baseMed * 100
	fmt.Printf("Trigger overhead:          -%.1f%% (writes _changes in same txn)\n", trigOverhead)
	fmt.Printf("preupdate (mem) overhead:  -%.1f%% (Go callback, in-memory only)\n", hookOverhead)
	fmt.Printf("preupdate+pebble overhead: -%.1f%% (Go callback + batch write Pebble LSM)\n", hookPebbleOverhead)
	fmt.Println()

	fmt.Printf(">>> Fair comparison (both persist change records):\n")
	if trigMed >= hookPebbleMed {
		fmt.Printf("    triggers WIN by %.0f%% (%s vs %s QPS)\n",
			(trigMed/hookPebbleMed-1)*100, fmtQPS(trigMed), fmtQPS(hookPebbleMed))
	} else {
		fmt.Printf("    preupdate+pebble WINS by %.0f%% (%s vs %s QPS)\n",
			(hookPebbleMed/trigMed-1)*100, fmtQPS(hookPebbleMed), fmtQPS(trigMed))
	}
	fmt.Println()

	fmt.Printf(">>> Hook callback overhead alone (preupdate mem vs baseline):\n")
	fmt.Printf("    -%.1f%% — CGO trampoline + Go function call + data.New() per row\n", hookOverhead)
	fmt.Printf(">>> Pebble write cost (preupdate+pebble minus preupdate mem):\n")
	fmt.Printf("    -%.1f%% additional — channel + batch Set to LSM (append-only, no B-tree)\n",
		(hookMed-hookPebbleMed)/hookMed*100)
}
