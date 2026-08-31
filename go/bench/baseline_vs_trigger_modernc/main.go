// go/bench/baseline_vs_trigger_modernc/main.go
//
// Direct SQLite benchmark (no HTTP): baseline vs triggers
// Uses modernc.org/sqlite (pure Go, no CGO) instead of mattn/go-sqlite3
//
// 100K INSERTs per run, 10 runs, report median QPS
//
// Run: go run ./go/bench/baseline_vs_trigger_modernc/

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

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

const (
	totalItems = 100_000
	runs       = 10
)

func bench(mode string) []float64 {
	var qpsList []float64

	for run := 0; run < runs; run++ {
		// Fresh DB each run
		for _, ext := range []string{"", "-wal", "-shm"} {
			os.Remove("/tmp/bench-go-modernc.db" + ext)
		}

		// modernc driver name is "sqlite"
		db, err := sql.Open("sqlite", "/tmp/bench-go-modernc.db?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)")
		if err != nil {
			log.Fatal(err)
		}
		db.SetMaxOpenConns(1)

		_, err = db.Exec(`CREATE TABLE items (
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

		start := time.Now()

		tx, err := db.Begin()
		if err != nil {
			log.Fatal(err)
		}

		stmt, err := tx.Prepare("INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)")
		if err != nil {
			log.Fatal(err)
		}

		now := time.Now().UnixMilli()
		for i := 0; i < totalItems; i++ {
			_, err := stmt.Exec(uuid.New().String(), fmt.Sprintf("item-%d", i), i, now, now, "bench")
			if err != nil {
				tx.Rollback()
				log.Fatal(err)
			}
		}
		stmt.Close()

		if err := tx.Commit(); err != nil {
			log.Fatal(err)
		}

		elapsed := time.Since(start).Seconds()
		qps := float64(totalItems) / elapsed
		qpsList = append(qpsList, qps)

		// Verify
		var count int
		db.QueryRow("SELECT COUNT(*) FROM items").Scan(&count)
		if count != totalItems {
			log.Fatalf("VERIFY FAIL: items=%d expected=%d", count, totalItems)
		}

		if mode == "triggers" {
			var cc int
			db.QueryRow("SELECT COUNT(*) FROM _changes").Scan(&cc)
			if cc != totalItems {
				log.Fatalf("VERIFY FAIL: _changes=%d expected=%d", cc, totalItems)
			}
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
	fmt.Println("Benchmark: baseline vs triggers (Go + modernc.org/sqlite, pure Go, no CGO)")
	fmt.Printf("  %d INSERTs per run, %d runs, direct SQLite (no HTTP)\n", totalItems, runs)
	fmt.Println("  WAL mode, synchronous=NORMAL, single transaction")
	fmt.Println("============================================")
	fmt.Println()

	results := map[string][]float64{}

	for _, mode := range []string{"baseline", "triggers"} {
		fmt.Printf(">>> %s\n", mode)
		qps := bench(mode)
		results[mode] = qps

		allQPS := make([]string, len(qps))
		for i, q := range qps {
			allQPS[i] = fmtQPS(q)
		}
		fmt.Printf("  QPS: min=%s med=%s max=%s\n", fmtQPS(minQ(qps)), fmtQPS(median(qps)), fmtQPS(maxQ(qps)))
		fmt.Printf("  All: %s\n", strings.Join(allQPS, ", "))
		fmt.Println()
	}

	fmt.Println("============================================")
	fmt.Println("Summary")
	fmt.Println("============================================")
	fmt.Println()

	baseMed := median(results["baseline"])
	trigMed := median(results["triggers"])
	overhead := (baseMed - trigMed) / baseMed * 100

	fmt.Println("| Mode     | QPS median | vs baseline |")
	fmt.Println("|----------|------------|-------------|")
	fmt.Printf("| baseline | %10s |       —     |\n", fmtQPS(baseMed))
	fmt.Printf("| triggers | %10s | -%.1f%%      |\n", fmtQPS(trigMed), overhead)
	fmt.Println()
	fmt.Printf("Trigger overhead: -%.1f%% (%s → %s QPS)\n", overhead, fmtQPS(baseMed), fmtQPS(trigMed))
}
