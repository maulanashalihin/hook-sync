package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

// Benchmark UUIDv4 vs UUIDv7 as SQLite primary key
// Tests: sequential insert, transaction batch insert
// Measures: QPS, insert time

func main() {
	db, err := sql.Open("sqlite3", "bench_uuid.db?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	// Schema: TEXT PRIMARY KEY (same as hook-sync prototype)
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS items (
		id TEXT PRIMARY KEY, name TEXT, value INTEGER,
		created_at INTEGER, updated_at INTEGER, node_id TEXT
	)`)
	if err != nil {
		log.Fatal(err)
	}

	sizes := []int{100, 1000, 10000, 100000}

	fmt.Println("=== UUIDv4 vs UUIDv7 — SQLite TEXT PRIMARY KEY ===")
	fmt.Println()

	// --- Sequential (per-write WAL flush) ---
	fmt.Println("--- Sequential (per-write commit) ---")
	fmt.Printf("%-8s  %12s  %12s  %8s  %8s\n", "N", "v4 QPS", "v7 QPS", "v4/v7", "winner")
	fmt.Println(string([]byte{0x2d}) + " " + string(repeat(0x2d, 58)))

	for _, n := range sizes {
		v4 := benchSequential(db, n, false)
		v7 := benchSequential(db, n, true)
		ratio := v4 / v7
		winner := "v7"
		if v4 > v7 {
			winner = "v4"
		}
		fmt.Printf("%-8d  %12.0f  %12.0f  %7.2fx  %s\n", n, v4, v7, ratio, winner)
	}

	fmt.Println()

	// --- Transaction (single WAL flush) ---
	fmt.Println("--- Transaction (single commit) ---")
	fmt.Printf("%-8s  %12s  %12s  %8s  %8s\n", "N", "v4 QPS", "v7 QPS", "v4/v7", "winner")
	fmt.Println(string([]byte{0x2d}) + " " + string(repeat(0x2d, 58)))

	for _, n := range sizes {
		v4 := benchTransaction(db, n, false)
		v7 := benchTransaction(db, n, true)
		ratio := v4 / v7
		winner := "v7"
		if v4 > v7 {
			winner = "v4"
		}
		fmt.Printf("%-8d  %12.0f  %12.0f  %7.2fx  %s\n", n, v4, v7, ratio, winner)
	}

	fmt.Println()

	// --- UUID generation speed (no DB) ---
	fmt.Println("--- UUID generation only (no DB) ---")
	fmt.Printf("%-8s  %12s  %12s  %8s  %8s\n", "N", "v4 QPS", "v7 QPS", "v4/v7", "winner")
	fmt.Println(string([]byte{0x2d}) + " " + string(repeat(0x2d, 58)))

	for _, n := range sizes {
		v4 := benchGenOnly(n, false)
		v7 := benchGenOnly(n, true)
		ratio := v4 / v7
		winner := "v7"
		if v4 > v7 {
			winner = "v4"
		}
		fmt.Printf("%-8d  %12.0f  %12.0f  %7.2fx  %s\n", n, v4, v7, ratio, winner)
	}

	db.Exec("DELETE FROM items")
}

func benchSequential(db *sql.DB, n int, useV7 bool) float64 {
	db.Exec("DELETE FROM items")
	start := time.Now()
	for i := 0; i < n; i++ {
		var id string
		if useV7 {
			u, _ := uuid.NewV7()
			id = u.String()
		} else {
			id = uuid.New().String()
		}
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
	return float64(n) / elapsed.Seconds()
}

func benchTransaction(db *sql.DB, n int, useV7 bool) float64 {
	db.Exec("DELETE FROM items")
	start := time.Now()
	tx, _ := db.Begin()
	for i := 0; i < n; i++ {
		var id string
		if useV7 {
			u, _ := uuid.NewV7()
			id = u.String()
		} else {
			id = uuid.New().String()
		}
		now := time.Now().UnixMilli()
		tx.Exec(
			"INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
			id, fmt.Sprintf("item-%d", i), i, now, now, "bench",
		)
	}
	tx.Commit()
	elapsed := time.Since(start)
	return float64(n) / elapsed.Seconds()
}

func benchGenOnly(n int, useV7 bool) float64 {
	start := time.Now()
	for i := 0; i < n; i++ {
		if useV7 {
			u, _ := uuid.NewV7()
			_ = u.String()
		} else {
			_ = uuid.New().String()
		}
	}
	elapsed := time.Since(start)
	return float64(n) / elapsed.Seconds()
}

func repeat(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}
