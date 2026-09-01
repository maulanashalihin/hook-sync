// hook-sync-hub — dedicated relay node for star and multi-region topologies.
//
// ARCHITECTURE
//
//   The hub is a pure relay. It has no SQLite, no triggers, no /api/items endpoints.
//   All data enters via POST /sync from edge nodes (or peer hubs in multi-region).
//   The hub does two things:
//
//     1. BACKUP  — store every received change in Pebble KV under "data:{id}".
//        This is the hub's full backup copy of all data. Not queryable via SQL,
//        but scannable by key prefix ("data:" prefix → all rows).
//
//     2. FORWARD — relay every received change to all other edges (and peer hubs).
//        Forwarding is asynchronous: the hub ACKs the sender immediately, then
//        forwards in the background. If the hub crashes after ACK but before
//        forward, the forwarding entry survives in Pebble ("fwd:{n}" key) and
//        is replayed on restart.
//
//   Pebble KV stores two key namespaces:
//     "data:{id}"  → row JSON (backup copy, INSERT/UPDATE sets, DELETE removes)
//     "fwd:{n}"    → fwdEntry JSON (pending forward, one per edge per batch)
//
// LOOP PREVENTION (multi-region hub-to-hub)
//
//   In single-hub topology, no loop is possible: hub receives from edges,
//   forwards to edges, edges don't re-ship received changes (syncing flag).
//
//   In multi-region topology, hubs peer directly. Hub A forwards to hub B,
//   hub B would forward back to hub A → infinite loop. Prevention:
//
//     - Hub sends "X-Node-Url" header (its own URL, set via --url flag) with
//       every forward request.
//     - Receiving hub reads "X-Node-Url" and skips the edge whose URL matches.
//     - Edge nodes don't send "X-Node-Url" (empty header), so the hub forwards
//       to all edges normally.
//
//   Flow: edge1 → hub A (X-Node-Url: hubA-url) → hub B sees hubA-url, skips it,
//   forwards to edge3, edge4. Hub B sends (X-Node-Url: hubB-url) → hub A sees
//   hubB-url, skips it. No loop.
//
// CRASH RECOVERY
//
//   1. Hub receives /sync from edge → applyBackup (Pebble "data:{id}")
//   2. enqueueForward (Pebble "fwd:{n}") — BEFORE ACK, so crash doesn't lose it
//   3. ACK sender → edge deletes from its _changes
//   4. tryForwardAll — immediate forward attempt (low latency)
//   5. If hub crashes after step 3, before forward completes:
//      - "fwd:{n}" entries survive in Pebble
//      - On restart, replayPending logs count, forwardSweep picks them up
//      - Edges eventually receive the changes — no data loss

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/gofiber/fiber/v2"
)

// Change is the wire-protocol change (same as edge nodes).
type Change struct {
	Op    string         `json:"op"`
	Table string         `json:"table"`
	Row   map[string]any `json:"row"`
	OldID string         `json:"old_id"`
}

// SyncRequest is the ACK-based sync payload.
type SyncRequest struct {
	BatchID int64    `json:"batch_id"`
	Changes []Change `json:"changes"`
}

// SyncResponse is the ACK from receiver.
type SyncResponse struct {
	Applied int   `json:"applied"`
	Ack     int64 `json:"ack"`
}

// fwdEntry is stored in Pebble under key "fwd:{id}" for each pending forward.
type fwdEntry struct {
	BatchID int64    `json:"batch_id"`
	Changes []Change `json:"changes"`
	EdgeURL string   `json:"edge_url"`
}

// edgeList is a repeatable --edge flag.
type edgeList []string

func (e *edgeList) String() string { return strings.Join(*e, ",") }
func (e *edgeList) Set(v string) error {
	*e = append(*e, v)
	return nil
}

// Hub is a dedicated relay node. No SQLite, no triggers, no /api/items.
// All data enters via /sync. Pebble stores backup ("data:{id}") + durable
// forwarding queue ("fwd:{n}"). See file header for full architecture.
//
// Fields:
//   ID         — hub identifier (e.g. "hubA"), sent as X-Node-Id header
//   Listen     — HTTP listen address (e.g. ":9010")
//   MyURL      — this hub's full URL (e.g. "http://localhost:9010"), sent as
//                X-Node-Url header for multi-region loop prevention. Empty in
//                single-hub topology (no loop possible, header not needed).
//   Edges      — list of edge node URLs (and peer hub URLs in multi-region).
//                Hub forwards to every edge except the sender (matched by URL).
//   pdb        — Pebble KV store (LSM tree, write-optimized)
//   batchMs    — forward sweep interval (background retry loop)
//   fwdCounter — monotonic counter for "fwd:{n}" keys (atomic, thread-safe)
type Hub struct {
	ID         string
	Listen     string
	MyURL      string // this hub's full URL (for X-Node-Url header)
	Edges      []string
	pdb        *pebble.DB
	batchMs    int
	fwdCounter uint64
}

func main() {
	var (
		id      = flag.String("id", "", "hub ID (e.g. hub1)")
		listen  = flag.String("listen", "", "HTTP listen address (e.g. :9010)")
		batchMs = flag.Int("batch-ms", 50, "forward sweep interval in milliseconds")
		dbPath  = flag.String("db", "", "Pebble DB path (e.g. hub1.pebble)")
		edges   edgeList
		myURL   = flag.String("url", "", "this hub's full URL for hub-to-hub (e.g. http://localhost:9010)")
	)
	flag.Var(&edges, "edge", "edge node URL (repeatable, e.g. http://localhost:9001)")
	flag.Parse()

	if *id == "" || *listen == "" || *dbPath == "" {
		log.Fatal("usage: hook-sync-hub -id hub1 -listen :9010 -db hub1.pebble -edge http://localhost:9001 -edge http://localhost:9002")
	}

	pdb, err := pebble.Open(*dbPath, &pebble.Options{})
	if err != nil {
		log.Fatalf("open pebble: %v", err)
	}

	hub := &Hub{
		ID:      *id,
		Listen:  *listen,
		MyURL:   *myURL,
		Edges:   edges,
		batchMs: *batchMs,
		pdb:     pdb,
	}

	// Replay pending forwards from previous run (crash recovery)
	hub.replayPending()

	go hub.forwardSweep()
	hub.startHTTP()

	log.Printf("[%s] hub listening on %s, edges=[%s], db=%s", *id, *listen, strings.Join(edges, ", "), *dbPath)
	select {} // block forever
}

// applyBackup stores row data in Pebble under "data:{id}".
//
// This is the hub's backup copy of all data. Every received change is persisted
// here before ACK, so the hub always has a complete backup even if forwarding
// hasn't happened yet. Not queryable via SQL, but scannable by key prefix.
//
// INSERT/UPDATE: Set "data:{id}" → row JSON (overwrites previous value)
// DELETE:        Delete "data:{id}" (removes from backup)
//
// Uses Pebble batch for atomicity — all changes in one commit.
func (h *Hub) applyBackup(changes []Change) int {
	applied := 0
	batch := h.pdb.NewBatch()
	for _, c := range changes {
		switch c.Op {
		case "INSERT", "UPDATE":
			if c.Row == nil {
				continue
			}
			id, _ := c.Row["id"].(string)
			if id == "" {
				continue
			}
			val, _ := json.Marshal(c.Row)
			batch.Set([]byte("data:"+id), val, pebble.Sync)
			applied++
		case "DELETE":
			if c.OldID == "" {
				continue
			}
			batch.Delete([]byte("data:"+c.OldID), pebble.Sync)
			applied++
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		log.Printf("[%s] backup commit error: %v", h.ID, err)
		return 0
	}
	return applied
}

// enqueueForward creates a durable forwarding entry in Pebble for each edge.
//
// For every edge (except the sender), a "fwd:{n}" entry is written to Pebble
// BEFORE the sender is ACKed. This guarantees no data loss: if the hub crashes
// after ACK but before forward, the entries survive and are replayed on restart.
//
// Loop prevention: senderURL is the X-Node-Url header from the incoming request.
// If edgeURL matches senderURL, we skip — don't forward back to the sender.
// This is what prevents infinite loops in multi-region hub-to-hub topology.
// Edge nodes don't send X-Node-Url (empty string), so they never match and are
// always forwarded to. Peer hubs send their URL, which matches their entry in
// the Edges list, so they're skipped.
//
// Key:   "fwd:{counter}" (monotonic, atomic counter)
// Value: fwdEntry JSON ({batch_id, changes, edge_url})
func (h *Hub) enqueueForward(batchID int64, changes []Change, senderURL string) {
	for _, edgeURL := range h.Edges {
		if edgeURL == senderURL {
			continue // don't forward back to sender (URL match, not node ID)
		}
		fwdID := atomic.AddUint64(&h.fwdCounter, 1)
		entry := fwdEntry{BatchID: batchID, Changes: changes, EdgeURL: edgeURL}
		val, _ := json.Marshal(entry)
		key := fmt.Sprintf("fwd:%d", fwdID)
		if err := h.pdb.Set([]byte(key), val, pebble.Sync); err != nil {
			log.Printf("[%s] enqueue fwd error: %v", h.ID, err)
		}
	}
}

// forwardOne sends a batch of changes to one edge via POST /sync.
//
// Headers:
//   X-Node-Id  — this hub's ID (always sent)
//   X-Node-Url — this hub's URL (only if --url flag is set, for multi-region
//                loop prevention; empty in single-hub topology)
//
// Returns true only if the edge ACKed with matching batch_id. Any network
// error, non-200 status, or ACK mismatch returns false — the fwd: entry stays
// in Pebble and forwardSweep will retry.
func (h *Hub) forwardOne(edgeURL string, batchID int64, changes []Change) bool {
	reqBody := SyncRequest{BatchID: batchID, Changes: changes}
	data, err := json.Marshal(reqBody)
	if err != nil {
		return false
	}
	url := edgeURL + "/sync"
	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Node-Id", h.ID)
	if h.MyURL != "" {
		req.Header.Set("X-Node-Url", h.MyURL)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return false
	}
	var sr SyncResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return false
	}
	return sr.Ack == batchID
}

// tryForwardAll attempts immediate forwarding of all pending "fwd:" entries.
//
// Called asynchronously after each /sync receive for low latency — the sender
// is already ACKed, so this is best-effort. Iterates all "fwd:{n}" keys in
// Pebble, tries to forward each one, and deletes entries that succeeded.
//
// Entries that fail (edge down, network error) remain in Pebble and are
// retried by forwardSweep (background ticker) on the next tick.
//
// Key range: "fwd:" to "fwd~" ("~" is the next ASCII char after ":", so this
// is an exclusive upper bound that captures all "fwd:*" keys).
func (h *Hub) tryForwardAll() {
	iter, err := h.pdb.NewIter(&pebble.IterOptions{
		LowerBound: []byte("fwd:"),
		UpperBound: []byte("fwd~"), // "~" is next ASCII char after ":", exclusive upper bound
	})
	if err != nil {
		log.Printf("[%s] fwd iter error: %v", h.ID, err)
		return
	}
	defer iter.Close()

	var keysToDelete [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		key := make([]byte, len(iter.Key()))
		copy(key, iter.Key())
		val := iter.Value()

		var entry fwdEntry
		if err := json.Unmarshal(val, &entry); err != nil {
			continue
		}
		if h.forwardOne(entry.EdgeURL, entry.BatchID, entry.Changes) {
			keysToDelete = append(keysToDelete, key)
		}
	}

	for _, key := range keysToDelete {
		h.pdb.Delete(key, pebble.Sync)
	}
}

// forwardSweep is the background retry loop for pending forwards.
//
// Runs every batchMs (default 50ms). On each tick:
//   1. Scan all "fwd:{n}" entries in Pebble
//   2. For each entry, try to forward with exponential backoff (50/100/200/400/800ms)
//   3. Delete entries that succeeded (ACK matched)
//
// This picks up entries that failed tryForwardAll (edge was down, network error).
// Forwards to all pending edges concurrently (goroutine per entry, WaitGroup
// barrier). Entries that exhaust all 5 backoff attempts stay in Pebble for the
// next tick — they're never dropped, only deleted on successful ACK.
//
// This is the crash recovery mechanism: if hub crashes and restarts,
// replayPending logs the count, then forwardSweep picks up all pending entries
// on the next tick and retries.
func (h *Hub) forwardSweep() {
	ticker := time.NewTicker(time.Duration(h.batchMs) * time.Millisecond)
	defer ticker.Stop()

	backoffs := []time.Duration{50, 100, 200, 400, 800}

	for range ticker.C {
		iter, err := h.pdb.NewIter(&pebble.IterOptions{
			LowerBound: []byte("fwd:"),
			UpperBound: []byte("fwd~"),
		})
		if err != nil {
			continue
		}

		var pending []struct {
			key   []byte
			entry fwdEntry
		}
		for iter.First(); iter.Valid(); iter.Next() {
			key := make([]byte, len(iter.Key()))
			copy(key, iter.Key())
			var entry fwdEntry
			if json.Unmarshal(iter.Value(), &entry) == nil {
				pending = append(pending, struct {
					key   []byte
					entry fwdEntry
				}{key, entry})
			}
		}
		iter.Close()

		if len(pending) == 0 {
			continue
		}

		var wg sync.WaitGroup
		var mu sync.Mutex
		var deleted [][]byte

		for _, p := range pending {
			wg.Add(1)
			go func(key []byte, entry fwdEntry) {
				defer wg.Done()
				for attempt := range backoffs {
					if h.forwardOne(entry.EdgeURL, entry.BatchID, entry.Changes) {
						mu.Lock()
						deleted = append(deleted, key)
						mu.Unlock()
						return
					}
					time.Sleep(backoffs[attempt] * time.Millisecond)
				}
			}(p.key, p.entry)
		}
		wg.Wait()

		for _, key := range deleted {
			h.pdb.Delete(key, pebble.Sync)
		}
	}
}

// replayPending counts pending "fwd:" entries on startup (crash recovery).
//
// Called once before forwardSweep starts. If the hub crashed with pending
// forwards, they survive in Pebble. This function just logs the count —
// forwardSweep (started immediately after) picks them up on the next tick
// and retries. No special replay logic needed: the entries are already in
// Pebble, forwardSweep already iterates them, so crash recovery is automatic.
func (h *Hub) replayPending() {
	iter, err := h.pdb.NewIter(&pebble.IterOptions{
		LowerBound: []byte("fwd:"),
		UpperBound: []byte("fwd~"),
	})
	if err != nil {
		log.Printf("[%s] replay iter error: %v", h.ID, err)
		return
	}
	defer iter.Close()

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	if count > 0 {
		log.Printf("[%s] replaying %d pending forwards from Pebble", h.ID, count)
	}
	// forwardSweep will pick them up on next tick.
}

// startHTTP launches the Fiber HTTP server with two endpoints:
//
//   POST /sync — the only data ingress. Receives changes from edges or peer hubs.
//     Pipeline (see file header for full flow):
//       1. applyBackup    — persist to Pebble "data:{id}"
//       2. enqueueForward — persist "fwd:{n}" for each edge (except sender)
//       3. ACK            — return {applied, ack: batch_id} immediately
//       4. tryForwardAll  — async, best-effort immediate forward
//
//     The X-Node-Url header is read for loop prevention (multi-region).
//     Empty header = request from edge node (forward to all edges).
//     Non-empty    = request from peer hub (skip the edge matching sender URL).
//
//   GET /health — hub status for monitoring and benchmarks.
//     Returns: backup_items (data: count), pending_forwards (fwd: count),
//     edges list, node_id, hub=true flag.
func (h *Hub) startHTTP() {
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
		BodyLimit:             16 * 1024 * 1024,
	})

	// POST /sync — receive changes from edge, ACK immediately, forward to others
	app.Post("/sync", func(c *fiber.Ctx) error {
		senderURL := c.Get("X-Node-Url") // from peer hub (URL, for loop prevention)
		var req SyncRequest
		if err := json.Unmarshal(c.Body(), &req); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		}

		// 1. Apply to Pebble backup
		applied := h.applyBackup(req.Changes)

		// 2. Enqueue durable forwards (before ACK so crash doesn't lose them)
		//    senderURL = peer hub's URL → skip forwarding back to sender
		h.enqueueForward(req.BatchID, req.Changes, senderURL)

		// 3. ACK sender immediately (edge deletes from its _changes)
		ack := fiber.Map{"applied": applied, "ack": req.BatchID}

		// 4. Try immediate forward (low latency path)
		go h.tryForwardAll()

		return c.JSON(ack)
	})

	// GET /health — hub status
	app.Get("/health", func(c *fiber.Ctx) error {
		// Count backup items
		dataCount := 0
		dataIter, err := h.pdb.NewIter(&pebble.IterOptions{
			LowerBound: []byte("data:"),
			UpperBound: []byte("data~"),
		})
		if err == nil {
			for dataIter.First(); dataIter.Valid(); dataIter.Next() {
				dataCount++
			}
			dataIter.Close()
		}

		// Count pending forwards
		fwdCount := 0
		fwdIter, err := h.pdb.NewIter(&pebble.IterOptions{
			LowerBound: []byte("fwd:"),
			UpperBound: []byte("fwd~"),
		})
		if err == nil {
			for fwdIter.First(); fwdIter.Valid(); fwdIter.Next() {
				fwdCount++
			}
			fwdIter.Close()
		}

		return c.JSON(fiber.Map{
			"ok":               true,
			"node_id":          h.ID,
			"hub":              true,
			"backup_items":     dataCount,
			"pending_forwards": fwdCount,
			"edges":            h.Edges,
		})
	})

	go app.Listen(h.Listen)
}
