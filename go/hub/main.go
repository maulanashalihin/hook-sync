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
// All data enters via /sync. Pebble stores backup + durable forwarding queue.
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
// For DELETE, removes the key. This is the backup copy — not queryable via SQL,
// but scannable by key prefix.
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

// enqueueForward puts a durable forwarding entry in Pebble for each edge.
// Key: "fwd:{counter}" → value: fwdEntry JSON.
// The entry survives hub crash. On restart, replayPending re-sends.
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

// forwardOne sends changes to an edge and returns true on ACK match.
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

// tryForwardAll attempts immediate forwarding of all pending fwd: entries.
// Called after each /sync receive for low latency.
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

// forwardSweep runs in background. Retries pending forwards periodically.
// Picks up entries that failed immediate forward (edge was down).
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

// replayPending re-sends all fwd: entries on startup (crash recovery).
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
