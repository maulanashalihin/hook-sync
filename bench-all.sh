#!/usr/bin/env bash
# bench-all.sh — Run all hook-sync benchmarks in one command.
# Tests all topologies (dual-ack, full mesh, hub, split-brain) across all runtimes (Go, Bun, Node).
# Each runtime is tested independently — servers are stopped and restarted between runtimes.
#
# Usage: bash bench-all.sh
set -uo pipefail

cd "$(dirname "$0")"
ROOT="$(pwd)"

PASS=0
FAIL=0
RESULTS=()

run_bench() {
	local name="$1"
	local script="$2"
	echo
	echo "========================================"
	echo "  RUNNING: $name"
	echo "========================================"
	if bash "$ROOT/$script" 2>&1; then
		PASS=$((PASS + 1))
		RESULTS+=("✅ $name")
	else
		FAIL=$((FAIL + 1))
		RESULTS+=("❌ $name")
	fi
}

# Build all Go binaries first
echo "Building Go binaries..."
cd "$ROOT/go"
go build -o "$ROOT/hook-sync-go" ./cmd/server 2>&1
go build -o "$ROOT/hook-sync-mesh-go" ./cmd/mesh 2>&1
go build -o "$ROOT/hook-sync-hub" ./cmd/hub 2>&1
cd "$ROOT"
echo "Build complete."
echo

echo "############################################"
echo "# hook-sync Full Benchmark Suite"
echo "# Topologies: dual-ack, full mesh, hub, split-brain"
echo "# Runtimes: Go, Bun, Node (each tested independently)"
echo "############################################"

# 1. Dual-writer point-to-point (Go, Bun, Node)
run_bench "Dual-writer (point-to-point)" "bench-dual-ack.sh"

# 2. Full mesh 4 nodes (Go, Bun, Node)
run_bench "Full mesh (4 nodes all-to-all)" "bench-fullmesh.sh"

# 3. Dedicated hub star (Go hub + Go/Bun/Node edges)
run_bench "Dedicated hub (star topology)" "bench-hub.sh"

# 4. Split-brain safety (Go)
run_bench "Split-brain safety" "bench-splitbrain.sh"

# Summary
echo
echo "############################################"
echo "# Benchmark Suite Complete"
echo "# Passed: $PASS | Failed: $FAIL"
echo "############################################"
echo
for r in "${RESULTS[@]}"; do
	echo "  $r"
done
echo

# Cleanup any leftover processes
pkill -9 -f "hook-sync-go|hook-sync-mesh|hook-sync-hub|bun/server|node server" 2>/dev/null || true
rm -f "$ROOT"/*.db "$ROOT"/*.db-wal "$ROOT"/*.db-shm 2>/dev/null || true

[ $FAIL -eq 0 ] && exit 0 || exit 1
