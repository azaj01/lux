#!/bin/bash
set -euo pipefail

BENCH_PORT=${BENCH_PORT:-6390}
REQUESTS=${BENCH_REQUESTS:-1000000}
CLIENTS=${BENCH_CLIENTS:-50}

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

cleanup() {
    [ -n "${SERVER_PID:-}" ] && kill "$SERVER_PID" 2>/dev/null
    [ -n "${TMPDIR_LUX:-}" ] && rm -rf "$TMPDIR_LUX"
    wait 2>/dev/null
} 2>/dev/null
trap cleanup EXIT

wait_for_port() {
    local port=$1
    local name=$2
    for i in $(seq 1 20); do
        if redis-cli -p "$port" PING >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.25
    done
    echo -e "${RED}$name failed to start on port $port${NC}"
    exit 1
}

kill_port() {
    local port=$1
    lsof -ti:"$port" 2>/dev/null | xargs kill -9 2>/dev/null || true
    sleep 0.2
}

if ! command -v redis-benchmark &>/dev/null || ! command -v redis-server &>/dev/null; then
    echo -e "${YELLOW}redis-benchmark and/or redis-server not found. Installing...${NC}"
    if command -v brew &>/dev/null; then
        brew install redis
    elif command -v apt-get &>/dev/null; then
        sudo apt-get update && sudo apt-get install -y redis-tools redis-server
    elif command -v dnf &>/dev/null; then
        sudo dnf install -y redis
    elif command -v pacman &>/dev/null; then
        sudo pacman -S --noconfirm redis
    else
        echo -e "${RED}Cannot auto-install redis. Please install redis-benchmark and redis-server manually.${NC}"
        exit 1
    fi
fi

if ! command -v redis-benchmark &>/dev/null; then
    echo -e "${RED}redis-benchmark still not found after install attempt.${NC}"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LUX_BIN="$SCRIPT_DIR/target/release/lux"

if [ ! -f "$LUX_BIN" ]; then
    echo -e "${YELLOW}Building Lux (release)...${NC}"
    cd "$SCRIPT_DIR"
    cargo build --release
fi

REDIS_VER=$(redis-server --version 2>&1 | head -1 | grep -oE 'v=[0-9]+\.[0-9]+\.[0-9]+' | cut -d= -f2)
LUX_VER=$(grep '^version' "$SCRIPT_DIR/Cargo.toml" | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')

echo -e "${BOLD}=== Lux Benchmark ===${NC}"
echo "    redis-benchmark: $(redis-benchmark --version 2>&1 | head -1)"
echo "    redis-server:    $(redis-server --version 2>&1 | head -1)"
echo "    lux:             v${LUX_VER}"
echo "    requests:        $REQUESTS"
echo "    clients:         $CLIENTS"
echo "    mode:            sequential (one server at a time)"
echo ""

kill_port "$BENCH_PORT"

run_bench() {
    local port=$1
    local pipeline=$2
    local tmpfile=$(mktemp)
    redis-benchmark -p "$port" -t SET -n "$REQUESTS" -c "$CLIENTS" -P "$pipeline" -q >"$tmpfile" 2>/dev/null
    local rps=$(tr '\r' '\n' < "$tmpfile" | grep "requests per second" | grep -oE '[0-9]+\.[0-9]+' | head -1)
    rm -f "$tmpfile"
    echo "${rps:-0}"
}

declare -a LUX_RESULTS
declare -a REDIS_RESULTS
PIPELINES=(1 16 64 128 256 512)

echo -e "${BOLD}Benchmarking Lux...${NC}"
TMPDIR_LUX=$(mktemp -d)
LUX_PORT=$BENCH_PORT LUX_SAVE_INTERVAL=0 LUX_DATA_DIR="$TMPDIR_LUX" "$LUX_BIN" >/dev/null 2>&1 &
SERVER_PID=$!
wait_for_port "$BENCH_PORT" "Lux"

for i in "${!PIPELINES[@]}"; do
    P=${PIPELINES[$i]}
    LUX_RESULTS[$i]=$(run_bench "$BENCH_PORT" "$P")
    echo "  pipeline $P: ${LUX_RESULTS[$i]}"
done

kill "$SERVER_PID" 2>/dev/null
wait "$SERVER_PID" 2>/dev/null || true
rm -rf "$TMPDIR_LUX"
TMPDIR_LUX=""
sleep 1

echo -e "${BOLD}Benchmarking Redis...${NC}"
kill_port "$BENCH_PORT"
redis-server --port "$BENCH_PORT" --save "" --appendonly no --daemonize no --loglevel warning >/dev/null 2>&1 &
SERVER_PID=$!
wait_for_port "$BENCH_PORT" "Redis"

for i in "${!PIPELINES[@]}"; do
    P=${PIPELINES[$i]}
    REDIS_RESULTS[$i]=$(run_bench "$BENCH_PORT" "$P")
    echo "  pipeline $P: ${REDIS_RESULTS[$i]}"
done

kill "$SERVER_PID" 2>/dev/null
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""

echo ""
echo -e "${BOLD}| Pipeline |         Lux |   Redis ${REDIS_VER} | Lux/Redis |${NC}"
echo "|----------|------------:|------------:|----------:|"

for i in "${!PIPELINES[@]}"; do
    P=${PIPELINES[$i]}
    python3 -c "
lux=${LUX_RESULTS[$i]:-0}; red=${REDIS_RESULTS[$i]:-0}
if red > 0:
    ratio = f'{lux/red:.2f}x'
else:
    ratio = 'N/A'

def fmt(n):
    if n >= 1_000_000:
        return f'{n/1_000_000:.2f}M'
    elif n >= 1_000:
        return f'{n/1_000:.0f}K'
    else:
        return f'{n:.0f}'

print(f'| {\"$P\":>8} | {fmt(lux):>11} | {fmt(red):>11} | {ratio:>9} |')
"
done

echo ""
echo -e "${GREEN}Done.${NC}"
