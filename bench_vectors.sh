#!/bin/bash
set -e

PORT=6399
DIM=1536
COUNTS=(1000 10000 50000)

pkill -9 -f "lux.*$PORT" 2>/dev/null || true
sleep 1

cd "$(dirname "$0")"
LUX_PORT=$PORT ./target/release/lux > /dev/null 2>&1 &
LUX_PID=$!
sleep 1

generate_vector() {
    local dim=$1
    local parts=""
    for ((i=0; i<dim; i++)); do
        parts="$parts $(awk 'BEGIN{srand(); printf "%.6f", rand()*2-1}')"
    done
    echo "$parts"
}

echo "Lux Vector Search Benchmark"
echo "Dimension: $DIM"
echo "========================================"

for N in "${COUNTS[@]}"; do
    redis-cli -p $PORT FLUSHALL > /dev/null

    echo ""
    echo "--- $N vectors, ${DIM}d ---"

    VSET_CMD=""
    for ((i=0; i<N; i++)); do
        VSET_CMD="VSET vec:$i $DIM"
        for ((d=0; d<DIM; d++)); do
            VSET_CMD="$VSET_CMD $(awk 'BEGIN{srand('$i$d'); printf "%.4f", rand()*2-1}')"
        done
        VSET_CMD="$VSET_CMD META {\"idx\":$i}"
        echo "$VSET_CMD" >> /tmp/lux_bench_cmds.txt
    done

    echo "Inserting $N vectors..."
    START=$(date +%s%3N)
    cat /tmp/lux_bench_cmds.txt | redis-cli -p $PORT --pipe > /dev/null 2>&1
    END=$(date +%s%3N)
    INSERT_MS=$((END - START))
    echo "  Insert: ${INSERT_MS}ms ($(echo "scale=0; $N * 1000 / $INSERT_MS" | bc) vectors/sec)"
    rm -f /tmp/lux_bench_cmds.txt

    QUERY="VSEARCH $DIM"
    for ((d=0; d<DIM; d++)); do
        QUERY="$QUERY $(awk 'BEGIN{srand('$d'); printf "%.4f", rand()*2-1}')"
    done
    QUERY="$QUERY K 10"

    echo "  Searching (K=10)..."
    START=$(date +%s%3N)
    for ((q=0; q<100; q++)); do
        redis-cli -p $PORT $QUERY > /dev/null 2>&1
    done
    END=$(date +%s%3N)
    SEARCH_MS=$((END - START))
    echo "  Search: ${SEARCH_MS}ms for 100 queries ($(echo "scale=1; $SEARCH_MS / 100" | bc)ms avg)"
done

kill $LUX_PID 2>/dev/null
echo ""
echo "Done."
