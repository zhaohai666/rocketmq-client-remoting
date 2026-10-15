#!/bin/bash
# Live verification of the Go producer (Go sends -> a **verified Python client**
# reads back for cross-checking).
#
# Usage: bash scripts/run_go_producer_live.sh [namesrv]      (default 127.0.0.1:9876)
#
# Why two languages must run together: the Go side can only prove the broker
# accepted the messages (SEND_OK). A malformed wire encoding still gets SEND_OK
# -- wrong V2 short-key header (a..n) key names, a broken 6-section lightweight
# batch frame, a missing zlib compression flag, a transaction half message
# without TRAN_MSG/PGROUP -- the broker may still return success while the
# consumer receives a corrupt message. So the criterion is "can another
# implementation read it back correctly".
#
# Cluster: assumed to be already running by default (the scripts/rmq_test_broker.sh
# setup). If the port probe fails, `rmq_test_broker.sh start` is run automatically,
# and only the instance started here is stopped at cleanup; anything already
# running is left untouched.
#
# Note: cluster start + port wait + verification + kill must all be completed
# serially inside this script: returning to the foreground reaps background JVMs.
set -u

NS=${1:-${ROCKETMQ_NAMESRV:-127.0.0.1:9876}}
BROKER_PORT=${ROCKETMQ_BROKER_PORT:-10911}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PY=${RMQ_PY:-/Users/haizai/.workbuddy/binaries/python/envs/default/bin/python}
GO=${RMQ_GO:-go}
STAMP=$(date +%s)
TOPIC=${RMQ_GO_TOPIC:-"GoLive_$STAMP"}
GROUP=${RMQ_GO_GROUP:-"GID_GoLive_$STAMP"}

port_open() {
    "$PY" -c "import socket,sys; s=socket.socket(); s.settimeout(0.5); sys.exit(0 if s.connect_ex(('127.0.0.1',$1))==0 else 1)" 2>/dev/null
}

STARTED_BROKER=0
cleanup() {
    if [ "$STARTED_BROKER" = "1" ]; then
        echo "--- cleanup: stopping the broker started by this script ---"
        sh "$ROOT/scripts/rmq_test_broker.sh" stop || true
    fi
}
trap cleanup EXIT

if ! port_open "$BROKER_PORT"; then
    echo "=== broker not listening on ${BROKER_PORT}, starting local test cluster ==="
    sh "$ROOT/scripts/rmq_test_broker.sh" start || { echo "failed to start broker"; exit 1; }
    STARTED_BROKER=1
    # The port is ready before the broker registers its route with the
    # namesrv; not waiting means the TBW102 route is missing and the run
    # false-fails.
    sleep 12
fi
port_open 9876 || { echo "nameserver 9876 not listening, start namesrv first"; exit 1; }

echo ""
echo "=== [1/2] Go producer live (topic=${TOPIC} group=${GROUP}) ==="
cd "$ROOT/go" || exit 1
GO_LOG=$(mktemp -t go_live_producer.XXXXXX)
# The expected count for read-back comes from the Go side's own `SENT=<n>`
# report instead of being hardcoded: adding one check to live_producer would
# silently invalidate a hardcoded expectation (fewer messages requested ->
# false green).
set -o pipefail
"$GO" run ./examples/live_producer -ns "$NS" -topic "$TOPIC" -group "$GROUP" 2>&1 | tee "$GO_LOG"
GO_RC=$?
set +o pipefail
echo "--- go exit=$GO_RC ---"
EXPECT=$(sed -n 's/^PASS=[0-9]* FAIL=[0-9]* SENT=\([0-9][0-9]*\)$/\1/p' "$GO_LOG" | tail -1)
if [ -n "$EXPECT" ]; then
    echo "--- Go reported $EXPECT messages sent; reading back that many for cross-check ---"
else
    EXPECT=8
    echo "--- SENT not found, falling back to the legacy count of 8 ---"
fi

echo ""
echo "=== [2/2] Python client read-back cross-check ==="
cd "$ROOT/python" || exit 1
"$PY" go_producer_consume_check.py "$TOPIC" "$EXPECT" "GID_PyReadGo_$STAMP" 2>&1 | grep -E "^(PASS|FAIL|[0-9]+$|PASS=)"
PY_RC=${PIPESTATUS[0]}
echo "--- python exit=$PY_RC ---"

echo ""
if [ "$GO_RC" -eq 0 ] && [ "$PY_RC" -eq 0 ]; then
    echo "RESULT: all passed (topic=$TOPIC)"
    exit 0
fi
echo "RESULT: failures present (go=$GO_RC python=$PY_RC topic=$TOPIC)"
exit 1
