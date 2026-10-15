#!/bin/bash
# Live verification of the Go active-pull consumer (DefaultMQPullConsumer) (run Go -> read
# back and cross-check with the **already-verified Python client**).
#
# Usage: bash scripts/run_go_pull_live.sh [namesrv]        (default 127.0.0.1:9876)
#
# Why both languages must run together: half of the Go side's assertions against the broker
# are "self-asking and self-answering" — it commits offsets itself and reads them back itself.
# If the extFields key names in the request are misspelled (UPDATE_CONSUMER_OFFSET's
# consumerGroup/topic/queueId/commitOffset, CONSUMER_SEND_MSG_BACK's
# group/originTopic/offset/delayLevel/originMsgId/maxReconsumeTimes/unitMode), the broker may
# still reply SUCCESS. The verdict standard is "ask with an independent implementation using
# the same set of protocol field names and see whether the same result comes out" (see the
# docstring of python/go_pull_consume_check.py).
#
# Cluster: assumes one is already running by default (the scripts/rmq_test_broker.sh setup).
# If the port probe fails, it automatically runs `rmq_test_broker.sh start`, and on cleanup
# stops only the instance it started; anything already running is left untouched.
#
# ⚠ Cluster start + waiting for ports + running verification + kill must all be chained inside
# this script: when a foreground command returns, the background JVMs are reaped.
set -u

NS=${1:-${ROCKETMQ_NAMESRV:-127.0.0.1:9876}}
BROKER_PORT=${ROCKETMQ_BROKER_PORT:-10911}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PY=${RMQ_PY:-/Users/haizai/.workbuddy/binaries/python/envs/default/bin/python}
GO=${RMQ_GO:-go}
STAMP=$(date +%s)
TOPIC=${RMQ_GO_PULL_TOPIC:-"GoPullLive_$STAMP"}
GROUP=${RMQ_GO_PULL_GROUP:-"GID_GoPullLive_$STAMP"}
SUSPEND=${RMQ_GO_PULL_SUSPEND:-3s}

port_open() {
    "$PY" -c "import socket,sys; s=socket.socket(); s.settimeout(0.5); sys.exit(0 if s.connect_ex(('127.0.0.1',$1))==0 else 1)" 2>/dev/null
}

STARTED_BROKER=0
cleanup() {
    if [ "$STARTED_BROKER" = "1" ]; then
        echo "--- Done: stopping the broker started by this script ---"
        sh "$ROOT/scripts/rmq_test_broker.sh" stop || true
    fi
}
trap cleanup EXIT

if ! port_open "$BROKER_PORT"; then
    echo "=== broker not listening on ${BROKER_PORT}, starting a local test cluster ==="
    sh "$ROOT/scripts/rmq_test_broker.sh" start || { echo "broker failed to start"; exit 1; }
    STARTED_BROKER=1
    # The port opens before "routes are registered into the namesrv"; without waiting, TBW102
    # routes are missing and the run fails spuriously.
    sleep 12
fi
port_open 9876 || { echo "nameserver 9876 not listening; start namesrv first"; exit 1; }

echo ""
echo "=== [1/2] Go pull consumer live (topic=${TOPIC} group=${GROUP} suspend=${SUSPEND}) ==="
cd "$ROOT/go" || exit 1
GO_LOG=$(mktemp -t go_live_pull.XXXXXX)
# The expected offset/count for read-back come from the Go side's own COMMITTED=/TOTAL= report,
# not hardcoded: adding one more check inside live_pull would silently invalidate hardcoded
# expectations (asking for a few fewer -> false green).
set -o pipefail
"$GO" run ./examples/live_pull -ns "$NS" -topic "$TOPIC" -group "$GROUP" -suspend "$SUSPEND" 2>&1 | tee "$GO_LOG"
GO_RC=$?
set +o pipefail
echo "--- go exit=$GO_RC ---"
EXPECT_LINE=$(sed -n 's/^PASS=[0-9]* FAIL=[0-9]* COMMITTED=\([0-9][0-9]*\) TOTAL=\([0-9][0-9]*\)$/\1 \2/p' "$GO_LOG" | tail -1)
if [ -n "$EXPECT_LINE" ]; then
    COMMITTED=${EXPECT_LINE% *}
    TOTAL=${EXPECT_LINE#* }
    echo "--- Go reports COMMITTED=$COMMITTED TOTAL=${TOTAL}, cross-checking against it ---"
else
    COMMITTED=3
    TOTAL=6
    echo "--- COMMITTED/TOTAL not found, falling back to the old baseline 3/6 ---"
fi

echo ""
echo "=== [2/2] Python client read-back cross-check ==="
cd "$ROOT/python" || exit 1
PY_LOG=$(mktemp -t py_read_go_pull.XXXXXX)
"$PY" go_pull_consume_check.py "$TOPIC" "$GROUP" "$COMMITTED" "$TOTAL" 2>&1 | tee "$PY_LOG" | grep -E "^(PASS|FAIL|PASS=)"
PY_RC=${PIPESTATUS[0]}
# The summary line only appears when the script finishes normally. Without it something threw
# mid-run — print the full output (with the stack); otherwise the traceback filtered out by
# grep would make people think "one assertion fewer".
if ! grep -q '^PASS=' "$PY_LOG"; then
    echo "--- PASS= summary line not found; full output below (most likely aborted by an exception) ---"
    cat "$PY_LOG"
fi
echo "--- python exit=$PY_RC ---"

echo ""
if [ "$GO_RC" -eq 0 ] && [ "$PY_RC" -eq 0 ]; then
    echo "Result: all passed (topic=$TOPIC group=$GROUP)"
    exit 0
fi
echo "Result: failures detected (go=$GO_RC python=$PY_RC topic=$TOPIC)"
exit 1
