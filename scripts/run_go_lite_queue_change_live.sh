#!/bin/bash
# Live verification of the Go LitePull topic queue-set change listener
# (1s periodic comparison pass + real scale-out/in).
#
# Usage: bash scripts/run_go_lite_queue_change_live.sh [namesrv] (default 127.0.0.1:9876)
#
# Prerequisite: the script itself creates/expands/shrinks the topic internally
# (timestamp-isolated), no manual setup needed.
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
    sleep 12
fi
port_open 9876 || { echo "nameserver 9876 not listening, start namesrv first"; exit 1; }

echo ""
echo "=== lite_topic_queue_change live verification ==="
cd "$ROOT/go" || exit 1
"$GO" run ./examples/live_lite_topic_queue_change -ns "$NS"
GO_RC=$?

echo ""
if [ "$GO_RC" -eq 0 ]; then
    echo "RESULT: all passed"
    exit 0
fi
echo "RESULT: failures present (go=${GO_RC})"
exit 1
