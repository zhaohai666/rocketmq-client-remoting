#!/bin/bash
# Live verification of the Go static topic (513 createStaticTopic + endpoint
# rewrite): the Go version of S1-S5 (counterpart of python/verify_static_topic_live.py
# and the other five live_static_topic implementations).
#
# Usage: bash scripts/run_go_static_topic_live.sh [namesrv]  (default 127.0.0.1:9876)
#
# Scenario: S1 single broker, totalQueues=8 with 4 physical queues, create static
# topic -> S2 read back route mapping -> S3 endpoint set of 8 (0-3->broker,
# 4-7->__syslo__none__) -> S4 instance rewrite/passthrough -> S5 send 4, receive 4.
# Everything loops back through the broker inside the tool, so single-sided
# self-assertion is enough.
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
TOPIC=${RMQ_GO_STATIC_TOPIC:-"StaticLive_${STAMP}"}

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
echo "=== static topic live verification (topic=${TOPIC}) ==="
cd "$ROOT/go" || exit 1
"$GO" run ./examples/live_static_topic -ns "$NS" -topic "$TOPIC"
GO_RC=$?

echo ""
if [ "$GO_RC" -eq 0 ]; then
    echo "RESULT: all passed (topic=${TOPIC})"
    exit 0
fi
echo "RESULT: failures present (go=${GO_RC} topic=${TOPIC})"
exit 1
