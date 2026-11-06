#!/bin/bash
# Live verification of the Go PullConsumerScheduleService (the scheduled PullTask loop) —
# go run -> the service expands one PullTask per assigned queue, the pull loop delivers
# every seed body, and the broker-side offset for every queue advances.
#
# Usage: bash scripts/run_go_pull_schedule_live.sh [namesrv]   (default 127.0.0.1:9876)
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
SEED=${RMQ_GO_PULL_SCHEDULE_SEED:-6}

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
echo "=== Go PullConsumerScheduleService live (seed=${SEED}) ==="
cd "$ROOT/go" || exit 1
"$GO" run ./examples/live_pull_schedule -ns "$NS" -seed "$SEED" 2>&1
GO_RC=$?
echo "--- go exit=$GO_RC ---"

echo ""
if [ "$GO_RC" -eq 0 ]; then
    echo "Result: all passed"
    exit 0
fi
echo "Result: failures detected (go=$GO_RC)"
exit 1
