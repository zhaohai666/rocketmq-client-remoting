#!/bin/bash
# Live verification of the Go client's "immediate shutdown / immediate process
# exit" data-loss contract (counterpart of the Rust live_shutdown_race).
#
# Usage: bash scripts/run_go_shutdown_race_live.sh [namesrv] [legs]    # legs: s1,s2,t, all by default
#
# Why a real cluster is required: every criterion for this contract lands on
# the **broker side** --
#   * whether round A's in-flight send-backs landed can only be decided by
#     whether those 20 original messages are in %RETRY%;
#   * whether messages past the retry cap reached %DLQ% can only be decided
#     by %DLQ%;
#   * whether the last batch of trace data flushed out can only be decided by
#     RMQ_SYS_TRACE_TOPIC.
# In all three cases the client itself can "look all green" (callbacks ran, no
# errors logged), yet once the process exits the messages are gone forever --
# so the criteria must be read back across process and language boundaries.
#
# The broker must have `traceTopicEnable=true`: leg T reads RMQ_SYS_TRACE_TOPIC,
# which the broker pre-creates (already enabled in the /tmp/rmq_rust_live/broker.conf
# used by scripts/rmq_test_broker.sh).
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
LEGS=${2:-${RMQ_SHUTDOWN_RACE_LEGS:-}}
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
    echo "=== broker not listening on $BROKER_PORT, starting local test cluster ==="
    sh "$ROOT/scripts/rmq_test_broker.sh" start || { echo "failed to start broker"; exit 1; }
    STARTED_BROKER=1
    # The port is ready before the broker registers its route with the
    # namesrv; not waiting means the TBW102 route is missing and the run
    # false-fails.
    sleep 12
fi
port_open 9876 || { echo "nameserver 9876 not listening, start namesrv first"; exit 1; }

echo ""
echo "=== Go shutdown-race live (ns=$NS legs=${LEGS:-all}) ==="
echo "    one independent subprocess per stage; exits immediately after Shutdown(); criteria read back from the broker side"
cd "$ROOT/go" || exit 1
if [ -n "$LEGS" ]; then
    "$GO" run ./examples/live_shutdown_race -ns "$NS" -legs "$LEGS"
else
    "$GO" run ./examples/live_shutdown_race -ns "$NS"
fi
RC=$?
echo "--- go exit=$RC ---"

echo ""
if [ "$RC" -eq 0 ]; then
    echo "RESULT: all passed (ns=${NS})"
    exit 0
fi
echo "RESULT: failures present (ns=${NS})"
exit 1
