#!/bin/bash
# Live verification of the Go client's "redelivery / dead-letter final state /
# partial ack" behavior.
#
# Usage: bash scripts/run_go_redelivery_live.sh [namesrv] [legs]   # legs: s1,s2,s3,s4, all by default
#
# Why a real cluster is required: half of the criteria for this area live
# **inside the broker** --
#   * the send-back delay gradient (delayLevel 3/4) and the RECONSUME_TIMES
#     ladder are decided by the broker's redelivery logic;
#   * the dead-letter final-state criterion is "whether that message is
#     actually in %DLQ%<group> and what its reconsumeTimes is", which can only
#     be read back from the queue head with a different consumer group; the
#     client-side counters can be all green while nothing ever reached the DLQ;
#   * the ackIndex partial-ack criterion is "the acked first message is never
#     redelivered" + "the business offset is still committed as a whole batch
#     to 3" + "the control group gets zero redeliveries" -- miss any one of the
#     three and a redelivery could be misread as harness jitter.
#
# The easiest client-side pitfall: for messages pulled from %RETRY%, the client
# **restores** msg.Topic to the business topic (resetRetryTopicAndNamespace),
# so bookkeeping by `topic == %RETRY%<group>` always reads 0. This tool always
# books by body and prints the topic distribution the listener actually saw.
#
# Cluster: assumed to be already running by default (the scripts/rmq_test_broker.sh
# setup). If the port probe fails, `rmq_test_broker.sh start` is run automatically,
# and only the instance started here is stopped at cleanup; anything already
# running is left untouched.
#
# Note: cluster start + port wait + verification + kill must all be completed
# serially inside this script: returning to the foreground reaps background JVMs.
# Note: a full run takes about 5-7 minutes (S2/S3 each wait for the 10s/30s
# delay tiers plus the observation window).
set -u

NS=${1:-${ROCKETMQ_NAMESRV:-127.0.0.1:9876}}
LEGS=${2:-${RMQ_REDELIVERY_LEGS:-}}
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
echo "=== Go redelivery live (ns=$NS legs=${LEGS:-all}) ==="
echo "    %RETRY% redelivery / %DLQ% final state / orderly poison message / ackIndex partial ack; criteria read back from the broker side"
cd "$ROOT/go" || exit 1
if [ -n "$LEGS" ]; then
    "$GO" run ./examples/live_redelivery -ns "$NS" -legs "$LEGS"
else
    "$GO" run ./examples/live_redelivery -ns "$NS"
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
