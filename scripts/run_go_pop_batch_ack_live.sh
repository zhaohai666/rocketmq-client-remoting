#!/bin/bash
# Live verification of "BATCH_ACK_MESSAGE(200151) for the Go client's POP path".
#
# Usage: bash scripts/run_go_pop_batch_ack_live.sh [namesrv] [legs]   # legs: l1,l2,l3, all by default
#
# Why a real cluster is mandatory here: BATCH_ACK_MESSAGE is the one POP command
# the push consumer never sends — its ack path answers message by message — so the
# capability is only reachable through the exported wire primitive. A primitive
# that has never been pointed at a broker is a primitive that may not work, and
# the failure mode is invisible on the return code: the broker answers SUCCESS to a
# batch body whose group key, start offset or bits are wrong. What it does NOT
# answer is the message going away, so every criterion here is a timed observation
# window plus broker-side visible state:
#   * l1 proves one request clears a whole batch: after a single 200151 covering 8
#     checkpoints, nothing may be revived for 2.5x the invisibility window.
#   * l2 is the paired control: the same request with exactly one checkpoint left
#     out must bring that one message back and leave the rest gone. It also closes
#     the retry direction — the revived message's checkpoint is addressed inside
#     %RETRY%<group>_<topic> with the retry marker (the retry topic's route
#     reaching the name server is the broker-side proof), and the batch body that
#     clears it must still name the BUSINESS topic, because the broker re-derives
#     the physical topic from marker + group.
#   * l3 pins the grouping key: two pop rounds of one queue, acked by one request,
#     must produce one entry per (retry, queueId, ck offset, popTime) and no
#     over-merging across rounds.
#
# No broker switch is needed: 200151 is registered unconditionally and the revive
# side handles the batch ack tag the same way, so the standard test cluster config
# already serves it. (enablePopBatchAck only steers the broker's internal re-put of
# buffered acks, which is inert while enablePopBufferMerge=false.)
#
# Cluster: assumes one is already running by default (the scripts/rmq_test_broker.sh
# setup). If the port probe fails, it automatically runs `rmq_test_broker.sh start`,
# and on cleanup stops only the instance it started; anything already running is
# left untouched.
#
# ⚠ A full run takes a couple of minutes: l1 and l3 observe silence for 25s,
# while l2's first window is a probe that stops as soon as the dropped message is
# sighted (its checkpoint must still be inside its own invisibility when the ack
# goes out), followed by a 25s silence window after the marker-driven ack.
set -u

NS=${1:-${ROCKETMQ_NAMESRV:-127.0.0.1:9876}}
LEGS=${2:-${RMQ_POP_LEGS:-}}
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
        echo "--- Done: stopping the broker started by this script ---"
        sh "$ROOT/scripts/rmq_test_broker.sh" stop || true
    fi
}
trap cleanup EXIT

if ! port_open "$BROKER_PORT"; then
    echo "=== broker not listening on ${BROKER_PORT}, starting a local test cluster ==="
    sh "$ROOT/scripts/rmq_test_broker.sh" start || { echo "broker failed to start"; exit 1; }
    STARTED_BROKER=1
    # The port opens before "routes are registered into the namesrv"; without
    # waiting, the topic routes are missing and the run fails spuriously.
    sleep 12
fi
port_open 9876 || { echo "nameserver 9876 not listening; start namesrv first"; exit 1; }

echo ""
echo "=== Go POP batch-ack live (ns=${NS} legs=${LEGS:-all}) ==="
echo "    one request per batch / bitmap honored / retry marker direction / grouping key"
cd "$ROOT/go" || exit 1
if [ -n "$LEGS" ]; then
    "$GO" run ./examples/live_pop_batch_ack -ns "$NS" -legs "$LEGS"
else
    "$GO" run ./examples/live_pop_batch_ack -ns "$NS"
fi
RC=$?
echo "--- go exit=${RC} ---"

echo ""
if [ "$RC" -eq 0 ]; then
    echo "Result: all passed (ns=${NS})"
    exit 0
fi
echo "Result: failures detected (ns=${NS})"
exit 1
