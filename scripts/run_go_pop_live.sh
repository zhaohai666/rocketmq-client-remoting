#!/bin/bash
# Live verification of "POP consumption" for the Go client.
#
# Usage: bash scripts/run_go_pop_live.sh [namesrv] [legs]   # legs: s1,s2, all legs by default
#
# Why a real cluster is mandatory: POP is the **only consumption path whose failures are fully
# silent**.
#   * The client has no offset to read back, no send-back to observe, and no error code to
#     assert on — the broker hands out a batch of "invisible" messages, and the client either
#     ACKs them or asks to extend the invisibility time. If POP_CK is off by one segment (e.g.
#     treating the batch start segment 0 as the message's own segment 7), every ACK is a no-op
#     on the broker side while the client keeps reporting success; the only symptom is
#     **messages coming back after popInvisibleTime**. So the criteria can only be a "timed
#     observation window" + "broker-side visibility state", not return codes.
#   * "The ACK really landed on the broker" can only be proven this way: after consuming the 6
#     messages, keep watching for 2.5x the invisibility window and assert that **no body is
#     delivered a second time**. If ACK were a no-op, the broker would revive the whole batch
#     once the window closes, and the redelivery would necessarily land inside that window.
#   * "POP does not commit offsets" (the essential difference from pull) can only be asked of
#     the broker: ExamineConsumerOffset must find nothing.
#   * Half of S2's redelivery criteria live in the broker: the failed message's window is
#     extended by changePopInvisibleTime, the message is moved into %RETRY%<group>_<topic> by
#     PopReviveService, and when popped back its POP_CK retry marker must be "1" — this is both
#     the evidence that "it came from the retry topic" and the reason "subsequent ACKs must be
#     sent back to the retry topic" (the broker records the checkpoint under the retry topic).
#     A marker wrongly "0" is exactly the bug S2 is designed to catch.
#
# POP-related hard prerequisites in this cluster config (all present in the local test
# broker.conf; the script does not set them again):
#   timerWheelEnable=true                     —— without it POP_MESSAGE errors outright, S1 hard-fails
#   defaultMessageRequestMode=PULL            —— which is why SET_MESSAGE_REQUEST_MODE(401) in Start
#                                                is the "stop answering this group with PULL" step
#   popResponseReturnActualRetryTopic=false   —— default retry path: the broker itself stamps POP_CK and
#                                                rewrites msg.Topic back to the business topic; S2 pins exactly this
#
# enablePopBatchAck is NOT on that list: it only steers the broker-internal re-put of buffered
# acks, it never gates BATCH_ACK_MESSAGE(200151). This tool still does not send 200151, because
# the POP push consumer acks message by message — the batch command has its own live tool
# (run_go_pop_batch_ack_live.sh).
#
# Cluster: assumes one is already running by default (the scripts/rmq_test_broker.sh setup). If
# the port probe fails, it automatically runs `rmq_test_broker.sh start`, and on cleanup stops
# only the instance it started; anything already running is left untouched.
#
# ⚠ Cluster start + waiting for ports + running verification + kill must all be chained inside
# this script: when a foreground command returns, the background JVMs are reaped.
# ⚠ A full run takes about 1 minute (S1 observes silently for 13s; S2 waits for one revive
# round trip + route refresh).
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
    # The port opens before "routes are registered into the namesrv"; without waiting, TBW102
    # routes are missing and the run fails spuriously.
    sleep 12
fi
port_open 9876 || { echo "nameserver 9876 not listening; start namesrv first"; exit 1; }

echo ""
echo "=== Go POP live (ns=${NS} legs=${LEGS:-all}) ==="
echo "    POP_CK reconstruction / ACK effectiveness window / no offset commit / invisibility change / POP retry topic"
cd "$ROOT/go" || exit 1
if [ -n "$LEGS" ]; then
    "$GO" run ./examples/live_pop -ns "$NS" -legs "$LEGS"
else
    "$GO" run ./examples/live_pop -ns "$NS"
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
