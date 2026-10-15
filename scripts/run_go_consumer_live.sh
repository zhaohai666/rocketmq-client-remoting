#!/bin/bash
# Live verification of the Go push consumer: **Python produces -> Go consumes
# -> Python reads the broker-side offsets back**.
#
# Usage: bash scripts/run_go_consumer_live.sh [namesrv]      (default 127.0.0.1:9876)
#
# Why two languages must run together: the Go side can only prove "Go itself
# received the messages". The ack only lives in local memory, the offset was
# never really committed to the broker, shutdown never sent UNREGISTER_CLIENT
# -- all of these look green from the Go side and can only be falsified by
# reading back from the broker (via another language's admin API). So the
# criterion is "Go can correctly read back and ack the messages Python wrote,
# and Python can see the offsets on the broker".
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
TOPIC=${RMQ_GO_CONSUMER_TOPIC:-"GoConsumerLive_$STAMP"}
ORDERLY_TOPIC=${RMQ_GO_CONSUMER_ORDERLY_TOPIC:-"GoConsumerLiveOrd_$STAMP"}
GROUP=${RMQ_GO_CONSUMER_GROUP:-"GID_GoConsumerLive_$STAMP"}
COUNT=${RMQ_GO_CONSUMER_COUNT:-12}
ORDERLY_COUNT=${RMQ_GO_CONSUMER_ORDERLY_COUNT:-6}

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
echo "=== [1/3] Python creates topic + produces $COUNT main / $ORDERLY_COUNT orderly ==="
echo "    main topic=${TOPIC} (4 queues)   orderly topic=${ORDERLY_TOPIC} (1 queue)"
cd "$ROOT/python" || exit 1
ROCKETMQ_NAMESRV="$NS" "$PY" go_consumer_feed_check.py \
    produce "$TOPIC" "$COUNT" "$ORDERLY_TOPIC" "$ORDERLY_COUNT" 2>&1 | tail -8
FEED_RC=${PIPESTATUS[0]}
echo "--- feed exit=$FEED_RC ---"

echo ""
echo "=== [2/3] Go push consumer live (topic=${TOPIC} group=${GROUP}) ==="
cd "$ROOT/go" || exit 1
"$GO" run ./examples/live_consumer -ns "$NS" \
    -topic "$TOPIC" -group "$GROUP" -expect "$COUNT" \
    -orderly-topic "$ORDERLY_TOPIC" -orderly-expect "$ORDERLY_COUNT" 2>&1 | tail -40
GO_RC=${PIPESTATUS[0]}
echo "--- go exit=$GO_RC ---"

echo ""
echo "=== [3/3] Python reads back offsets + unregistration state from the broker side ==="
cd "$ROOT/python" || exit 1
ROCKETMQ_NAMESRV="$NS" "$PY" go_consumer_feed_check.py \
    verify "$TOPIC" "$GROUP" "$COUNT" "${GROUP}_unreg" 2>&1 | tail -10
VERIFY_RC=${PIPESTATUS[0]}
echo "--- verify exit=$VERIFY_RC ---"

echo ""
if [ "$FEED_RC" -eq 0 ] && [ "$GO_RC" -eq 0 ] && [ "$VERIFY_RC" -eq 0 ]; then
    echo "RESULT: all passed (topic=$TOPIC group=$GROUP)"
    exit 0
fi
echo "RESULT: failures present (feed=$FEED_RC go=$GO_RC verify=$VERIFY_RC topic=$TOPIC)"
exit 1
