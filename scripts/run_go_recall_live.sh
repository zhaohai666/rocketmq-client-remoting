#!/bin/bash
# Live verification of the Go recall (RECALL_MESSAGE(370)) path — a timer message
# carries a recallHandle, %RETRY% topics and garbage handles are rejected locally,
# the recall returns the withdrawn message's uniqKey, and the recalled message is
# never delivered while its control timers arrive.
#
# Usage: bash scripts/run_go_recall_live.sh [namesrv]   (default 127.0.0.1:9876)
#
# Cluster: assumes one is already running by default (both ns+broker ports
# reachable). If not, it starts its own inline (finds pids with jps for cleanup,
# stopping only the instance it started; anything already running is left
# untouched — rmq_test_broker.sh locates pids via /bin/ps, which is disabled in
# some sandboxes, so namesrv+broker are started inline here).
#
# ⚠ Cluster start + waiting for ports + running verification + kill must all be chained
# inside this script: when a foreground command returns, the background JVMs are reaped.
set -u

NS=${1:-${ROCKETMQ_NAMESRV:-127.0.0.1:9876}}
NS_PORT=${ROCKETMQ_NS_PORT:-9876}
BROKER_PORT=${ROCKETMQ_BROKER_PORT:-10911}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PY=${RMQ_PY:-/Users/haizai/.workbuddy/binaries/python/envs/default/bin/python}
GO=${RMQ_GO:-go}
DIST=${ROCKETMQ_DIST:-/Users/haizai/project/jingsai/roocketmq/zhaohai666-rocketmq/distribution/target/rocketmq-5.5.1/rocketmq-5.5.1}
WORK=/tmp/rmq_go_recall_live
STARTED=0

port_open() {
    "$PY" -c "import socket,sys; s=socket.socket(); s.settimeout(0.5); sys.exit(0 if s.connect_ex(('127.0.0.1',$1))==0 else 1)" 2>/dev/null
}

cleanup() {
    if [ "$STARTED" = "1" ]; then
        echo "--- Done: stopping the cluster started by this script (finding pids via jps) ---"
        local pids
        pids=$(jps -l 2>/dev/null | awk '/NamesrvStartup|BrokerStartup/ {print $1}')
        if [ -n "$pids" ]; then
            # shellcheck disable=SC2086
            kill $pids 2>/dev/null || true
            sleep 5
            pids=$(jps -l 2>/dev/null | awk '/NamesrvStartup|BrokerStartup/ {print $1}')
            # shellcheck disable=SC2086
            [ -n "$pids" ] && kill -9 $pids 2>/dev/null
        fi
    fi
}
trap cleanup EXIT

if ! port_open "$NS_PORT" || ! port_open "$BROKER_PORT"; then
    echo "=== cluster not listening (${NS_PORT}/${BROKER_PORT}), starting a local test cluster ==="
    [ -d "$DIST" ] || { echo "RocketMQ distribution directory not found: ${DIST} (override with ROCKETMQ_DIST)" >&2; exit 2; }
    mkdir -p "$WORK/store"
    cat > "$WORK/broker.conf" <<EOF
brokerClusterName = DefaultCluster
brokerName = broker-a
brokerId = 0
deleteWhen = 04
fileReservedTime = 48
brokerRole = ASYNC_MASTER
flushDiskType = ASYNC_FLUSH
namesrvAddr = 127.0.0.1:$NS_PORT
listenPort = $BROKER_PORT
storePathRootDir = $WORK/store
autoCreateTopicEnable = true
EOF
    export ROCKETMQ_HOME="$DIST"
    export JAVA_OPT_EXT="-Xms1g -Xmx2g -Xmn768m"
    nohup "$DIST/bin/mqnamesrv" > "$WORK/ns.log" 2>&1 &
    nohup "$DIST/bin/mqbroker" -c "$WORK/broker.conf" > "$WORK/broker.log" 2>&1 &
    STARTED=1
    for i in $(seq 1 60); do
        if port_open "$NS_PORT" && port_open "$BROKER_PORT"; then
            echo "=== cluster ready (waited ${i}x1s) ==="
            sleep 2
            break
        fi
        sleep 1
        if [ "$i" = "60" ]; then
            echo "=== cluster not ready within 60s, giving up (see $WORK/ns.log / $WORK/broker.log) ===" >&2
            exit 2
        fi
    done
    # Port open ≠ TBW102 registered: before the route reaches the nameserver, createTopic
    # dies with "No route info of default topic TBW102". Wait for a real route query.
    for i in $(seq 1 60); do
        if php "$ROOT/php/examples/wait_broker_route.php" "127.0.0.1:$NS_PORT" 5 2>/dev/null; then
            echo "=== TBW102 route registered ==="
            break
        fi
        sleep 1
        if [ "$i" = "60" ]; then
            echo "=== broker not registered into nameserver within 60s, giving up ===" >&2
            exit 2
        fi
    done
fi

echo ""
echo "=== Go recall live (window = timer delay 12s + 20s observation) ==="
cd "$ROOT/go" || exit 1
"$GO" run ./examples/live_recall -ns "$NS" 2>&1
GO_RC=$?
echo "--- go exit=$GO_RC ---"

echo ""
if [ "$GO_RC" -eq 0 ]; then
    echo "Result: all passed"
    exit 0
fi
echo "Result: failures detected (go=$GO_RC)"
exit 1
