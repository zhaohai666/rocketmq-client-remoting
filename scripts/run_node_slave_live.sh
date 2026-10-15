#!/bin/bash
# Live verification of slave-only consumption on the Node.js client: a
# master+slave two-node cluster -> send messages -> kill the master ->
# a **freshly started** Push consumer must still consume from the slave (fix5).
#
# Usage: bash scripts/run_node_slave_live.sh [namesrv]
#
# Note: starts the full cluster itself (namesrv + master + slave), finds pids
# via jps/lsof at cleanup, and only stops the instance it started (/bin/ps is
# banned in the sandbox). Cluster start + port wait + master kill + two-phase
# verification + kill must all be completed serially inside this script:
# returning to the sandbox foreground reaps background JVMs.
set -u

NS=${1:-${ROCKETMQ_NAMESRV:-127.0.0.1:9876}}
NS_PORT=${ROCKETMQ_NS_PORT:-9876}
MASTER_PORT=${ROCKETMQ_BROKER_PORT:-10911}
SLAVE_PORT=${ROCKETMQ_SLAVE_PORT:-10921}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
NODEJS_DIR="$ROOT/nodeJs"
NODE_BIN=${NODE_BIN:-/Users/haizai/.workbuddy/binaries/node/versions/22.22.2-6/bin/node}
if ! [ -x "$NODE_BIN" ] && ! command -v "$NODE_BIN" >/dev/null 2>&1; then
    NODE_BIN=node
fi
DIST=${ROCKETMQ_DIST:-/Users/haizai/project/jingsai/roocketmq/zhaohai666-rocketmq/distribution/target/rocketmq-5.5.1/rocketmq-5.5.1}
WORK=/tmp/rmq_node_slave_live
STAMP=$(date +%s)
TOTAL=10

port_open() {
    "$NODE_BIN" -e "const s=require('net').createConnection({host:'127.0.0.1',port:$1});s.on('connect',()=>{s.end();process.exit(0)});s.on('error',()=>process.exit(1));setTimeout(()=>process.exit(1),500)" 2>/dev/null
}

STARTED=0
RC=0
cleanup() {
    if [ "$STARTED" = "1" ]; then
        echo "--- cleanup: stopping the cluster started by this script (pids via jps) ---"
        local pids
        pids=$(jps -l 2>/dev/null | awk '/NamesrvStartup|BrokerStartup/ {print $1}')
        if [ -n "$pids" ]; then
            # shellcheck disable=SC2086
            kill $pids 2>/dev/null || true
            sleep 5
            pids=$(jps -l 2>/dev/null | awk '/NamesrvStartup|BrokerStartup/ {print $1}')
            if [ -n "$pids" ]; then
                # shellcheck disable=SC2086
                kill -9 $pids 2>/dev/null || true
            fi
        fi
    fi
}
trap cleanup EXIT

if port_open "$NS_PORT" || port_open "$MASTER_PORT" || port_open "$SLAVE_PORT"; then
    echo "=== slave-only verification needs an exclusive cluster, but something is already running on ${NS_PORT}/${MASTER_PORT}/${SLAVE_PORT} ===" >&2
    exit 2
fi
[ -d "$DIST" ] || { echo "RocketMQ distribution directory not found: ${DIST} (override with ROCKETMQ_DIST)" >&2; exit 2; }

mkdir -p "$WORK/store-master" "$WORK/store-slave"
cat > "$WORK/broker-master.conf" <<EOF
brokerClusterName = DefaultCluster
brokerName = broker-a
brokerId = 0
brokerRole = ASYNC_MASTER
flushDiskType = ASYNC_FLUSH
namesrvAddr = 127.0.0.1:$NS_PORT
listenPort = $MASTER_PORT
storePathRootDir = $WORK/store-master
autoCreateTopicEnable = true
EOF
cat > "$WORK/broker-slave.conf" <<EOF
brokerClusterName = DefaultCluster
brokerName = broker-a
brokerId = 1
brokerRole = SLAVE
flushDiskType = ASYNC_FLUSH
namesrvAddr = 127.0.0.1:$NS_PORT
listenPort = $SLAVE_PORT
storePathRootDir = $WORK/store-slave
autoCreateTopicEnable = true
# BrokerConfig.slaveReadEnable defaults to false: PullMessageProcessor.rejectRequest()
# rejects all pulls when role=SLAVE and the switch is off ([REJECTREQUEST]system busy).
# slave-only consumption requires this broker-side switch anyway (RocketMQ's own
# integration test ContainerIntegrationTestBase configures it the same way);
# it is unrelated to the client implementation.
slaveReadEnable = true
EOF

export ROCKETMQ_HOME="$DIST"
export JAVA_OPT_EXT="-Xms1g -Xmx2g -Xmn768m"
echo "=== starting cluster: namesrv + master(10911) + slave(10921) ==="
nohup "$DIST/bin/mqnamesrv" > "$WORK/ns.log" 2>&1 &
nohup "$DIST/bin/mqbroker" -c "$WORK/broker-master.conf" > "$WORK/broker-master.log" 2>&1 &
nohup "$DIST/bin/mqbroker" -c "$WORK/broker-slave.conf" > "$WORK/broker-slave.log" 2>&1 &
STARTED=1
for i in $(seq 1 90); do
    if port_open "$NS_PORT" && port_open "$MASTER_PORT" && port_open "$SLAVE_PORT"; then
        echo "=== cluster ready (waited ${i}x1s) ==="
        break
    fi
    sleep 1
    if [ "$i" = "90" ]; then
        echo "=== cluster not ready after 90s (see $WORK/ns.log / $WORK/*.log) ===" >&2
        exit 2
    fi
done

cd "$NODEJS_DIR" || exit 1
echo "=== [phase 1] send (master alive) ==="
"$NODE_BIN" --experimental-strip-types examples/live_slave_only.ts \
    --ns "$NS" --stamp "$STAMP" --phase send --total "$TOTAL" || RC=1

echo "=== waiting for HA replication (8s) ==="
sleep 8

echo "=== killing master (SIGTERM -> unregisterBrokerAll, route for brokerId=0 removed immediately) ==="
MPID=$(lsof -t -i ":$MASTER_PORT" -s TCP:LISTEN 2>/dev/null | head -1)
[ -z "$MPID" ] && MPID=$(lsof -t -i ":$MASTER_PORT" 2>/dev/null | head -1)
if [ -z "$MPID" ]; then
    echo "=== master pid not found (lsof -i :$MASTER_PORT) ===" >&2
    exit 2
fi
kill -15 "$MPID"
for i in $(seq 1 30); do
    port_open "$MASTER_PORT" || break
    sleep 1
    if [ "$i" = "30" ]; then
        kill -9 "$MPID" 2>/dev/null || true
        sleep 2
    fi
done
echo "=== master is down, waiting for namesrv to drop the route (6s) ==="
sleep 6

echo "=== [phase 2] consume (slave only, brand-new consumer group) ==="
"$NODE_BIN" --experimental-strip-types examples/live_slave_only.ts \
    --ns "$NS" --stamp "$STAMP" --phase consume --total "$TOTAL" || RC=1

echo "=== exit=$RC ==="
exit $RC
