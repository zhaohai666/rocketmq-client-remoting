#!/bin/bash
# Live verification of the AccessChannel=CLOUD trace routing (python reference probe).
#
# Usage: bash scripts/run_trace_cloud_live.sh [namesrv]
#
# What is being verified (see docs/classic-remoting-capability-checklist.md §10 trace row):
#   Java's AsyncTraceDispatcher routes CLOUD-channel records to
#   "rmq_sys_TRACE_DATA_" + regionId instead of RMQ_SYS_TRACE_TOPIC, falling back
#   to the dispatcher's own channel when the context carries none. The broker
#   stamps MSG_REGION=DefaultRegion both into the send response header and into
#   the message properties, so Pub AND Sub records carry regionId=DefaultRegion.
#   The probe proves, against a real broker:
#     C1  rmq_sys_TRACE_DATA_DefaultRegion is creatable via admin (not in the system topic set);
#     C2  a producer with setAccessChannel(CLOUD) sends and resolves regionId=DefaultRegion;
#     C3  its Pub record lands in the region topic;
#     C4  a default LOCAL producer's Pub record stays in RMQ_SYS_TRACE_TOPIC and does not leak;
#     C5  a CLOUD consumer's SubBefore lands in the region topic;
#     C6  a default LOCAL consumer's SubBefore stays classic and does not leak.
#
# ⚠ Cluster start + port wait + verification + kill must all be chained inside this script:
#   when a foreground command returns, the background JVMs are reaped.
set -u

NS_PORT=${ROCKETMQ_NS_PORT:-9876}
BROKER_PORT=${ROCKETMQ_BROKER_PORT:-10911}
NS=${1:-${ROCKETMQ_NAMESRV:-127.0.0.1:${NS_PORT}}}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DIST=${ROCKETMQ_DIST:-/Users/haizai/project/jingsai/roocketmq/zhaohai666-rocketmq/distribution/target/rocketmq-5.5.1/rocketmq-5.5.1}
WORK=${RMQ_TRACECLOUD_WORK:-/tmp/rmq_tracecloud_live}
PY=${RMQ_PY:-/Users/haizai/.workbuddy/binaries/python/envs/default/bin/python}

port_open() {
    "$PY" -c "import socket,sys; s=socket.socket(); s.settimeout(0.5); sys.exit(0 if s.connect_ex(('127.0.0.1',$1))==0 else 1)" 2>/dev/null
}

STARTED_CLUSTER=0
cleanup() {
    if [ "$STARTED_CLUSTER" = "1" ]; then
        echo "--- cleanup: stopping the cluster started by this script (ports ${NS_PORT}/${BROKER_PORT}) ---"
        # Kill by listening port, not by main class: several verifications may run at the
        # same time on different ports, and `jps | grep BrokerStartup` would take them all down.
        for p in "$BROKER_PORT" "$NS_PORT"; do
            for pid in $(lsof -ti ":$p" 2>/dev/null); do
                kill -15 "$pid" 2>/dev/null || true
            done
        done
        i=0
        while [ "$i" -lt 120 ]; do
            if ! lsof -ti ":$BROKER_PORT" >/dev/null 2>&1 && ! lsof -ti ":$NS_PORT" >/dev/null 2>&1; then
                break
            fi
            sleep 0.5
            i=$((i + 1))
        done
    fi
}
trap cleanup EXIT

# ---------------------------------------------------------------- cluster
if ! port_open "$NS_PORT" || ! port_open "$BROKER_PORT"; then
    echo "=== no cluster on ${NS_PORT}/${BROKER_PORT}, starting one from ${DIST} ==="
    [ -d "$DIST" ] || { echo "distribution not found: $DIST (set ROCKETMQ_DIST)" >&2; exit 2; }
    mkdir -p "$WORK/store"
    cat > "$WORK/namesrv.conf" <<EOF
listenPort = ${NS_PORT}
EOF
    cat > "$WORK/broker.conf" <<EOF
brokerClusterName = DefaultCluster
brokerName = broker-a
brokerId = 0
deleteWhen = 04
fileReservedTime = 48
brokerRole = ASYNC_MASTER
flushDiskType = ASYNC_FLUSH
namesrvAddr = 127.0.0.1:${NS_PORT}
listenPort = ${BROKER_PORT}
storePathRootDir = $WORK/store
autoCreateTopicEnable = true
autoCreateSubscriptionGroup = true
traceTopicEnable = true
# The CLOUD branch writes to rmq_sys_TRACE_DATA_<region>, which the broker
# refuses to create while validateSystemTopicWhenUpdateTopic is on
# (TopicValidator.isSystemTopic matches the "rmq_sys_" prefix, not just the
# SYSTEM_TOPIC_SET). On a real cloud region the platform pre-creates the topic;
# a self-built cluster needs this switch to model that.
validateSystemTopicWhenUpdateTopic = false
EOF
    export ROCKETMQ_HOME="$DIST"
    export JAVA_OPT_EXT="-Xms1g -Xmx2g -Xmn768m -Duser.home=$WORK"
    STARTED_CLUSTER=1
    nohup "$DIST/bin/mqnamesrv" -c "$WORK/namesrv.conf" > "$WORK/ns.log" 2>&1 &
    nohup "$DIST/bin/mqbroker" -c "$WORK/broker.conf" > "$WORK/broker.log" 2>&1 &
    for i in $(seq 1 90); do
        if port_open "$NS_PORT" && port_open "$BROKER_PORT"; then break; fi
        sleep 1
        if [ "$i" = "90" ]; then
            echo "=== cluster not ready within 90s (see $WORK/ns.log / $WORK/broker.log) ===" >&2
            exit 2
        fi
    done
    ready=0
    for i in $(seq 1 90); do
        out=$( cd "$DIST" && ROCKETMQ_HOME="$DIST" sh bin/mqadmin topicRoute \
                 -n "127.0.0.1:${NS_PORT}" -t TBW102 2>/dev/null )
        case "$out" in
            *brokerName*) ready=1; break ;;
        esac
        sleep 1
    done
    if [ "$ready" != "1" ]; then
        echo "=== TBW102 route never became visible (see $WORK/ns.log / $WORK/broker.log) ===" >&2
        exit 2
    fi
fi
port_open "$NS_PORT" || { echo "nameserver ${NS_PORT} not listening" >&2; exit 2; }
port_open "$BROKER_PORT" || { echo "broker ${BROKER_PORT} not listening" >&2; exit 2; }

# ---------------------------------------------------------------- probe
echo ""
echo "=== python: AccessChannel=CLOUD trace live ==="
( cd "$ROOT/python" && "$PY" verify_trace_cloud_live.py "$NS" )
RC=$?

echo ""
if [ "$RC" -eq 0 ]; then
    echo "RESULT: trace CLOUD all green"
    exit 0
fi
echo "RESULT: trace CLOUD has failures"
exit 1
