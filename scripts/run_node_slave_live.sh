#!/bin/bash
# Node.js 端的 slave-only 真机验证：master+slave 双节点集群 → 发消息 →
# 杀 master → **新启动的** Push 消费者必须还能从 slave 消费（fix5）。
#
# 用法: bash scripts/run_node_slave_live.sh [namesrv]
#
# ⚠ 自起全套集群（namesrv + master + slave），jps/lsof 找 pid 收尾，只停自己
#   起来的那一次（/bin/ps 在沙箱里被禁）。集群 start + 等端口 + 杀 master +
#   两阶段验证 + kill 必须全部在本脚本内串完：沙箱前台命令返回会回收后台 JVM。
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
        echo "--- 收工：停掉本脚本起的集群（jps 找 pid） ---"
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
    echo "=== slave-only 验证需要独占集群，但 ${NS_PORT}/${MASTER_PORT}/${SLAVE_PORT} 已有进程在跑 ===" >&2
    exit 2
fi
[ -d "$DIST" ] || { echo "找不到 RocketMQ 分发目录: ${DIST}（用 ROCKETMQ_DIST 覆盖）" >&2; exit 2; }

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
# BrokerConfig.slaveReadEnable 默认 false：PullMessageProcessor.rejectRequest()
# 会在 role=SLAVE 且未开启时拒绝一切 pull（[REJECTREQUEST]system busy）。
# slave-only 消费本来就要求 broker 侧开这个开关（RocketMQ 自家集成测试
# ContainerIntegrationTestBase 也是这么配的），与客户端实现无关。
slaveReadEnable = true
EOF

export ROCKETMQ_HOME="$DIST"
export JAVA_OPT_EXT="-Xms1g -Xmx2g -Xmn768m"
echo "=== 起集群：namesrv + master(10911) + slave(10921) ==="
nohup "$DIST/bin/mqnamesrv" > "$WORK/ns.log" 2>&1 &
nohup "$DIST/bin/mqbroker" -c "$WORK/broker-master.conf" > "$WORK/broker-master.log" 2>&1 &
nohup "$DIST/bin/mqbroker" -c "$WORK/broker-slave.conf" > "$WORK/broker-slave.log" 2>&1 &
STARTED=1
for i in $(seq 1 90); do
    if port_open "$NS_PORT" && port_open "$MASTER_PORT" && port_open "$SLAVE_PORT"; then
        echo "=== 集群就绪（等待 ${i}x1s） ==="
        break
    fi
    sleep 1
    if [ "$i" = "90" ]; then
        echo "=== 集群 90s 未就绪（见 $WORK/ns.log / $WORK/*.log） ===" >&2
        exit 2
    fi
done

cd "$NODEJS_DIR" || exit 1
echo "=== [phase 1] send（master 存活） ==="
"$NODE_BIN" --experimental-strip-types examples/live_slave_only.ts \
    --ns "$NS" --stamp "$STAMP" --phase send --total "$TOTAL" || RC=1

echo "=== 等 HA 复制（8s） ==="
sleep 8

echo "=== 杀 master（SIGTERM → unregisterBrokerAll，路由立刻摘除 brokerId=0） ==="
MPID=$(lsof -t -i ":$MASTER_PORT" -s TCP:LISTEN 2>/dev/null | head -1)
[ -z "$MPID" ] && MPID=$(lsof -t -i ":$MASTER_PORT" 2>/dev/null | head -1)
if [ -z "$MPID" ]; then
    echo "=== 找不到 master pid（lsof -i :$MASTER_PORT） ===" >&2
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
echo "=== master 已下线，等 namesrv 摘除路由（6s） ==="
sleep 6

echo "=== [phase 2] consume（仅剩 slave，全新消费者组） ==="
"$NODE_BIN" --experimental-strip-types examples/live_slave_only.ts \
    --ns "$NS" --stamp "$STAMP" --phase consume --total "$TOTAL" || RC=1

echo "=== exit=$RC ==="
exit $RC
