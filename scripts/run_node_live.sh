#!/bin/bash
# Node.js 端的真机验证入口（对照 run_go_*_live.sh / run_php_live.sh 的口径）。
#
# 用法: bash scripts/run_node_live.sh producer|consumer|pull|lite_pull|admin|pop|request_reply|admin_ns|acl|tls|check_config|fixes2 [namesrv]
#
# 集群：默认假定已经在跑（ns+broker 端口都通）。没起会自拉一套（jps 找 pid 收尾，
# 只停自己起来的那一次；已经在跑的不碰——rmq_test_broker.sh 靠 /bin/ps 找 pid，
# 沙箱里被禁，所以这里直接内联起 namesrv+broker）。
#
# ⚠ 集群 start + 等端口 + 跑验证 + kill 必须都在本脚本内串完：前台返回会回收后台 JVM。
set -u

WHICH=${1:-producer}
NS=${2:-${ROCKETMQ_NAMESRV:-127.0.0.1:9876}}
BROKER_PORT=${ROCKETMQ_BROKER_PORT:-10911}
NS_PORT=${ROCKETMQ_NS_PORT:-9876}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
NODEJS_DIR="$ROOT/nodeJs"
NODE_BIN=${NODE_BIN:-/Users/haizai/.workbuddy/binaries/node/versions/22.22.2-6/bin/node}
# fall back to PATH node when the managed binary is missing (e.g. Windows)
if ! [ -x "$NODE_BIN" ] && ! command -v "$NODE_BIN" >/dev/null 2>&1; then
    NODE_BIN=node
fi
DIST=${ROCKETMQ_DIST:-/Users/haizai/project/jingsai/roocketmq/zhaohai666-rocketmq/distribution/target/rocketmq-5.5.1/rocketmq-5.5.1}
WORK=/tmp/rmq_node_live
STAMP=$(date +%s)
export ROCKETMQ_NAMESRV="$NS"

case "$WHICH" in
  producer)      EXAMPLE="examples/live_producer.ts"       ;;
  consumer)      EXAMPLE="examples/live_consumer.ts"       ;;
  pull)          EXAMPLE="examples/live_pull.ts"           ;;
  lite_pull)     EXAMPLE="examples/live_lite_pull.ts"      ;;
  admin)         EXAMPLE="examples/live_admin.ts"          ;;
  pop)           EXAMPLE="examples/live_pop.ts"            ;;
  request_reply) EXAMPLE="examples/live_request_reply.ts"  ;;
  admin_ns)      EXAMPLE="examples/live_admin_ns.ts"       ;;
  acl)           EXAMPLE="examples/live_acl.ts"            ;;
  tls)           EXAMPLE="examples/live_tls.ts"            ;;
  check_config)  EXAMPLE="examples/live_check_client_config.ts" ;;
  fixes2)        EXAMPLE="examples/live_fixes2.ts"              ;;
  slave)         EXAMPLE="examples/live_slave_only.ts"          ;;
  *) echo "unknown example: $WHICH (producer|consumer|pull|lite_pull|admin|pop|request_reply|admin_ns|acl|tls|check_config|fixes2|slave)" >&2 ; exit 2 ;;
esac

port_open() {
    "$NODE_BIN" -e "const s=require('net').createConnection({host:'127.0.0.1',port:$1});s.on('connect',()=>{s.end();process.exit(0)});s.on('error',()=>process.exit(1));setTimeout(()=>process.exit(1),500)" 2>/dev/null
}

STARTED_BROKER=0
cleanup() {
    if [ "$STARTED_BROKER" = "1" ]; then
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

if ! port_open "$NS_PORT" || ! port_open "$BROKER_PORT"; then
    echo "=== 集群未监听（${NS_PORT}/${BROKER_PORT}），拉起本地测试集群 ==="
    [ -d "$DIST" ] || { echo "找不到 RocketMQ 分发目录: ${DIST}（用 ROCKETMQ_DIST 覆盖）" >&2; exit 2; }
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
    STARTED_BROKER=1
    for i in $(seq 1 60); do
        if port_open "$NS_PORT" && port_open "$BROKER_PORT"; then
            echo "=== 集群就绪（等待 ${i}x1s） ==="
            sleep 2
            break
        fi
        sleep 1
        if [ "$i" = "60" ]; then
            echo "=== 集群 60s 未就绪，放弃（见 $WORK/ns.log / $WORK/broker.log） ===" >&2
            exit 2
        fi
    done
    # 端口通了 ≠ TBW102 已注册：路由没进 nameserver 之前 createTopic 会死于
    # "No route info of default topic TBW102"。等一个真正的 route 查询成功。
    for i in $(seq 1 60); do
        if "$NODE_BIN" --experimental-strip-types --no-warnings -e "
            const { MQClient } = await import('$NODEJS_DIR/src/client/mq_client.ts');
            const { RemotingCommand } = await import('$NODEJS_DIR/src/remoting/remotingCommand.ts');
            const { RequestCode, ResponseCode } = await import('$NODEJS_DIR/src/remoting/codes.ts');
            const c = new MQClient('route-wait', '$NS');
            const req = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, null);
            req.addExtField('topic', 'TBW102');
            try {
              const resp = await c.remotingClient.invokeSync('$NS', req, 2000);
              process.exit(resp.code === ResponseCode.SUCCESS ? 0 : 1);
            } catch (e) { process.exit(1); }
        " 2>/dev/null; then
            echo "=== TBW102 路由已注册 ==="
            break
        fi
        sleep 1
        if [ "$i" = "60" ]; then
            echo "=== broker 60s 未注册进 nameserver，放弃 ===" >&2
            exit 2
        fi
    done
fi

echo "=== node live: $EXAMPLE (ns=$NS) ==="
cd "$NODEJS_DIR"
"$NODE_BIN" --experimental-strip-types "$EXAMPLE" --ns "$NS" --stamp "$STAMP"
RC=$?
echo "=== exit=$RC ==="
exit $RC
