#!/bin/bash
# Node.js 端的真机验证入口（对照 run_go_*_live.sh 的口径）。
#
# 用法: bash scripts/run_node_live.sh producer|consumer|pull|lite_pull|admin [namesrv]
#
# 集群：默认假定已经在跑（scripts/rmq_test_broker.sh 那套）。端口探测失败会自动
# `rmq_test_broker.sh start`，并在收工时只停自己起来的那一次；已经在跑的不碰。
#
# ⚠ 集群 start + 等端口 + 跑验证 + kill 必须都在本脚本内串完：前台返回会回收后台 JVM。
set -u

WHICH=${1:-producer}
NS=${2:-${ROCKETMQ_NAMESRV:-127.0.0.1:9876}}
BROKER_PORT=${ROCKETMQ_BROKER_PORT:-10911}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
NODEJS_DIR="$ROOT/nodeJs"
NODE_BIN=${NODE_BIN:-/Users/haizai/.workbuddy/binaries/node/versions/22.22.2-3/bin/node}
STAMP=$(date +%s)
export ROCKETMQ_NAMESRV="$NS"

case "$WHICH" in
  producer)  EXAMPLE="examples/live_producer.ts"  ;;
  consumer)  EXAMPLE="examples/live_consumer.ts"  ;;
  pull)      EXAMPLE="examples/live_pull.ts"      ;;
  lite_pull) EXAMPLE="examples/live_lite_pull.ts" ;;
  admin)     EXAMPLE="examples/live_admin.ts"     ;;
  *) echo "unknown example: $WHICH (producer|consumer|pull|lite_pull|admin)" >&2; exit 2 ;;
esac

port_open() {
    "$NODE_BIN" -e "const s=require('net').createConnection({host:'127.0.0.1',port:${BROKER_PORT}});s.on('connect',()=>{s.end();process.exit(0)});s.on('error',()=>process.exit(1));setTimeout(()=>process.exit(1),500)" 2>/dev/null
}

STARTED_BROKER=0
cleanup() {
    if [ "$STARTED_BROKER" = "1" ]; then
        echo "--- 收工：停掉本脚本起的 broker ---"
        sh "$ROOT/scripts/rmq_test_broker.sh" stop || true
    fi
}
trap cleanup EXIT

if ! port_open; then
    echo "=== broker 未监听 ${BROKER_PORT}，拉起本地测试集群 ==="
    sh "$ROOT/scripts/rmq_test_broker.sh" start
    STARTED_BROKER=1
fi

echo "=== node live: $EXAMPLE (ns=$NS) ==="
cd "$NODEJS_DIR"
"$NODE_BIN" --experimental-strip-types "$EXAMPLE" --ns "$NS" --stamp "$STAMP"
RC=$?
echo "=== exit=$RC ==="
exit $RC
