#!/bin/bash
# Go 批量 admin（go/client/admin_batch.go）的真机验证：18/225 批量配置、
# 513 静态主题、353 读禁配、323 half 恢复、ORDER_TOPIC_CONFIG KV、306/329 收尾。
#
# 用法: bash scripts/run_go_admin_batch_live.sh [namesrv]   （默认 127.0.0.1:9876）
#
# 判定口径：全部是「绕 broker 一圈回来」的自断言（建了再读、写了再读、禁了再查），
# 单侧跑即可，不需要另一语言对拍。
#
# 集群：默认假定已经在跑（scripts/rmq_test_broker.sh 那套）。端口探测失败会自动
# `rmq_test_broker.sh start`，并在收工时只停自己起来的那一次；已经在跑的不碰。
#
# ⚠ 集群 start + 等端口 + 跑验证 + kill 必须都在本脚本内串完：前台返回会回收后台 JVM。
set -u

NS=${1:-${ROCKETMQ_NAMESRV:-127.0.0.1:9876}}
BROKER_PORT=${ROCKETMQ_BROKER_PORT:-10911}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PY=${RMQ_PY:-/Users/haizai/.workbuddy/binaries/python/envs/default/bin/python}
GO=${RMQ_GO:-go}
STAMP=$(date +%s)
TOPIC_A=${RMQ_GO_BATCH_TOPIC_A:-"GoBatchA_${STAMP}"}
TOPIC_B=${RMQ_GO_BATCH_TOPIC_B:-"GoBatchB_${STAMP}"}
GROUP_A=${RMQ_GO_BATCH_GROUP_A:-"GID_GoBatchA_${STAMP}"}
GROUP_B=${RMQ_GO_BATCH_GROUP_B:-"GID_GoBatchB_${STAMP}"}

port_open() {
    "$PY" -c "import socket,sys; s=socket.socket(); s.settimeout(0.5); sys.exit(0 if s.connect_ex(('127.0.0.1',$1))==0 else 1)" 2>/dev/null
}

STARTED_BROKER=0
cleanup() {
    if [ "$STARTED_BROKER" = "1" ]; then
        echo "--- 收工：停掉本脚本起的 broker ---"
        sh "$ROOT/scripts/rmq_test_broker.sh" stop || true
    fi
}
trap cleanup EXIT

if ! port_open "$BROKER_PORT"; then
    echo "=== broker 未监听 ${BROKER_PORT}，拉起本地测试集群 ==="
    sh "$ROOT/scripts/rmq_test_broker.sh" start || { echo "broker 起不来"; exit 1; }
    STARTED_BROKER=1
    sleep 12
fi
port_open 9876 || { echo "nameserver 9876 没监听，先起 namesrv"; exit 1; }

echo ""
echo "=== admin_batch 真机验证 (topics=${TOPIC_A},${TOPIC_B}) ==="
cd "$ROOT/go" || exit 1
"$GO" run ./examples/live_admin_batch -ns "$NS" -topic-a "$TOPIC_A" -topic-b "$TOPIC_B" \
    -group-a "$GROUP_A" -group-b "$GROUP_B"
GO_RC=$?

echo ""
if [ "$GO_RC" -eq 0 ]; then
    echo "结果: 全部通过 (topics=${TOPIC_A},${TOPIC_B})"
    exit 0
fi
echo "结果: 有失败 (go=${GO_RC})"
exit 1
