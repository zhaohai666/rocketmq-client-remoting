#!/bin/bash
# Go LitePull 的 topic 队列集合变更监听（1s 周期比对趟次 + 真实扩缩容）的真机验证。
#
# 用法: bash scripts/run_go_lite_queue_change_live.sh [namesrv] （默认 127.0.0.1:9876）
#
# 前置：脚本内部自己建/扩/缩 topic（时间戳隔离），无需手工准备。
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
echo "=== lite_topic_queue_change 真机验证 ==="
cd "$ROOT/go" || exit 1
"$GO" run ./examples/live_lite_topic_queue_change -ns "$NS"
GO_RC=$?

echo ""
if [ "$GO_RC" -eq 0 ]; then
    echo "结果: 全部通过"
    exit 0
fi
echo "结果: 有失败 (go=${GO_RC})"
exit 1
