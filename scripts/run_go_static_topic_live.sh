#!/bin/bash
# Go 静态主题（513 createStaticTopic + 端点重写）的真机验证：七端口径 S1-S5 的
# Go 版（对应 python/verify_static_topic_live.py 及其余五端 live_static_topic）。
#
# 用法: bash scripts/run_go_static_topic_live.sh [namesrv]  （默认 127.0.0.1:9876）
#
# 场景：S1 单 broker totalQueues=8 物理 4 队列建静态主题 → S2 路由回读 mapping →
# S3 端点集 8 条（0-3→broker、4-7→__syslo__none__）→ S4 实例重写/透传 →
# S5 发 4 收 4。工具内全部绕 broker 一圈回来，单侧自断言即可。
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
TOPIC=${RMQ_GO_STATIC_TOPIC:-"StaticLive_${STAMP}"}

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
echo "=== 静态主题真机验证 (topic=${TOPIC}) ==="
cd "$ROOT/go" || exit 1
"$GO" run ./examples/live_static_topic -ns "$NS" -topic "$TOPIC"
GO_RC=$?

echo ""
if [ "$GO_RC" -eq 0 ]; then
    echo "结果: 全部通过 (topic=${TOPIC})"
    exit 0
fi
echo "结果: 有失败 (go=${GO_RC} topic=${TOPIC})"
exit 1
