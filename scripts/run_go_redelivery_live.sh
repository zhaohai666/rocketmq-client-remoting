#!/bin/bash
# Go 客户端的「重投 / 死信终态 / 部分 ack」真机验证。
#
# 用法: bash scripts/run_go_redelivery_live.sh [namesrv] [legs]   # legs: s1,s2,s3,s4，缺省全跑
#
# 为什么必须真集群：这一片的判据**一半在 broker 里**——
#   * 回投延迟梯度（delayLevel 3/4）、RECONSUME_TIMES 阶梯，由 broker 的回投逻辑决定；
#   * 死信终态的判据是「%DLQ%<group> 里到底有没有那条消息、它的 reconsumeTimes 是几」，
#     只能换一个消费组从队首读回来；客户端这边的计数全绿也可能根本没进 DLQ；
#   * ackIndex 部分 ack 的判据是「已被 ack 的首条一次都不再投」+「业务位点仍整批
#     提交到 3」+「对照组一条都不回投」——三者缺一，回投都可能被误判成 harness 抖动。
#
# 客户端侧最容易踩的坑：从 %RETRY% 拉到的消息，客户端会把 msg.Topic **还原**成业务
# topic（resetRetryTopicAndNamespace），所以按 `topic == %RETRY%<group>` 记账会永远读到 0。
# 本工具一律按 body 记账，并把 listener 实际看到的主题分布打出来。
#
# 集群：默认假定已经在跑（scripts/rmq_test_broker.sh 那套）。端口探测失败会自动
# `rmq_test_broker.sh start`，收工时只停自己起来的那一次；已经在跑的不碰。
#
# ⚠ 集群 start + 等端口 + 跑验证 + kill 必须都在本脚本内串完：前台返回会回收后台 JVM。
# ⚠ 全跑约 5~7 分钟（S2/S3 各自要等 10s/30s 两档延迟 + 观察窗口）。
set -u

NS=${1:-${ROCKETMQ_NAMESRV:-127.0.0.1:9876}}
LEGS=${2:-${RMQ_REDELIVERY_LEGS:-}}
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
    echo "=== broker 未监听 $BROKER_PORT，拉起本地测试集群 ==="
    sh "$ROOT/scripts/rmq_test_broker.sh" start || { echo "broker 起不来"; exit 1; }
    STARTED_BROKER=1
    # 端口先于「向 namesrv 注册路由」就绪，不等就会拿不到 TBW102 路由而假失败。
    sleep 12
fi
port_open 9876 || { echo "nameserver 9876 没监听，先起 namesrv"; exit 1; }

echo ""
echo "=== Go redelivery live（ns=$NS legs=${LEGS:-all}）==="
echo "    %RETRY% 回投 / %DLQ% 终态 / 顺序毒消息 / ackIndex 部分 ack，判据在 broker 侧读回"
cd "$ROOT/go" || exit 1
if [ -n "$LEGS" ]; then
    "$GO" run ./examples/live_redelivery -ns "$NS" -legs "$LEGS"
else
    "$GO" run ./examples/live_redelivery -ns "$NS"
fi
RC=$?
echo "--- go exit=$RC ---"

echo ""
if [ "$RC" -eq 0 ]; then
    echo "结果: 全部通过（ns=${NS}）"
    exit 0
fi
echo "结果: 有失败（ns=${NS}）"
exit 1
