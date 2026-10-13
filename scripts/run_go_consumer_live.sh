#!/bin/bash
# Go push 消费者的真机验证：**Python 生产 → Go 消费 → Python 回读 broker 位点**。
#
# 用法: bash scripts/run_go_consumer_live.sh [namesrv]      （默认 127.0.0.1:9876）
#
# 为什么必须两个语言一起跑：Go 侧只能证明「Go 自己把消息收下了」。ack 只写本地内存、
# 位点没真的提交到 broker、停机没发 UNREGISTER_CLIENT —— 这些在 Go 侧看起来全绿，
# 只有从 broker 侧（另一个语言的 admin 口径）回读才能证伪。所以判定标准是
# 「Python 写的消息 Go 能正确读回并 ack，且 Python 能在 broker 上看到位点」。
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
TOPIC=${RMQ_GO_CONSUMER_TOPIC:-"GoConsumerLive_$STAMP"}
ORDERLY_TOPIC=${RMQ_GO_CONSUMER_ORDERLY_TOPIC:-"GoConsumerLiveOrd_$STAMP"}
GROUP=${RMQ_GO_CONSUMER_GROUP:-"GID_GoConsumerLive_$STAMP"}
COUNT=${RMQ_GO_CONSUMER_COUNT:-12}
ORDERLY_COUNT=${RMQ_GO_CONSUMER_ORDERLY_COUNT:-6}

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
echo "=== [1/3] Python 建 topic + 生产 $COUNT 条主 / $ORDERLY_COUNT 条顺序 ==="
echo "    主 topic=${TOPIC}（4 队列）  顺序 topic=${ORDERLY_TOPIC}（1 队列）"
cd "$ROOT/python" || exit 1
ROCKETMQ_NAMESRV="$NS" "$PY" go_consumer_feed_check.py \
    produce "$TOPIC" "$COUNT" "$ORDERLY_TOPIC" "$ORDERLY_COUNT" 2>&1 | tail -8
FEED_RC=${PIPESTATUS[0]}
echo "--- feed exit=$FEED_RC ---"

echo ""
echo "=== [2/3] Go push consumer live (topic=${TOPIC} group=${GROUP}) ==="
cd "$ROOT/go" || exit 1
"$GO" run ./examples/live_consumer -ns "$NS" \
    -topic "$TOPIC" -group "$GROUP" -expect "$COUNT" \
    -orderly-topic "$ORDERLY_TOPIC" -orderly-expect "$ORDERLY_COUNT" 2>&1 | tail -40
GO_RC=${PIPESTATUS[0]}
echo "--- go exit=$GO_RC ---"

echo ""
echo "=== [3/3] Python 从 broker 侧回读位点 + 注销状态 ==="
cd "$ROOT/python" || exit 1
ROCKETMQ_NAMESRV="$NS" "$PY" go_consumer_feed_check.py \
    verify "$TOPIC" "$GROUP" "$COUNT" "${GROUP}_unreg" 2>&1 | tail -10
VERIFY_RC=${PIPESTATUS[0]}
echo "--- verify exit=$VERIFY_RC ---"

echo ""
if [ "$FEED_RC" -eq 0 ] && [ "$GO_RC" -eq 0 ] && [ "$VERIFY_RC" -eq 0 ]; then
    echo "结果: 全部通过 (topic=$TOPIC group=$GROUP)"
    exit 0
fi
echo "结果: 有失败 (feed=$FEED_RC go=$GO_RC verify=$VERIFY_RC topic=$TOPIC)"
exit 1
