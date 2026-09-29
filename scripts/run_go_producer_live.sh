#!/bin/bash
# Go 生产者的真机验证（跑 Go 发 → 用**已验证的 Python 客户端**回读对拍）。
#
# 用法: bash scripts/run_go_producer_live.sh [namesrv]      （默认 127.0.0.1:9876）
#
# 为什么必须两个语言一起跑：Go 侧只能证明 broker 收下了（SEND_OK）。报文编码错了
# 照样能拿 SEND_OK —— V2 短键头（a..n）写错键名、批量 6 段轻量帧拼错、zlib 压缩位漏置、
# 事务半消息缺 TRAN_MSG/PGROUP，broker 都可能回成功，而消费端拿到的是坏消息。所以
# 判定标准是「另一个语言的实现能不能正确读回来」，这也是四端一直用的口径。
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
TOPIC=${RMQ_GO_TOPIC:-"GoLive_$STAMP"}
GROUP=${RMQ_GO_GROUP:-"GID_GoLive_$STAMP"}

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
    # 端口先于「向 namesrv 注册路由」就绪，不等就会拿不到 TBW102 路由而假失败。
    sleep 12
fi
port_open 9876 || { echo "nameserver 9876 没监听，先起 namesrv"; exit 1; }

echo ""
echo "=== [1/2] Go producer live (topic=${TOPIC} group=${GROUP}) ==="
cd "$ROOT/go" || exit 1
GO_LOG=$(mktemp -t go_live_producer.XXXXXX)
# 回读的期望条数取自 Go 侧自己报的 `SENT=<n>`，不写死：往 live_producer 里加一个
# 检查就会让写死的期望值悄悄失准（少要几条 → 假绿）。
set -o pipefail
"$GO" run ./examples/live_producer -ns "$NS" -topic "$TOPIC" -group "$GROUP" 2>&1 | tee "$GO_LOG"
GO_RC=$?
set +o pipefail
echo "--- go exit=$GO_RC ---"
EXPECT=$(sed -n 's/^PASS=[0-9]* FAIL=[0-9]* SENT=\([0-9][0-9]*\)$/\1/p' "$GO_LOG" | tail -1)
if [ -n "$EXPECT" ]; then
    echo "--- Go 报称已写入 $EXPECT 条，按此数目回读对拍 ---"
else
    EXPECT=8
    echo "--- 没读到 SENT，回退到旧口径 8 条 ---"
fi

echo ""
echo "=== [2/2] Python 客户端回读对拍 ==="
cd "$ROOT/python" || exit 1
"$PY" go_producer_consume_check.py "$TOPIC" "$EXPECT" "GID_PyReadGo_$STAMP" 2>&1 | grep -E "^(PASS|FAIL|[0-9]+$|PASS=)"
PY_RC=${PIPESTATUS[0]}
echo "--- python exit=$PY_RC ---"

echo ""
if [ "$GO_RC" -eq 0 ] && [ "$PY_RC" -eq 0 ]; then
    echo "结果: 全部通过 (topic=$TOPIC)"
    exit 0
fi
echo "结果: 有失败 (go=$GO_RC python=$PY_RC topic=$TOPIC)"
exit 1
