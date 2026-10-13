#!/bin/bash
# Go ProduceAccumulator（autoBatch 自动攒批）的真机验证：Go 攒批发 → 用**已验证的
# Python 客户端**回读对拍。批量帧拼错时 broker 照样回 SEND_OK，所以判定标准仍是
# 「另一个语言能否正确读回」。
#
# 用法: bash scripts/run_go_autobatch_live.sh [namesrv]      （默认 127.0.0.1:9876）
#
# ⚠ 集群 start + 等端口 + 跑验证 + kill 必须都在本脚本内串完：前台返回会回收后台 JVM。
set -u

NS=${1:-${ROCKETMQ_NAMESRV:-127.0.0.1:9876}}
BROKER_PORT=${ROCKETMQ_BROKER_PORT:-10911}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PY=${RMQ_PY:-/Users/haizai/.workbuddy/binaries/python/envs/default/bin/python}
GO=${RMQ_GO:-go}
STAMP=$(date +%s)
TOPIC=${RMQ_GO_TOPIC:-"GoAutoBatchLive_$STAMP"}
GROUP=${RMQ_GO_GROUP:-"GID_GoAutoBatchLive_$STAMP"}

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
echo "=== [1/2] Go autoBatch producer live (topic=${TOPIC} group=${GROUP}) ==="
cd "$ROOT/go" || exit 1
GO_LOG=$(mktemp -t go_live_autobatch.XXXXXX)
set -o pipefail
"$GO" run ./examples/live_autobatch -ns "$NS" -topic "$TOPIC" -group "$GROUP" 2>&1 | tee "$GO_LOG"
GO_RC=$?
set +o pipefail
echo "--- go exit=$GO_RC ---"
EXPECT=$(sed -n 's/^PASS=[0-9]* FAIL=[0-9]* SENT=\([0-9][0-9]*\)$/\1/p' "$GO_LOG" | tail -1)
if [ -n "$EXPECT" ]; then
    echo "--- Go 报称已写入 $EXPECT 条，按此数目回读对拍 ---"
else
    EXPECT=12
    echo "--- 没读到 SENT，回退到旧口径 12 条 ---"
fi

echo ""
echo "=== [2/2] Python 客户端回读对拍 ==="
cd "$ROOT/python" || exit 1
"$PY" go_autobatch_consume_check.py "$TOPIC" "$EXPECT" "GID_PyReadGoAutoBatch_$STAMP" 2>&1 | grep -E "^(PASS|FAIL|[0-9]+$|PASS=)"
PY_RC=${PIPESTATUS[0]}
echo "--- python exit=$PY_RC ---"

echo ""
if [ "$GO_RC" -eq 0 ] && [ "$PY_RC" -eq 0 ]; then
    echo "结果: 全部通过 (topic=$TOPIC)"
    exit 0
fi
echo "结果: 有失败 (go=$GO_RC python=$PY_RC topic=$TOPIC)"
exit 1
