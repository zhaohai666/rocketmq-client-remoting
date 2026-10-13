#!/bin/bash
# Go 主动拉取消费者（DefaultMQPullConsumer）的真机验证（跑 Go → 用**已验证的 Python
# 客户端**回读对拍）。
#
# 用法: bash scripts/run_go_pull_live.sh [namesrv]        （默认 127.0.0.1:9876）
#
# 为什么必须两个语言一起跑：Go 侧对 broker 的断言有一半是"自问自答" —— 自己提交位点、
# 自己读回来，报文里 extFields 键名拼错（UPDATE_CONSUMER_OFFSET 的
# consumerGroup/topic/queueId/commitOffset、CONSUMER_SEND_MSG_BACK 的
# group/originTopic/offset/delayLevel/originMsgId/maxReconsumeTimes/unitMode）broker
# 照样可能回 SUCCESS。判定标准是「换一个独立实现、按同一套协议字段名去问，能不能问出同样的
# 结果」（见 python/go_pull_consume_check.py 的 docstring）。
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
TOPIC=${RMQ_GO_PULL_TOPIC:-"GoPullLive_$STAMP"}
GROUP=${RMQ_GO_PULL_GROUP:-"GID_GoPullLive_$STAMP"}
SUSPEND=${RMQ_GO_PULL_SUSPEND:-3s}

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
echo "=== [1/2] Go pull consumer live (topic=${TOPIC} group=${GROUP} suspend=${SUSPEND}) ==="
cd "$ROOT/go" || exit 1
GO_LOG=$(mktemp -t go_live_pull.XXXXXX)
# 回读的期望位点/条数取自 Go 侧自己报的 COMMITTED=/TOTAL=，不写死：往 live_pull 里加
# 一个检查就会让写死的期望值悄悄失准（少要几条 → 假绿）。
set -o pipefail
"$GO" run ./examples/live_pull -ns "$NS" -topic "$TOPIC" -group "$GROUP" -suspend "$SUSPEND" 2>&1 | tee "$GO_LOG"
GO_RC=$?
set +o pipefail
echo "--- go exit=$GO_RC ---"
EXPECT_LINE=$(sed -n 's/^PASS=[0-9]* FAIL=[0-9]* COMMITTED=\([0-9][0-9]*\) TOTAL=\([0-9][0-9]*\)$/\1 \2/p' "$GO_LOG" | tail -1)
if [ -n "$EXPECT_LINE" ]; then
    COMMITTED=${EXPECT_LINE% *}
    TOTAL=${EXPECT_LINE#* }
    echo "--- Go 报称 COMMITTED=$COMMITTED TOTAL=${TOTAL}，按此对拍 ---"
else
    COMMITTED=3
    TOTAL=6
    echo "--- 没读到 COMMITTED/TOTAL，回退到旧口径 3/6 ---"
fi

echo ""
echo "=== [2/2] Python 客户端回读对拍 ==="
cd "$ROOT/python" || exit 1
PY_LOG=$(mktemp -t py_read_go_pull.XXXXXX)
"$PY" go_pull_consume_check.py "$TOPIC" "$GROUP" "$COMMITTED" "$TOTAL" 2>&1 | tee "$PY_LOG" | grep -E "^(PASS|FAIL|PASS=)"
PY_RC=${PIPESTATUS[0]}
# 汇总行只在脚本正常跑完时出现。没有它说明中途抛异常了 —— 那就把完整输出（含栈）
# 打出来，否则被 grep 过滤掉的 traceback 会让人以为"少了一条断言"。
if ! grep -q '^PASS=' "$PY_LOG"; then
    echo "--- 没读到 PASS= 汇总行，完整输出如下（很可能是异常中断）---"
    cat "$PY_LOG"
fi
echo "--- python exit=$PY_RC ---"

echo ""
if [ "$GO_RC" -eq 0 ] && [ "$PY_RC" -eq 0 ]; then
    echo "结果: 全部通过 (topic=$TOPIC group=$GROUP)"
    exit 0
fi
echo "结果: 有失败 (go=$GO_RC python=$PY_RC topic=$TOPIC)"
exit 1
