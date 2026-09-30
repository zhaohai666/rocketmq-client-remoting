#!/bin/bash
# Go 客户端的「POP 消费」真机验证。
#
# 用法: bash scripts/run_go_pop_live.sh [namesrv] [legs]   # legs: s1,s2，缺省全跑
#
# 为什么必须真集群：POP 是**唯一一条失败全静默**的消费路径。
#   * 客户端没有位点可回读，没有 send-back 可观察，也没有错误码可断言 —— broker 交出一批
#     「不可见」消息，客户端要么 ACK 要么申请延长不可见时间。POP_CK 差一段（比如把批次起始
#     段 0 当成消息自身段 7），每个 ACK 在 broker 侧都是空操作，而客户端一路报成功；
#     唯一症状是**消息在 popInvisibleTime 之后又回来**。所以判据只能是「计时观察窗口」+
#     「broker 侧可见状态」，不是返回码。
#   * 「ACK 真的落到 broker」只能这样证：消费完 6 条后继续盯着 2.5 倍不可见窗口，断言
#     **没有任何 body 被投第二次**。ACK 若是空操作，broker 会在窗口关闭后 revive 整批，
#     重投必然落在这个窗口内。
#   * 「POP 不提交位点」（与 pull 的本质区别）只能问 broker：ExamineConsumerOffset 必须查不到。
#   * S2 的重投判据有一半在 broker 里：失败消息由 changePopInvisibleTime 延长窗口、由
#     PopReviveService 搬进 %RETRY%<group>_<topic>，再被 pop 回来时 POP_CK 的 retry marker
#     必须是 "1" —— 这既是「它来自重试 topic」的证据，也是「后续 ACK 必须发回重试 topic」
#     的原因（broker 把 checkpoint 记在重试 topic 下）。marker 错成 "0" 就是 S2 要抓的 bug。
#
# 该集群配置里与 POP 相关的硬前提（本地测试 broker.conf 都有，脚本不重复设置）：
#   timerWheelEnable=true                     —— 关掉的话 POP_MESSAGE 直接报错，S1 会硬失败
#   defaultMessageRequestMode=PULL            —— 所以 Start 里的 SET_MESSAGE_REQUEST_MODE(401)
#                                                才是「别再用 PULL 应答这个组」的那一步
#   popResponseReturnActualRetryTopic=false   —— 默认重试路径：broker 自己盖 POP_CK 并把
#                                                msg.Topic 改回业务 topic，S2 正是钉这条
#   enablePopBatchAck=false                   —— 所以本工具**不**跑 BATCH_ACK_MESSAGE(200151)；
#                                                经典 Java 客户端本来也从不发它（逐条 ackAsync）
#
# 集群：默认假定已经在跑（scripts/rmq_test_broker.sh 那套）。端口探测失败会自动
# `rmq_test_broker.sh start`，收工时只停自己起来的那一次；已经在跑的不碰。
#
# ⚠ 集群 start + 等端口 + 跑验证 + kill 必须都在本脚本内串完：前台返回会回收后台 JVM。
# ⚠ 全跑约 1 分钟左右（S1 要静观 13s，S2 要等一次 revive 往返 + 路由刷新）。
set -u

NS=${1:-${ROCKETMQ_NAMESRV:-127.0.0.1:9876}}
LEGS=${2:-${RMQ_POP_LEGS:-}}
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
    # 端口先于「向 namesrv 注册路由」就绪，不等就会拿不到 TBW102 路由而假失败。
    sleep 12
fi
port_open 9876 || { echo "nameserver 9876 没监听，先起 namesrv"; exit 1; }

echo ""
echo "=== Go POP live（ns=${NS} legs=${LEGS:-all}）==="
echo "    POP_CK 重建 / ACK 生效窗口 / 不提交位点 / 改不可见时间 / POP 重试 topic"
cd "$ROOT/go" || exit 1
if [ -n "$LEGS" ]; then
    "$GO" run ./examples/live_pop -ns "$NS" -legs "$LEGS"
else
    "$GO" run ./examples/live_pop -ns "$NS"
fi
RC=$?
echo "--- go exit=${RC} ---"

echo ""
if [ "$RC" -eq 0 ]; then
    echo "结果: 全部通过（ns=${NS}）"
    exit 0
fi
echo "结果: 有失败（ns=${NS}）"
exit 1
