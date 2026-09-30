#!/bin/bash
# Go 客户端的「立即关停 / 立即退进程」丢数据契约真机验证（对标 Rust 的 live_shutdown_race）。
#
# 用法: bash scripts/run_go_shutdown_race_live.sh [namesrv] [legs]    # legs: s1,s2,t，缺省全跑
#
# 为什么必须真集群：这条契约的判据全部落在 **broker 侧**——
#   * A 轮在途 send-back 有没有落地，只能由 %RETRY% 里有没有那 20 条原文来判定；
#   * 越过重试上限的消息有没有进 %DLQ%，只能由 %DLQ% 来判定；
#   * 最后一批 trace 有没有冲出去，只能由 RMQ_SYS_TRACE_TOPIC 来判定。
# 客户端自己在这三种情况下都可能"看起来全绿"（回调跑了、日志没报错），
# 而进程一退，消息就永久消失 —— 所以判据必须跨进程、跨端读回来。
#
# broker 必须 `traceTopicEnable=true`：T 腿要读 RMQ_SYS_TRACE_TOPIC，
# 该 topic 由 broker 预建（scripts/rmq_test_broker.sh 用的 /tmp/rmq_rust_live/broker.conf 已开）。
#
# 集群：默认假定已经在跑（scripts/rmq_test_broker.sh 那套）。端口探测失败会自动
# `rmq_test_broker.sh start`，收工时只停自己起来的那一次；已经在跑的不碰。
#
# ⚠ 集群 start + 等端口 + 跑验证 + kill 必须都在本脚本内串完：前台返回会回收后台 JVM。
set -u

NS=${1:-${ROCKETMQ_NAMESRV:-127.0.0.1:9876}}
LEGS=${2:-${RMQ_SHUTDOWN_RACE_LEGS:-}}
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
echo "=== Go shutdown-race live（ns=$NS legs=${LEGS:-all}）==="
echo "    每个阶段一个独立子进程；Shutdown() 后立刻退进程，判据在 broker 侧读回"
cd "$ROOT/go" || exit 1
if [ -n "$LEGS" ]; then
    "$GO" run ./examples/live_shutdown_race -ns "$NS" -legs "$LEGS"
else
    "$GO" run ./examples/live_shutdown_race -ns "$NS"
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
