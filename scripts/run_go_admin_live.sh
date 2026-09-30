#!/bin/bash
# Go 管理端（DefaultMQAdminExt）的真机验证。
#
# 用法: bash scripts/run_go_admin_live.sh [namesrv]      （默认 127.0.0.1:9876）
#
# 为什么必须真机跑：管理端库（go/client/admin*.go）此前只有 35 条单测，而单测用的
# 假 broker 不校验任何东西 —— 报文编码错了照样能拿到 SUCCESS。本脚本第一次跑就
# 抓到一个真实缺陷：`SubscriptionGroupConfig.groupRetryPolicy` 为 nil 时序列化直接
# panic（单测从来都是走 `NewSubscriptionGroupConfig()` 构造、字段非 nil，所以从没
# 覆盖到"手工构造 config"这条路径）。
#
# 判定口径：工具内所有断言都是**绕 broker 一圈回来**的 —— 建完 topic 读路由/配置/
# topic 列表、写完 KV 再读、写完订阅组再读、写位点再用另一个 RPC 读回、按 KEYS 索引
# 查消息再 viewMessage 取正文、删完 topic 再列一次确认消失。所以 Go 单侧自断言即可
# （不需要另一个语言回读对拍，那是 producer/consumer 那类"线上格式另解一遍"的场景）。
#
# 集群：默认假定已经在跑（scripts/rmq_test_broker.sh 那套）。端口探测失败会自动
# `rmq_test_broker.sh start`，并在收工时只停自己起来的那一次；已经在跑的不碰。
# 需要一个含**从节点**的集群才能覆盖 master/slave 地址表 —— 本脚本用的就是默认的
# rmq_test_broker.sh（broker-a master 10911 + slave 10931）。
#
# ⚠ 集群 start + 等端口 + 跑验证 + kill 必须都在本脚本内串完：前台返回会回收后台 JVM。
set -u

NS=${1:-${ROCKETMQ_NAMESRV:-127.0.0.1:9876}}
BROKER_PORT=${ROCKETMQ_BROKER_PORT:-10911}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PY=${RMQ_PY:-/Users/haizai/.workbuddy/binaries/python/envs/default/bin/python}
GO=${RMQ_GO:-go}
STAMP=$(date +%s)
TOPIC=${RMQ_GO_ADMIN_TOPIC:-"GoAdmin_${STAMP}"}
GROUP=${RMQ_GO_ADMIN_GROUP:-"GID_GoAdmin_${STAMP}"}

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
echo "=== 管理端真机验证 (topic=${TOPIC} group=${GROUP}) ==="
cd "$ROOT/go" || exit 1
"$GO" run ./examples/live_admin -ns "$NS" -topic "$TOPIC" -group "$GROUP"
GO_RC=$?

echo ""
if [ "$GO_RC" -eq 0 ]; then
    echo "结果: 全部通过 (topic=${TOPIC})"
    exit 0
fi
echo "结果: 有失败 (go=${GO_RC} topic=${TOPIC})"
exit 1
