#!/bin/bash
# 集群开启 ACL 时的「全部消息类型收发」真机验证（七端一条命令跑完）。
#
# 为什么要有这个脚本：ACL 的假通过太多了 —— broker 没开鉴权时正向场景照样全绿，
# 只有「无凭据必须被拒(NO_PERMISSION=16)」这一半断言才戳得穿。所以每一端都是成对跑：
# 先无凭据探一次必须被拒，再带签名把同步 / 异步 / 单向 / 批量 / 延迟等级 / 定时 /
# 事务 / 顺序 / Tag 过滤 / 用户属性 / KEYS 服务端查询逐条发出去并从 broker 读回。
#
# 用法:
#   bash scripts/run_acl_message_types_live.sh                 # 假定集群已按下面的
#                                                              # authentication* 配置起好
#   bash scripts/run_acl_message_types_live.sh python cpp      # 只跑某几端
#
# 依赖的环境变量（都有默认值，指向本机集群）：
#   ROCKETMQ_NAMESRV   namesrv 地址（默认 127.0.0.1:9876）
#   RMQ_ACL_AK / RMQ_ACL_SK   与 broker initAuthenticationUser 一致的凭据
#   RMQ_PY             python 解释器（默认 python3）
#   RMQ_CPP_BUILD      cpp 的 cmake 构建目录（默认 cpp/build）
#   RMQ_DOTNET         dotnet 可执行文件
#   LOGDIR             各端输出落盘目录（默认 /tmp/rmq_acl_types_live_logs）
#
# 集群没起时按下面的 broker.conf 关键三项拉起（缺 authenticationMetadataProvider
# 的话 initAuthenticationUser 是白写的，所有请求都会被拒）：
#   authenticationEnabled=true
#   authenticationMetadataProvider=org.apache.rocketmq.auth.authentication.provider.LocalAuthenticationMetadataProvider
#   initAuthenticationUser={"username":"<AK>","password":"<SK>"}
set -u

NS=${ROCKETMQ_NAMESRV:-127.0.0.1:9876}
AK=${RMQ_ACL_AK:-AK_TEST}
SK=${RMQ_ACL_SK:-SK_TEST_SECRET_12345678}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PY=${RMQ_PY:-python3}
CPP_BUILD=${RMQ_CPP_BUILD:-$ROOT/cpp/build}
DOTNET=${RMQ_DOTNET:-dotnet}
LOGDIR=${LOGDIR:-/tmp/rmq_acl_types_live_logs}
mkdir -p "$LOGDIR"

ENDS=("$@")
[ ${#ENDS[@]} -eq 0 ] && ENDS=(python cpp csharp go nodejs php rust)
if [ "${ENDS[0]}" = "all" ]; then ENDS=(python cpp csharp go nodejs php rust); fi

# 每一端都要求带凭据跑，缺凭据一律记 FAIL 而不是跳过：跳过会让「没鉴权」的集群
# 看起来和「鉴权全绿」的集群一模一样。
require_creds() {
    [ -n "$AK" ] && [ -n "$SK" ] && return 0
    echo "需要 RMQ_ACL_AK / RMQ_ACL_SK（broker initAuthenticationUser 里的那对）" >&2
    return 1
}

run_python() {
    cd "$ROOT/python" || return 1
    "$PY" verify_message_types.py "$NS" "$AK" "$SK" 2>&1 | tee "$LOGDIR/python.log"
    return "${PIPESTATUS[0]}"
}

run_cpp() {
    "$CPP_BUILD/examples/rmq_live_message_types" "$NS" "$AK" "$SK" 2>&1 | tee "$LOGDIR/cpp.log"
    return "${PIPESTATUS[0]}"
}

run_csharp() {
    cd "$ROOT/csharp" || return 1
    "$DOTNET" run --project examples/RocketMQ.Examples -- message-types "$NS" "$AK" "$SK" \
        2>&1 | tee "$LOGDIR/csharp.log"
    return "${PIPESTATUS[0]}"
}

run_go() {
    cd "$ROOT/go" || return 1
    go run ./examples/live_acl_message_types -ns "$NS" -ak "$AK" -sk "$SK" \
        2>&1 | tee "$LOGDIR/go.log"
    return "${PIPESTATUS[0]}"
}

run_nodejs() {
    cd "$ROOT/nodeJs" || return 1
    node --experimental-strip-types examples/live_acl_message_types.ts \
        --ns "$NS" --ak "$AK" --sk "$SK" 2>&1 | tee "$LOGDIR/nodejs.log"
    return "${PIPESTATUS[0]}"
}

run_php() {
    cd "$ROOT/php" || return 1
    ROCKETMQ_CLIENT_LOG_DIR="$LOGDIR/php" php examples/live_acl_message_types.php "$NS" "$AK" "$SK" \
        2>&1 | tee "$LOGDIR/php.log"
    return "${PIPESTATUS[0]}"
}

run_rust() {
    cd "$ROOT/rust" || return 1
    cargo run --quiet --example live_acl_message_types -- "$NS" "$AK" "$SK" \
        2>&1 | tee "$LOGDIR/rust.log"
    return "${PIPESTATUS[0]}"
}

rc=0
for end in "${ENDS[@]}"; do
    echo ""
    echo "=== $end ==="
    if ! require_creds; then rc=1; continue; fi
    "run_$end"
    code=$?
    echo "--- $end exit=$code ---"
    [ "$code" -ne 0 ] && rc=1
done

echo ""
echo "=== 收工 ==="
for end in "${ENDS[@]}"; do
    sum=$(grep -E "PASS=|FAIL=|passed|ok$" "$LOGDIR/$end.log" 2>/dev/null | tail -1)
    echo "$end: ${sum:-无输出}"
done
exit $rc
