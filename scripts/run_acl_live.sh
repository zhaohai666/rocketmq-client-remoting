#!/bin/bash
# ACL 鉴权真机验证（起 namesrv + broker(authenticationEnabled=true) → 跑四端 → 收工）。
#
# 用法: bash scripts/run_acl_live.sh [python|rust|dotnet|cpp|all ...]   （默认 all）
#
# 为什么要有这个脚本：ACL 的假通过太多了 —— broker 没开鉴权时**正向场景照样全绿**，
# 只有反向场景（无凭据 / 错 secretKey）才戳得穿；而四端的「签名算错」与「没签名」在
# broker 侧是同一种拒绝。所以四端脚本都成对断言「签名被接受」+「拒绝码必须是 16」，
# 并且必须打真集群。本脚本负责把集群按 ACL 配置拉起来（本机没有现成的 ACL harness，
# 仓库里另一份 scripts/rmq_test_broker.sh 是 macOS 的普通配置集群）。
#
# ⚠ 集群 start + 等端口/路由 + 跑验证 + kill 必须在**同一条命令**里跑完：前台返回会回收
#   后台 JVM。本脚本内部已经串好这条链，直接 `bash scripts/run_acl_live.sh all` 即可。
#
# 全部路径可用环境变量覆盖（默认值是本机 Windows 开发机的形态）：
#   RMQ_JAVA           java 可执行文件（需要 JDK 17+，本机用 LibericaJDK-21）
#   RMQ_DIST_WIN       发行版目录的 **Windows** 形态路径（给 -cp / -c 用）
#   RMQ_DIST           同一个目录的 MSYS/Git-Bash 形态路径（给读日志用）
#   RMQ_ACL_STORE      broker store 根目录（Windows 形态，正斜杠；AuthConfig 取 $STORE/config）
#   RMQ_ACL_LOG        namesrv/broker 的 stdout 落盘目录
#   ROCKETMQ_NAMESRV   namesrv 地址（默认 127.0.0.1:9876）
#   RMQ_ACL_AK / RMQ_ACL_SK  broker 里 initAuthenticationUser 建的 SUPER 用户凭据
#   各端产物：RMQ_PY / RMQ_DOTNET / RMQ_CPP_ACL（默认指向本仓库构建产物）
#
# broker 侧鉴权失败一律是 NO_PERMISSION(16)（broker/auth/pipeline/AuthenticationPipeline.java:53），
# 具体原因只能看 remark：签名不对 → "check signature failed."；凭据缺失 → "username cannot be null."。
set -u

CYGWIN_ROOT=${RMQ_CYGWIN_ROOT:-/c/Users/zhaoh}
JAVA=${RMQ_JAVA:-"C:/Program Files/BellSoft/LibericaJDK-21/bin/java.exe"}
DIST_WIN=${RMQ_DIST_WIN:-'D:\zhaohai666-rocketmq\rocketmq\distribution\target\rocketmq-5.5.0\rocketmq-5.5.0'}
DIST=${RMQ_DIST:-/d/zhaohai666-rocketmq/rocketmq/distribution/target/rocketmq-5.5.0/rocketmq-5.5.0}
DEPS=${RMQ_DEPS:-$CYGWIN_ROOT/rmqdeps}
STORE=${RMQ_ACL_STORE:-C:/Users/zhaoh/rmqdeps/acl-store}
LOGDIR=${RMQ_ACL_LOG:-$DEPS/acl-live-logs}
CONF="$DEPS/broker-acl.conf"
CONF_WIN=${RMQ_ACL_CONF_WIN:-'C:\Users\zhaoh\rmqdeps\broker-acl.conf'}
NS=${ROCKETMQ_NAMESRV:-127.0.0.1:9876}
BROKER_PORT=${ROCKETMQ_BROKER_PORT:-10911}
AK=${RMQ_ACL_AK:-AK_TEST}
SK=${RMQ_ACL_SK:-SK_TEST_SECRET_12345678}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PY=${RMQ_PY:-$CYGWIN_ROOT/.workbuddy/binaries/python/envs/default/Scripts/python.exe}
DOTNET=${RMQ_DOTNET:-$CYGWIN_ROOT/dotsdk/dotnet.exe}
CPP_ACL=${RMQ_CPP_ACL:-$ROOT/cpp/build/examples/rmq_live_acl.exe}

ENDS=("$@")
[ ${#ENDS[@]} -eq 0 ] && ENDS=(all)
if [ "${ENDS[0]}" = "all" ]; then ENDS=(python rust dotnet cpp); fi

mkdir -p "$LOGDIR"

# ---------------------------------------------------------------- broker conf
# 关掉鉴权的 broker 会让「无凭据被拒」这一半断言假绿，所以这份配置是脚本的核心产物：
# authenticationMetadataProvider 必须显式给（不给就是 null，initAuthenticationUser 白写，
# 所有请求都过不了鉴权），authenticationProvider 不给则默认 DefaultAuthenticationProvider。
cat > "$CONF" <<EOF
brokerClusterName=DefaultCluster
brokerName=broker-a
brokerId=0
namesrvAddr=$NS
brokerIP1=127.0.0.1
listenPort=$BROKER_PORT
storePathRootDir=$STORE
autoCreateTopicEnable=true
autoCreateSubscriptionGroup=true
deleteWhen=04
fileReservedTime=48
brokerRole=ASYNC_MASTER
flushDiskType=ASYNC_FLUSH
authenticationEnabled=true
authenticationMetadataProvider=org.apache.rocketmq.auth.authentication.provider.LocalAuthenticationMetadataProvider
initAuthenticationUser={"username":"$AK","password":"$SK"}
EOF

NS_PID=""
BR_PID=""
cleanup() {
    [ -n "$BR_PID" ] && kill "$BR_PID" 2>/dev/null
    [ -n "$NS_PID" ] && kill "$NS_PID" 2>/dev/null
    wait 2>/dev/null
}
trap cleanup EXIT

wait_port() {
    local port=$1 i=0
    while [ "$i" -lt 120 ]; do
        if (echo > "/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then return 0; fi
        sleep 0.5
        i=$((i + 1))
    done
    return 1
}

echo "=== [1/4] 起 nameServer ==="
rm -f "$DIST/logs/rocketmqlogs/namesrv.log"
( cd "$DIST" && ROCKETMQ_HOME="$DIST_WIN" "$JAVA" \
    -Drocketmq.home.dir="$DIST_WIN" -Xms512m -Xmx1g -cp "$DIST_WIN\\lib\\*" \
    org.apache.rocketmq.namesrv.NamesrvStartup > "$LOGDIR/namesrv.out" 2>&1 ) &
NS_PID=$!
wait_port 9876 || { echo "nameServer 没起来，看 $LOGDIR/namesrv.out"; tail -30 "$LOGDIR/namesrv.out"; exit 1; }
echo "nameServer UP"

echo "=== [2/4] 起 broker（authenticationEnabled=true）==="
rm -f "$DIST/logs/rocketmqlogs/broker.log"
( cd "$DIST" && ROCKETMQ_HOME="$DIST_WIN" "$JAVA" \
    -Drocketmq.home.dir="$DIST_WIN" -Xms1g -Xmx1g -cp "$DIST_WIN\\lib\\*" \
    org.apache.rocketmq.broker.BrokerStartup -c "$CONF_WIN" > "$LOGDIR/broker.out" 2>&1 ) &
BR_PID=$!
wait_port $BROKER_PORT || { echo "broker 没起来，看 $LOGDIR/broker.out"; tail -40 "$LOGDIR/broker.out"; exit 1; }

# 端口先于「向 namesrv 注册路由」就绪，不等就会拿不到 TBW102 路由而假失败。
i=0
while [ "$i" -lt 60 ]; do
    grep -q "boot success" "$DIST/logs/rocketmqlogs/broker.log" 2>/dev/null && break
    sleep 1
    i=$((i + 1))
done
sleep 12
echo "broker UP（boot success + 12s 路由注册窗口）"

# ---------------------------------------------------------------- 跑各端
rc=0
run_python() {
    cd "$ROOT/python" || return 1
    "$PY" verify_acl_live.py "$NS" "$AK" "$SK" 2>&1 | tee "$LOGDIR/python.log"
    return "${PIPESTATUS[0]}"
}
run_rust() {
    cd "$ROOT/rust" || return 1
    cargo run --quiet --example live_acl -- "$NS" "$AK" "$SK" 2>&1 | tee "$LOGDIR/rust.log"
    return "${PIPESTATUS[0]}"
}
run_dotnet() {
    cd "$ROOT/dotnet" || return 1
    RMQ_NATIVE_COMPRESSION_DIR='C:\Users\zhaoh\rmqdeps\native-compression' \
        "$DOTNET" run --no-build --project examples/RocketMQ.Examples \
        -- acl "$NS" "$AK" "$SK" 2>&1 | tee "$LOGDIR/dotnet.log"
    return "${PIPESTATUS[0]}"
}
run_cpp() {
    PATH="$DEPS/zlib-install/bin:$PATH" \
        "$CPP_ACL" "$NS" "$AK" "$SK" 2>&1 | tee "$LOGDIR/cpp.log"
    return "${PIPESTATUS[0]}"
}

for end in "${ENDS[@]}"; do
    echo ""
    echo "=== [3/4] $end ==="
    "run_$end"
    code=$?
    echo "--- $end exit=$code ---"
    [ "$code" -ne 0 ] && rc=1
done

echo ""
echo "=== [4/4] 收工 ==="
for end in "${ENDS[@]}"; do
    sum=$(grep -E "PASS=|passed," "$LOGDIR/$end.log" 2>/dev/null | tail -1)
    echo "$end: ${sum:-无输出}"
done
exit $rc
