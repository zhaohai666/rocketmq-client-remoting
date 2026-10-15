#!/bin/bash
# ACL authentication live verification (start namesrv + broker(authenticationEnabled=true) -> run each client -> wrap up).
#
# Usage: bash scripts/run_acl_live.sh [python|rust|csharp|cpp|all ...]   (all by default)
#
# Why this script exists: ACL produces too many false positives — when the broker has
# authentication disabled, **positive scenarios stay fully green**; only negative scenarios
# (no credentials / wrong secretKey) can pierce through, and "wrong signature" vs "no
# signature" are the same rejection on the broker side. So each client script pairs the
# assertions "signature accepted" + "rejection code must be 16", and must run against a real
# cluster. This script is responsible for bringing the cluster up with ACL configuration
# (there is no ready-made ACL harness on this machine; the other script in the repo,
# scripts/rmq_test_broker.sh, is a plain macOS-configured cluster).
#
# ⚠ Cluster start + waiting for ports/routes + running verification + kill must all finish
#   within **the same command**: when a foreground command returns, the background JVMs are
#   reaped. This script chains that internally already; just run `bash scripts/run_acl_live.sh all`.
#
# All paths can be overridden via environment variables (defaults match this Windows dev machine):
#   RMQ_JAVA           java executable (JDK 17+ required; LibericaJDK-21 on this machine)
#   RMQ_DIST_WIN       the **Windows**-style path of the distribution directory (for -cp / -c)
#   RMQ_DIST           the same directory in MSYS/Git-Bash form (for reading logs)
#   RMQ_ACL_STORE      broker store root directory (Windows form, forward slashes; AuthConfig reads $STORE/config)
#   RMQ_ACL_LOG        directory where namesrv/broker stdout lands
#   ROCKETMQ_NAMESRV   namesrv address (default 127.0.0.1:9876)
#   RMQ_ACL_AK / RMQ_ACL_SK  SUPER user credentials created by initAuthenticationUser in the broker
#   Per-side artifacts: RMQ_PY / RMQ_DOTNET / RMQ_CPP_ACL (default to this repo's build outputs)
#
# Broker-side authentication failures always return NO_PERMISSION(16); the specific reason can
# only be seen in the remark: wrong signature -> "check signature failed."; missing credentials
# -> "username cannot be null.".
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
if [ "${ENDS[0]}" = "all" ]; then ENDS=(python rust csharp cpp); fi

mkdir -p "$LOGDIR"

# ---------------------------------------------------------------- broker conf
# A broker with authentication disabled makes the "no credentials rejected" half of the
# assertions falsely green, so this config file is the script's core artifact:
# authenticationMetadataProvider must be given explicitly (otherwise it is null,
# initAuthenticationUser is written for nothing, and no request passes authentication);
# without authenticationProvider, DefaultAuthenticationProvider is the default.
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

echo "=== [1/4] starting nameServer ==="
rm -f "$DIST/logs/rocketmqlogs/namesrv.log"
( cd "$DIST" && ROCKETMQ_HOME="$DIST_WIN" "$JAVA" \
    -Drocketmq.home.dir="$DIST_WIN" -Xms512m -Xmx1g -cp "$DIST_WIN\\lib\\*" \
    org.apache.rocketmq.namesrv.NamesrvStartup > "$LOGDIR/namesrv.out" 2>&1 ) &
NS_PID=$!
wait_port 9876 || { echo "nameServer failed to start, see $LOGDIR/namesrv.out"; tail -30 "$LOGDIR/namesrv.out"; exit 1; }
echo "nameServer UP"

echo "=== [2/4] starting broker (authenticationEnabled=true) ==="
rm -f "$DIST/logs/rocketmqlogs/broker.log"
( cd "$DIST" && ROCKETMQ_HOME="$DIST_WIN" "$JAVA" \
    -Drocketmq.home.dir="$DIST_WIN" -Xms1g -Xmx1g -cp "$DIST_WIN\\lib\\*" \
    org.apache.rocketmq.broker.BrokerStartup -c "$CONF_WIN" > "$LOGDIR/broker.out" 2>&1 ) &
BR_PID=$!
wait_port $BROKER_PORT || { echo "broker failed to start, see $LOGDIR/broker.out"; tail -40 "$LOGDIR/broker.out"; exit 1; }

# The port opens before "routes are registered into the namesrv"; without waiting, TBW102
# routes are missing and the run fails spuriously.
i=0
while [ "$i" -lt 60 ]; do
    grep -q "boot success" "$DIST/logs/rocketmqlogs/broker.log" 2>/dev/null && break
    sleep 1
    i=$((i + 1))
done
sleep 12
echo "broker UP (boot success + 12s route registration window)"

# ---------------------------------------------------------------- run each client
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
run_csharp() {
    cd "$ROOT/csharp" || return 1
    RMQ_NATIVE_COMPRESSION_DIR='C:\Users\zhaoh\rmqdeps\native-compression' \
        "$DOTNET" run --no-build --project examples/RocketMQ.Examples \
        -- acl "$NS" "$AK" "$SK" 2>&1 | tee "$LOGDIR/csharp.log"
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
echo "=== [4/4] wrap up ==="
for end in "${ENDS[@]}"; do
    sum=$(grep -E "PASS=|passed," "$LOGDIR/$end.log" 2>/dev/null | tail -1)
    echo "$end: ${sum:-no output}"
done
exit $rc
