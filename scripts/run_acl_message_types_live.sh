#!/bin/bash
# Live verification of "all message types send/receive" with ACL enabled on
# the cluster (all seven clients in one command).
#
# Why this script exists: ACL has too many false positives -- when the broker
# has no authentication enabled, the positive scenarios still look all green;
# only the "no-credentials must be rejected (NO_PERMISSION=16)" half of the
# assertions can pierce through. So each client runs as a pair: first probe
# without credentials, which must be rejected, then send each of sync / async /
# oneway / batch / delay level / timer / transaction / orderly / Tag filtering /
# user properties / KEYS server-side query with a signature and read back from
# the broker.
#
# Usage:
#   bash scripts/run_acl_message_types_live.sh                 # assumes the cluster
#                                                              # is already up with the
#                                                              # authentication* config below
#   bash scripts/run_acl_message_types_live.sh python cpp      # run only the given clients
#
# Environment variables used (all have defaults, pointing at a local cluster):
#   ROCKETMQ_NAMESRV   namesrv address (default 127.0.0.1:9876)
#   RMQ_ACL_AK / RMQ_ACL_SK   credentials matching the broker's initAuthenticationUser
#   RMQ_PY             python interpreter (default python3)
#   RMQ_CPP_BUILD      cpp cmake build directory (default cpp/build)
#   RMQ_DOTNET         dotnet executable
#   LOGDIR             per-client output directory (default /tmp/rmq_acl_types_live_logs)
#
# When the cluster is not up, it is started with the three key broker.conf
# settings below (without authenticationMetadataProvider, initAuthenticationUser
# is a no-op and every request gets rejected):
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

# Every client must run with credentials; missing credentials are recorded as
# FAIL rather than skipped: skipping would make an "authentication disabled"
# cluster look identical to one where authentication is all green.
require_creds() {
    [ -n "$AK" ] && [ -n "$SK" ] && return 0
    echo "RMQ_ACL_AK / RMQ_ACL_SK are required (the pair from the broker's initAuthenticationUser)" >&2
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
echo "=== summary ==="
for end in "${ENDS[@]}"; do
    sum=$(grep -E "PASS=|FAIL=|passed|ok$|result:" "$LOGDIR/$end.log" 2>/dev/null | tail -1)
    echo "$end: ${sum:-no output}"
done
exit $rc
