#!/bin/bash
# Live verification of the 2026-11-11 capability closeout, one end at a time.
#
# Usage: bash scripts/run_capability_closeout_live.sh {python|go|node|cpp|csharp|rust|php|all} [namesrv]
#
# The capabilities under test (see docs/classic-remoting-capability-checklist.md §10):
#   L1  ConsumeFromWhere's three deprecated values fold into the LAST_OFFSET branch
#   L2  LitePull connectBrokerByUser / defaultBrokerId really reaches the pull request
#   L3  adminExtGroup identity (no wire effect) + node's awaitTerminationMillisWhenShutdown
#
# Cluster: assumes one is already running (both ports reachable). If not, it starts its
# own plaintext namesrv+broker from the pre-built 5.5.1 distribution and stops **only**
# that instance at the end; anything already running is left untouched.
#
# ⚠ Cluster start + port wait + verification + kill must all be chained inside this
#   script: when a foreground command returns, the background JVMs are reaped.
set -u

WHICH=${1:-python}
NS_PORT=${ROCKETMQ_NS_PORT:-9876}
BROKER_PORT=${ROCKETMQ_BROKER_PORT:-10911}
# The address handed to the probes must follow the nameserver port, not the 9876 default:
# a verification moved to another port pair would otherwise talk to whatever (or nothing)
# is listening on 9876.
NS=${2:-${ROCKETMQ_NAMESRV:-127.0.0.1:${NS_PORT}}}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DIST=${ROCKETMQ_DIST:-/Users/haizai/project/jingsai/roocketmq/zhaohai666-rocketmq/distribution/target/rocketmq-5.5.1/rocketmq-5.5.1}
WORK=${RMQ_CAP_WORK:-/tmp/rmq_cap_live}
PY=${RMQ_PY:-/Users/haizai/.workbuddy/binaries/python/envs/default/bin/python}
NODE_BIN=${NODE_BIN:-/Users/haizai/.workbuddy/binaries/node/versions/22.22.2-6/bin/node}
[ -x "$NODE_BIN" ] || NODE_BIN=node

port_open() {
    "$PY" -c "import socket,sys; s=socket.socket(); s.settimeout(0.5); sys.exit(0 if s.connect_ex(('127.0.0.1',$1))==0 else 1)" 2>/dev/null
}

STARTED_CLUSTER=0
cleanup() {
    if [ "$STARTED_CLUSTER" = "1" ]; then
        echo "--- cleanup: stopping the cluster started by this script (ports ${NS_PORT}/${BROKER_PORT}) ---"
        # Kill by listening port, not by main class: several verifications may run at the
        # same time on different ports, and `jps | grep BrokerStartup` would take them all
        # down. lsof is available where jps is.
        for p in "$BROKER_PORT" "$NS_PORT"; do
            for pid in $(lsof -ti ":$p" 2>/dev/null); do
                kill -15 "$pid" 2>/dev/null || true
            done
        done
        i=0
        while [ "$i" -lt 120 ]; do
            if ! lsof -ti ":$BROKER_PORT" >/dev/null 2>&1 && ! lsof -ti ":$NS_PORT" >/dev/null 2>&1; then
                break
            fi
            sleep 0.5
            i=$((i + 1))
        done
    fi
}
trap cleanup EXIT

# ---------------------------------------------------------------- cluster
if ! port_open "$NS_PORT" || ! port_open "$BROKER_PORT"; then
    echo "=== no cluster on ${NS_PORT}/${BROKER_PORT}, starting one from ${DIST} ==="
    [ -d "$DIST" ] || { echo "distribution not found: $DIST (set ROCKETMQ_DIST)" >&2; exit 2; }
    mkdir -p "$WORK/store"
    # mqnamesrv binds 9876 unless a config file says otherwise (there is no -D switch for
    # listenPort), so a non-default port needs its own namesrv.conf -- without this every
    # concurrently running verification would fight over 9876.
    cat > "$WORK/namesrv.conf" <<EOF
listenPort = ${NS_PORT}
EOF
    cat > "$WORK/broker.conf" <<EOF
brokerClusterName = DefaultCluster
brokerName = broker-a
brokerId = 0
deleteWhen = 04
fileReservedTime = 48
brokerRole = ASYNC_MASTER
flushDiskType = ASYNC_FLUSH
namesrvAddr = 127.0.0.1:${NS_PORT}
listenPort = ${BROKER_PORT}
storePathRootDir = $WORK/store
autoCreateTopicEnable = true
autoCreateSubscriptionGroup = true
traceTopicEnable = true
EOF
    export ROCKETMQ_HOME="$DIST"
    export JAVA_OPT_EXT="-Xms1g -Xmx2g -Xmn768m -Duser.home=$WORK"
    STARTED_CLUSTER=1
    nohup "$DIST/bin/mqnamesrv" -c "$WORK/namesrv.conf" > "$WORK/ns.log" 2>&1 &
    nohup "$DIST/bin/mqbroker" -c "$WORK/broker.conf" > "$WORK/broker.log" 2>&1 &
    for i in $(seq 1 90); do
        if port_open "$NS_PORT" && port_open "$BROKER_PORT"; then break; fi
        sleep 1
        if [ "$i" = "90" ]; then
            echo "=== cluster not ready within 90s (see $WORK/ns.log / $WORK/broker.log) ===" >&2
            exit 2
        fi
    done
    # A listening port is not the same as a registered route. The probes create topics
    # through the nameserver immediately, so they need TBW102 to be visible first
    # (otherwise "No route info of default topic TBW102"). Poll instead of sleeping a
    # fixed amount: the registration round is second-scale but not constant.
    ready=0
    for i in $(seq 1 90); do
        out=$( cd "$DIST" && ROCKETMQ_HOME="$DIST" sh bin/mqadmin topicRoute \
                 -n "127.0.0.1:${NS_PORT}" -t TBW102 2>/dev/null )
        case "$out" in
            *brokerName*) ready=1; break ;;
        esac
        sleep 1
    done
    if [ "$ready" != "1" ]; then
        echo "=== TBW102 route never became visible (see $WORK/ns.log / $WORK/broker.log) ===" >&2
        exit 2
    fi
fi
port_open "$NS_PORT" || { echo "nameserver ${NS_PORT} not listening" >&2; exit 2; }
port_open "$BROKER_PORT" || { echo "broker ${BROKER_PORT} not listening" >&2; exit 2; }

# ---------------------------------------------------------------- probes
run_python() {
    echo ""
    echo "=== python: capability closeout live ==="
    ( cd "$ROOT/python" && "$PY" verify_capability_closeout_live.py "$NS" )
}
run_go() {
    echo ""
    echo "=== go: capability closeout live ==="
    ( cd "$ROOT/go" && go run ./examples/live_capability_closeout -ns "$NS" )
}
run_node() {
    echo ""
    echo "=== node: capability closeout live ==="
    ( cd "$ROOT/nodeJs" && "$NODE_BIN" --experimental-strip-types --no-warnings \
        examples/live_capability_closeout.ts --ns "$NS" )
}
run_php() {
    echo ""
    echo "=== php: capability closeout live ==="
    ( cd "$ROOT/php" && php examples/live_capability_closeout.php "$NS" )
}
run_rust() {
    echo ""
    echo "=== rust: capability closeout live ==="
    ( cd "$ROOT/rust" && cargo run --quiet --example live_capability_closeout -- "$NS" )
}
run_cpp() {
    echo ""
    echo "=== cpp: capability closeout live ==="
    local BIN="$ROOT/cpp/build/examples/live_capability_closeout"
    [ -x "$BIN" ] || { echo "cpp example not built: $BIN (cmake --build cpp/build --target live_capability_closeout)" >&2; return 2; }
    "$BIN" "$NS"
}
run_csharp() {
    echo ""
    echo "=== csharp: capability closeout live ==="
    # The .NET end is one executable with subcommands (examples/RocketMQ.Examples,
    # AssemblyName=rmq); there is no per-scenario csproj.
    ( cd "$ROOT/csharp" && dotnet run --project examples/RocketMQ.Examples -- capability-closeout "$NS" )
}

RC=0
# Each branch's status must be captured: without this a failing probe still
# reports "all green" because the case statement's exit status is discarded.
case "$WHICH" in
    python) run_python || RC=$? ;;
    go)     run_go || RC=$? ;;
    node)   run_node || RC=$? ;;
    php)    run_php || RC=$? ;;
    rust)   run_rust || RC=$? ;;
    cpp)    run_cpp || RC=$? ;;
    csharp) run_csharp || RC=$? ;;
    all)
        for one in python go node php rust cpp csharp; do
            "run_$one" || RC=1
        done
        ;;
    *) echo "unknown end: $WHICH (python|go|node|cpp|csharp|rust|php|all)" >&2 ; exit 2 ;;
esac

echo ""
if [ "$RC" -eq 0 ]; then
    echo "RESULT: ${WHICH} all green"
    exit 0
fi
echo "RESULT: ${WHICH} has failures"
exit 1
