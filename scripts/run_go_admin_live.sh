#!/bin/bash
# Live verification of the Go admin client (DefaultMQAdminExt).
#
# Usage: bash scripts/run_go_admin_live.sh [namesrv]      (default 127.0.0.1:9876)
#
# Why this must run against a real broker: the admin library (go/client/admin*.go)
# previously had only 35 unit tests, and the fake broker they use validates
# nothing -- a malformed wire encoding still returned SUCCESS. The first run of
# this script caught a real defect: serialization panics outright when
# `SubscriptionGroupConfig.groupRetryPolicy` is nil (unit tests always construct
# via `NewSubscriptionGroupConfig()` with non-nil fields, so the "hand-built
# config" path was never covered).
#
# Criterion: every assertion in the tool loops **back through the broker** --
# after creating a topic, read back route/config/topic list; after writing KV,
# read it back; after writing a subscription group, read it back; after writing
# offsets, read them back with a different RPC; query a message by KEYS index
# and fetch its body via viewMessage; delete a topic then list again to confirm
# it is gone. So a Go single-sided self-assertion is enough (no cross-language
# read-back needed; that is for the producer/consumer scenario where the wire
# format must be parsed by another implementation).
#
# Cluster: assumed to be already running by default (the scripts/rmq_test_broker.sh
# setup). If the port probe fails, `rmq_test_broker.sh start` is run automatically,
# and only the instance started here is stopped at cleanup; anything already
# running is left untouched.
# A cluster **with a slave** is needed to cover the master/slave address table --
# this script uses the default rmq_test_broker.sh setup (broker-a master 10911 +
# slave 10931).
#
# Note: cluster start + port wait + verification + kill must all be completed
# serially inside this script: returning to the foreground reaps background JVMs.
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
        echo "--- cleanup: stopping the broker started by this script ---"
        sh "$ROOT/scripts/rmq_test_broker.sh" stop || true
    fi
}
trap cleanup EXIT

if ! port_open "$BROKER_PORT"; then
    echo "=== broker not listening on ${BROKER_PORT}, starting local test cluster ==="
    sh "$ROOT/scripts/rmq_test_broker.sh" start || { echo "failed to start broker"; exit 1; }
    STARTED_BROKER=1
    # The port is ready before the broker registers its route with the
    # namesrv; not waiting means the TBW102 route is missing and the run
    # false-fails.
    sleep 12
fi
port_open 9876 || { echo "nameserver 9876 not listening, start namesrv first"; exit 1; }

echo ""
echo "=== admin live verification (topic=${TOPIC} group=${GROUP}) ==="
cd "$ROOT/go" || exit 1
"$GO" run ./examples/live_admin -ns "$NS" -topic "$TOPIC" -group "$GROUP"
GO_RC=$?

echo ""
if [ "$GO_RC" -eq 0 ]; then
    echo "RESULT: all passed (topic=${TOPIC})"
    exit 0
fi
echo "RESULT: failures present (go=${GO_RC} topic=${TOPIC})"
exit 1
