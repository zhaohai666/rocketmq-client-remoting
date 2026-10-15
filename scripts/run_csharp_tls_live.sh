#!/bin/bash
# C# client TLS strict-verification live test (counterpart of run_php_live.sh tls / run_node_live.sh tls):
# self-managed certificates (CA/server/client) + self-started TLS cluster, running three legs:
#   leg1 plain      tlsEnable=True, trusting self-signed (test-mode)
#   leg2 ca_verify  TlsOptions.CaCert —— strict certificate chain + hostname SAN verification
#   leg3 mtls       restart broker (-Dtls.server.authClient=true) + client-certificate mutual authentication
#
# Usage: bash scripts/run_csharp_tls_live.sh [namesrv]
set -u

NS=${1:-${ROCKETMQ_NAMESRV:-127.0.0.1:9876}}
NS_PORT=${ROCKETMQ_NS_PORT:-9876}
BROKER_PORT=${ROCKETMQ_BROKER_PORT:-10911}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
CS_DIR="$ROOT/csharp"
PROJ="$CS_DIR/examples/RocketMQ.Examples/RocketMQ.Examples.csproj"
DIST=${ROCKETMQ_DIST:-/Users/haizai/project/jingsai/roocketmq/zhaohai666-rocketmq/distribution/target/rocketmq-5.5.1/rocketmq-5.5.1}
WORK=/tmp/rmq_cs_live_tls
STAMP=$(date +%s)

port_open() {
    python3 - "$1" <<'PYEOF' 2>/dev/null
import socket, sys
try:
    s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), 0.5)
    s.close(); sys.exit(0)
except OSError:
    sys.exit(1)
PYEOF
}

STARTED=0
cleanup() {
    if [ "$STARTED" = "1" ]; then
        echo "--- Done: stopping the cluster started by this script (finding pids via jps) ---"
        local pids
        pids=$(jps -l 2>/dev/null | awk '/NamesrvStartup|BrokerStartup/ {print $1}')
        if [ -n "$pids" ]; then
            # shellcheck disable=SC2086
            kill $pids 2>/dev/null || true
            sleep 5
            pids=$(jps -l 2>/dev/null | awk '/NamesrvStartup|BrokerStartup/ {print $1}')
            # shellcheck disable=SC2086
            [ -n "$pids" ] && kill -9 $pids 2>/dev/null
        fi
    fi
}
trap cleanup EXIT

if port_open "$NS_PORT" || port_open "$BROKER_PORT"; then
    echo "=== the tls run needs an exclusive cluster, but a cluster is already running on ${NS_PORT}/${BROKER_PORT}. Stop it first. ===" >&2
    exit 2
fi
[ -d "$DIST" ] || { echo "RocketMQ distribution directory not found: ${DIST} (override with ROCKETMQ_DIST)" >&2; exit 2; }
command -v openssl >/dev/null 2>&1 || { echo "openssl is not available" >&2; exit 2; }

mkdir -p "$WORK/store"
echo "=== generating test certificates (CA / server(SAN: IP:127.0.0.1,DNS:localhost) / client) ==="
(cd "$WORK" \
    && openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 2 -subj "/CN=rmq-cs-test-ca" >/dev/null 2>&1 \
    && openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/CN=127.0.0.1" >/dev/null 2>&1 \
    && printf "subjectAltName=IP:127.0.0.1,DNS:localhost\n" > server.ext \
    && openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 2 -extfile server.ext >/dev/null 2>&1 \
    && openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr -subj "/CN=rmq-cs-test-client" >/dev/null 2>&1 \
    && openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 2 >/dev/null 2>&1) \
    || { echo "certificate generation failed" >&2; exit 2; }
srv_hdr=$(head -1 "$WORK/server.key")
case "$srv_hdr" in
    *"BEGIN PRIVATE KEY"*) : ;;
    *) openssl pkcs8 -topk8 -nocrypt -in "$WORK/server.key" -out "$WORK/server.p8.key" \
        && mv "$WORK/server.p8.key" "$WORK/server.key" ;;
esac
cat > "$WORK/broker.conf" <<EOF
brokerClusterName = DefaultCluster
brokerName = broker-a
brokerId = 0
deleteWhen = 04
fileReservedTime = 48
brokerRole = ASYNC_MASTER
flushDiskType = ASYNC_FLUSH
namesrvAddr = 127.0.0.1:$NS_PORT
listenPort = $BROKER_PORT
storePathRootDir = $WORK/store
autoCreateTopicEnable = true
EOF
export ROCKETMQ_HOME="$DIST"
export JAVA_OPT_EXT="-Xms1g -Xmx2g -Xmn768m -Dtls.enable=true -Dtls.test.mode.enable=false -Dtls.server.certPath=$WORK/server.crt -Dtls.server.keyPath=$WORK/server.key"
echo "=== starting cluster (namesrv+broker both permissive TLS) ==="
nohup "$DIST/bin/mqnamesrv" > "$WORK/ns.log" 2>&1 &
nohup "$DIST/bin/mqbroker" -c "$WORK/broker.conf" > "$WORK/broker.log" 2>&1 &
STARTED=1
for i in $(seq 1 60); do
    if port_open "$NS_PORT" && port_open "$BROKER_PORT"; then sleep 2; break; fi
    sleep 1
    if [ "$i" = "60" ]; then
        echo "=== TLS cluster not ready within 60s (see $WORK/ns.log / $WORK/broker.log) ===" >&2
        exit 2
    fi
done

echo "=== building C# examples ==="
dotnet build "$PROJ" >/dev/null 2>&1 || { echo "dotnet build failed" >&2; exit 2; }
PROG=$(ls "$CS_DIR"/examples/RocketMQ.Examples/bin/Debug/*/rmq.dll 2>/dev/null | head -1)
[ -z "$PROG" ] && PROG=$(ls "$CS_DIR"/examples/RocketMQ.Examples/bin/Debug/*/*.dll 2>/dev/null | head -1)
[ -z "$PROG" ] && { echo "build output not found" >&2; exit 2; }

# Port open ≠ TBW102 registered: before the route reaches the nameserver, createTopic dies
# with "No route info of default topic TBW102", and a consumer started early would also miss
# the whole consumption window. Wait for a real route query to succeed (same route-wait as the
# node version in run_node_live.sh).
wait_tbw102() {
    for i in $(seq 1 30); do
        if "$DIST/bin/mqadmin" topicRoute -n "$NS" -t TBW102 2>/dev/null | /usr/bin/grep -qF 'broker-a'; then
            echo "=== TBW102 route registered ==="
            return 0
        fi
        sleep 2
    done
    echo "=== TBW102 not registered into nameserver after 30 rounds ===" >&2
    return 1
}

RC=0
wait_tbw102 || exit 2
echo "=== [leg 1/3] plain (test-mode: trust self-signed) ==="
dotnet "$PROG" tls "$NS" "CsTls_plain_$STAMP" "GID_CsTls_plain_$STAMP" plain || RC=1
echo "=== [leg 2/3] ca_verify (verify certificate chain + hostname SAN) ==="
dotnet "$PROG" tls "$NS" "CsTls_ca_$STAMP" "GID_CsTls_ca_$STAMP" ca_verify "" "$WORK/ca.crt" || RC=1

echo "=== restarting broker (adding -Dtls.server.authClient=true → mTLS) ==="
# Port closed ≠ process exited ≠ store file lock released. Wait until BrokerStartup is fully
# gone from jps, then allow 3s more; if startup fails (Lock failed), retry the whole block up
# to 3 times.
for attempt in 1 2 3; do
    pids=$(jps -l 2>/dev/null | awk '/BrokerStartup/ {print $1}')
    # shellcheck disable=SC2086
    [ -n "$pids" ] && kill $pids 2>/dev/null
    for i in $(seq 1 20); do
        jps -l 2>/dev/null | /usr/bin/grep -qF 'BrokerStartup' || break
        sleep 1
        if [ "$i" = "20" ]; then
            pids=$(jps -l 2>/dev/null | awk '/BrokerStartup/ {print $1}')
            # shellcheck disable=SC2086
            [ -n "$pids" ] && kill -9 $pids 2>/dev/null
        fi
    done
    sleep 3
    export JAVA_OPT_EXT="-Xms1g -Xmx2g -Xmn768m -Dtls.enable=true -Dtls.test.mode.enable=false -Dtls.server.certPath=$WORK/server.crt -Dtls.server.keyPath=$WORK/server.key -Dtls.server.authClient=true -Dtls.server.trustCertPath=$WORK/ca.crt"
    nohup "$DIST/bin/mqbroker" -c "$WORK/broker.conf" > "$WORK/broker2.log" 2>&1 &
    ok=0
    dead=0
    for i in $(seq 1 60); do
        # Not visible in jps ≠ process dead: hsperfdata takes a few seconds to appear after JVM
        # startup, so it must be invisible for 3 consecutive seconds before declaring death
        # (otherwise a just-started broker could be misjudged and killed).
        if jps -l 2>/dev/null | /usr/bin/grep -qF 'BrokerStartup'; then
            dead=0
        else
            dead=$((dead + 1))
            if [ "$dead" -ge 3 ]; then break; fi
        fi
        if port_open "$BROKER_PORT"; then ok=1; break; fi
        sleep 1
    done
    if [ "$ok" = "1" ]; then
        echo "=== mTLS broker ready (attempt ${attempt}) ==="
        break
    fi
    if [ "$attempt" = "3" ]; then
        echo "=== mTLS broker not ready after 3 attempts (see $WORK/broker2.log) ===" >&2
        exit 2
    fi
    echo "=== mTLS broker attempt ${attempt} failed, retrying ==="
done
echo "=== [leg 3/3] mtls (client certificates, mutual authentication) ==="
wait_tbw102 || exit 2
dotnet "$PROG" tls "$NS" "CsTls_mtls_$STAMP" "GID_CsTls_mtls_$STAMP" mtls "" "$WORK/ca.crt" "$WORK/client.crt" "$WORK/client.key" || RC=1
echo "=== exit=$RC ==="
exit $RC
