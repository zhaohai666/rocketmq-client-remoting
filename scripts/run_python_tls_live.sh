#!/bin/bash
# Live verification of strict TLS validation on the Python client (counterpart
# of run_php_live.sh tls / run_node_live.sh tls): self-managed certificates
# (CA/server/client) + a self-started TLS cluster, running three legs:
#   leg1 plain      tls_enable=True, trusting the self-signed cert (test-mode)
#   leg2 ca_verify  tls_options['caCert'] -- strict chain + hostname SAN validation
#   leg3 mtls       broker restart (-Dtls.server.authClient=true) + mutual auth with a client certificate
#
# Usage: bash scripts/run_python_tls_live.sh [namesrv]
#
# Note: cluster start + three-leg verification + kill must all be completed
# serially inside this script (returning to the sandbox foreground reaps
# background JVMs).
set -u

NS=${1:-${ROCKETMQ_NAMESRV:-127.0.0.1:9876}}
NS_PORT=${ROCKETMQ_NS_PORT:-9876}
BROKER_PORT=${ROCKETMQ_BROKER_PORT:-10911}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PY_DIR="$ROOT/python"
PYTHON_BIN=${PYTHON_BIN:-$PY_DIR/.venv/bin/python}
if ! [ -x "$PYTHON_BIN" ]; then PYTHON_BIN=python3; fi
DIST=${ROCKETMQ_DIST:-/Users/haizai/project/jingsai/roocketmq/zhaohai666-rocketmq/distribution/target/rocketmq-5.5.1/rocketmq-5.5.1}
WORK=/tmp/rmq_py_live_tls
STAMP=$(date +%s)

port_open() {
    "$PYTHON_BIN" - <<PYEOF 2>/dev/null
import socket, sys
try:
    s = socket.create_connection(("127.0.0.1", $1), 0.5)
    s.close(); sys.exit(0)
except OSError:
    sys.exit(1)
PYEOF
}

STARTED=0
cleanup() {
    if [ "$STARTED" = "1" ]; then
        echo "--- cleanup: stopping the cluster started by this script (pids via jps) ---"
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
    echo "=== the tls variant needs an exclusive cluster, but something is already running on ${NS_PORT}/${BROKER_PORT}. Stop it first. ===" >&2
    exit 2
fi
[ -d "$DIST" ] || { echo "RocketMQ distribution directory not found: ${DIST} (override with ROCKETMQ_DIST)" >&2; exit 2; }
command -v openssl >/dev/null 2>&1 || { echo "openssl unavailable" >&2; exit 2; }

mkdir -p "$WORK/store"
echo "=== generating test certificates (CA / server(SAN: IP:127.0.0.1,DNS:localhost) / client) ==="
(cd "$WORK" \
    && openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 2 -subj "/CN=rmq-py-test-ca" >/dev/null 2>&1 \
    && openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/CN=127.0.0.1" >/dev/null 2>&1 \
    && printf "subjectAltName=IP:127.0.0.1,DNS:localhost\n" > server.ext \
    && openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 2 -extfile server.ext >/dev/null 2>&1 \
    && openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr -subj "/CN=rmq-py-test-client" >/dev/null 2>&1 \
    && openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 2 >/dev/null 2>&1) \
    || { echo "certificate generation failed" >&2; exit 2; }
# RocketMQ's PemReader only accepts PKCS#8 ("BEGIN PRIVATE KEY"); when an old
# LibreSSL outputs PKCS#1 ("BEGIN RSA PRIVATE KEY") by default, convert explicitly.
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
echo "=== starting cluster (namesrv+broker, both permissive TLS) ==="
nohup "$DIST/bin/mqnamesrv" > "$WORK/ns.log" 2>&1 &
nohup "$DIST/bin/mqbroker" -c "$WORK/broker.conf" > "$WORK/broker.log" 2>&1 &
STARTED=1
for i in $(seq 1 60); do
    if port_open "$NS_PORT" && port_open "$BROKER_PORT"; then sleep 2; break; fi
    sleep 1
    if [ "$i" = "60" ]; then
        echo "=== TLS cluster not ready after 60s (see $WORK/ns.log / $WORK/broker.log) ===" >&2
        exit 2
    fi
done

cd "$PY_DIR" || exit 1
RC=0
echo "=== [leg 1/3] plain (test-mode: trust the self-signed cert) ==="
"$PYTHON_BIN" verify_tls_live.py "$NS" --leg plain || RC=1
echo "=== [leg 2/3] ca_verify (real chain + hostname SAN validation) ==="
"$PYTHON_BIN" verify_tls_live.py "$NS" --leg ca_verify --caCert "$WORK/ca.crt" --serverName 127.0.0.1 || RC=1

echo "=== restarting broker (add -Dtls.server.authClient=true -> mTLS) ==="
pids=$(jps -l 2>/dev/null | awk '/BrokerStartup/ {print $1}')
# shellcheck disable=SC2086
[ -n "$pids" ] && kill $pids 2>/dev/null
for i in $(seq 1 15); do
    port_open "$BROKER_PORT" || break
    sleep 1
    if [ "$i" = "15" ]; then
        pids=$(jps -l 2>/dev/null | awk '/BrokerStartup/ {print $1}')
        # shellcheck disable=SC2086
        [ -n "$pids" ] && kill -9 $pids 2>/dev/null
        sleep 2
    fi
done
# The port being closed does not mean the store file lock is released; allow a 3s buffer.
sleep 3
export JAVA_OPT_EXT="-Xms1g -Xmx2g -Xmn768m -Dtls.enable=true -Dtls.test.mode.enable=false -Dtls.server.certPath=$WORK/server.crt -Dtls.server.keyPath=$WORK/server.key -Dtls.server.authClient=true -Dtls.server.trustCertPath=$WORK/ca.crt"
nohup "$DIST/bin/mqbroker" -c "$WORK/broker.conf" > "$WORK/broker2.log" 2>&1 &
for i in $(seq 1 60); do
    if port_open "$BROKER_PORT"; then sleep 2; break; fi
    sleep 1
    if [ "$i" = "60" ]; then echo "=== mTLS broker not ready after 60s (see $WORK/broker2.log) ===" >&2; exit 2; fi
done
echo "=== [leg 3/3] mtls (mutual auth with a client certificate) ==="
"$PYTHON_BIN" verify_tls_live.py "$NS" --leg mtls --caCert "$WORK/ca.crt" --serverName 127.0.0.1 --clientCert "$WORK/client.crt" --clientKey "$WORK/client.key" || RC=1
echo "=== exit=$RC ==="
exit $RC
