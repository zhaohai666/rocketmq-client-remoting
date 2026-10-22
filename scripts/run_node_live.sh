#!/bin/bash
# Live verification entry point for the Node.js client (mirrors the approach of run_go_*_live.sh / run_php_live.sh).
#
# Usage: bash scripts/run_node_live.sh producer|consumer|pull|lite_pull|admin|pop|pop_batch_ack|request_reply|admin_ns|acl|tls|check_config|fixes2|batch|slave [namesrv]
#
# Cluster: assumes one is already running by default (both ns+broker ports reachable). If not,
# this script starts its own (finds pids with jps for cleanup, stopping only the instance it
# started; anything already running is left untouched — rmq_test_broker.sh locates pids via
# /bin/ps, which is disabled in the sandbox, so namesrv+broker are started inline here).
#
# ⚠ Cluster start + waiting for ports + running verification + kill must all be chained inside
# this script: when a foreground command returns, the background JVMs are reaped.
set -u

WHICH=${1:-producer}
NS=${2:-${ROCKETMQ_NAMESRV:-127.0.0.1:9876}}
BROKER_PORT=${ROCKETMQ_BROKER_PORT:-10911}
NS_PORT=${ROCKETMQ_NS_PORT:-9876}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
NODEJS_DIR="$ROOT/nodeJs"
NODE_BIN=${NODE_BIN:-/Users/haizai/.workbuddy/binaries/node/versions/22.22.2-6/bin/node}
# fall back to PATH node when the managed binary is missing (e.g. Windows)
if ! [ -x "$NODE_BIN" ] && ! command -v "$NODE_BIN" >/dev/null 2>&1; then
    NODE_BIN=node
fi
DIST=${ROCKETMQ_DIST:-/Users/haizai/project/jingsai/roocketmq/zhaohai666-rocketmq/distribution/target/rocketmq-5.5.1/rocketmq-5.5.1}
WORK=/tmp/rmq_node_live
STAMP=$(date +%s)
export ROCKETMQ_NAMESRV="$NS"

case "$WHICH" in
  producer)      EXAMPLE="examples/live_producer.ts"       ;;
  consumer)      EXAMPLE="examples/live_consumer.ts"       ;;
  pull)          EXAMPLE="examples/live_pull.ts"           ;;
  lite_pull)     EXAMPLE="examples/live_lite_pull.ts"      ;;
  admin)         EXAMPLE="examples/live_admin.ts"          ;;
  pop)           EXAMPLE="examples/live_pop.ts"            ;;
  pop_batch_ack) EXAMPLE="examples/live_pop_batch_ack.ts"  ;;
  request_reply) EXAMPLE="examples/live_request_reply.ts"  ;;
  admin_ns)      EXAMPLE="examples/live_admin_ns.ts"       ;;
  acl)           EXAMPLE="examples/live_acl.ts"            ;;
  tls)           EXAMPLE="examples/live_tls.ts"            ;;
  check_config)  EXAMPLE="examples/live_check_client_config.ts" ;;
  fixes2)        EXAMPLE="examples/live_fixes2.ts"              ;;
  slave)         EXAMPLE="examples/live_slave_only.ts"          ;;
  batch)         EXAMPLE="examples/live_auto_batch.ts"          ;;
  *) echo "unknown example: $WHICH (producer|consumer|pull|lite_pull|admin|pop|pop_batch_ack|request_reply|admin_ns|acl|tls|check_config|fixes2|batch|slave)" >&2 ; exit 2 ;;
esac

port_open() {
    "$NODE_BIN" -e "const s=require('net').createConnection({host:'127.0.0.1',port:$1});s.on('connect',()=>{s.end();process.exit(0)});s.on('error',()=>process.exit(1));setTimeout(()=>process.exit(1),500)" 2>/dev/null
}

# ---------------------------------------------------------------- tls branch (exclusive cluster + self-managed certs / three broker rounds)
# tlsEnable on nodeJs is a **process-wide global** switch: the namesrv connection also goes
# through TLS, so namesrv+broker both need -Dtls.enable=true + the same server certificate
# (permissive). Three legs: plain_tls (trusts self-signed) / ca_verify (strict CA verification) /
# mtls (broker requires client certificates).
if [ "$WHICH" = "tls" ]; then
    if port_open "$NS_PORT" || port_open "$BROKER_PORT"; then
        echo "=== the tls branch needs a TLS-dedicated cluster, but a cluster is already running on ${NS_PORT}/${BROKER_PORT} (most likely plaintext). Stop it first. ===" >&2
        exit 2
    fi
    command -v openssl >/dev/null 2>&1 || { echo "openssl is not available" >&2; exit 2; }
    TWORK=/tmp/rmq_node_live_tls
    rm -rf "$TWORK"
    mkdir -p "$TWORK/store"
    echo "=== generating test certificates (CA / server(SAN: IP:127.0.0.1,DNS:localhost) / client) ==="
    (cd "$TWORK" \
        && openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 2 -subj "/CN=rmq-node-test-ca" >/dev/null 2>&1 \
        && openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/CN=127.0.0.1" >/dev/null 2>&1 \
        && printf "subjectAltName=IP:127.0.0.1,DNS:localhost\n" > server.ext \
        && openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 2 -extfile server.ext >/dev/null 2>&1 \
        && openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr -subj "/CN=rmq-node-test-client" >/dev/null 2>&1 \
        && openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 2 >/dev/null 2>&1) \
        || { echo "certificate generation failed" >&2; exit 2; }
    # RocketMQ's PemReader only accepts PKCS#8 ("BEGIN PRIVATE KEY"); when an older LibreSSL
    # emits PKCS#1 ("BEGIN RSA PRIVATE KEY") by default, convert it explicitly.
    srv_hdr=$(head -1 "$TWORK/server.key")
    case "$srv_hdr" in
        *"BEGIN PRIVATE KEY"*) : ;;
        *) openssl pkcs8 -topk8 -nocrypt -in "$TWORK/server.key" -out "$TWORK/server.p8.key" \
            && mv "$TWORK/server.p8.key" "$TWORK/server.key" ;;
    esac
    cat > "$TWORK/broker.conf" <<EOF
brokerClusterName = DefaultCluster
brokerName = broker-a
brokerId = 0
deleteWhen = 04
fileReservedTime = 48
brokerRole = ASYNC_MASTER
flushDiskType = ASYNC_FLUSH
namesrvAddr = 127.0.0.1:$NS_PORT
listenPort = $BROKER_PORT
storePathRootDir = $TWORK/store
autoCreateTopicEnable = true
EOF
    export ROCKETMQ_HOME="$DIST"
    export JAVA_OPT_EXT="-Xms1g -Xmx2g -Xmn768m -Dtls.enable=true -Dtls.test.mode.enable=false -Dtls.server.certPath=$TWORK/server.crt -Dtls.server.keyPath=$TWORK/server.key"
    STARTED_BROKER=1
    nohup "$DIST/bin/mqnamesrv" > "$TWORK/ns.log" 2>&1 &
    nohup "$DIST/bin/mqbroker" -c "$TWORK/broker.conf" > "$TWORK/broker.log" 2>&1 &
    for i in $(seq 1 60); do
        if port_open "$NS_PORT" && port_open "$BROKER_PORT"; then sleep 2; break; fi
        sleep 1
        if [ "$i" = "60" ]; then
            echo "=== TLS cluster not ready within 60s (see $TWORK/ns.log / $TWORK/broker.log) ===" >&2
            exit 2
        fi
    done
    # Wait for the broker to register into the nameserver (a plaintext route query is fine on
    # a permissive server).
    for i in $(seq 1 60); do
        if "$NODE_BIN" --experimental-strip-types --no-warnings -e "
            const { MQClient } = await import('$NODEJS_DIR/src/client/mq_client.ts');
            const { RemotingCommand } = await import('$NODEJS_DIR/src/remoting/remotingCommand.ts');
            const { RequestCode, ResponseCode } = await import('$NODEJS_DIR/src/remoting/codes.ts');
            const c = new MQClient('tls-route-wait', '127.0.0.1:$NS_PORT');
            const req = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, null);
            req.addExtField('topic', 'TBW102');
            try {
              const resp = await c.remotingClient.invokeSync('127.0.0.1:$NS_PORT', req, 2000);
              process.exit(resp.code === ResponseCode.SUCCESS ? 0 : 1);
            } catch (e) { process.exit(1); }
        " 2>/dev/null; then break; fi
        sleep 1
        if [ "$i" = "60" ]; then echo "=== broker not registered into nameserver within 60s ===" >&2; exit 2; fi
    done

    cd "$NODEJS_DIR" || exit 1
    RC=0
    echo "=== [leg 1/3] plain_tls (test-mode: trust self-signed) ==="
    "$NODE_BIN" --experimental-strip-types "$EXAMPLE" --ns "$NS" --stamp "$STAMP" --leg plain_tls --caCert "$TWORK/ca.crt" --serverName 127.0.0.1 || RC=1
    echo "=== [leg 2/3] ca_verify (verify certificate chain + hostname SAN) ==="
    "$NODE_BIN" --experimental-strip-types "$EXAMPLE" --ns "$NS" --stamp "$STAMP" --leg ca_verify --caCert "$TWORK/ca.crt" --serverName 127.0.0.1 || RC=1

    echo "=== restarting broker (adding -Dtls.server.authClient=true → mTLS) ==="
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
    # Port closed ≠ store file lock ($WORK/store/lock) released: a cleanly exiting old JVM
    # still needs to delete the lock file, and starting too early hits "Lock failed, MQ
    # already started". Allow a 3s buffer.
    sleep 3
    # mTLS: the correct switch for the server side to require client certificates is
    # tls.server.authClient (tls.client.authServer means "client authenticates the server";
    # setting it poisons the broker→namesrv channel).
    export JAVA_OPT_EXT="-Xms1g -Xmx2g -Xmn768m -Dtls.enable=true -Dtls.test.mode.enable=false -Dtls.server.certPath=$TWORK/server.crt -Dtls.server.keyPath=$TWORK/server.key -Dtls.server.authClient=true -Dtls.server.trustCertPath=$TWORK/ca.crt"
    nohup "$DIST/bin/mqbroker" -c "$TWORK/broker.conf" > "$TWORK/broker2.log" 2>&1 &
    for i in $(seq 1 60); do
        if port_open "$BROKER_PORT"; then sleep 2; break; fi
        sleep 1
        if [ "$i" = "60" ]; then echo "=== mTLS broker not ready within 60s (see $TWORK/broker2.log) ===" >&2; exit 2; fi
    done
    # Wait for the broker to re-register into the nameserver (ready only once the createTopic
    # TBW102 route is in place).
    for i in $(seq 1 60); do
        if "$NODE_BIN" --experimental-strip-types --no-warnings -e "
            const { MQClient } = await import('$NODEJS_DIR/src/client/mq_client.ts');
            const { RemotingCommand } = await import('$NODEJS_DIR/src/remoting/remotingCommand.ts');
            const { RequestCode, ResponseCode } = await import('$NODEJS_DIR/src/remoting/codes.ts');
            const c = new MQClient('tls-route-wait2', '127.0.0.1:$NS_PORT');
            const req = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, null);
            req.addExtField('topic', 'TBW102');
            try {
              const resp = await c.remotingClient.invokeSync('127.0.0.1:$NS_PORT', req, 2000);
              process.exit(resp.code === ResponseCode.SUCCESS ? 0 : 1);
            } catch (e) { process.exit(1); }
        " 2>/dev/null; then break; fi
        sleep 1
        if [ "$i" = "60" ]; then echo "=== mTLS broker not registered into nameserver within 60s ===" >&2; exit 2; fi
    done
    echo "=== [leg 3/3] mtls (client certificates, mutual authentication) ==="
    "$NODE_BIN" --experimental-strip-types "$EXAMPLE" --ns "$NS" --stamp "$STAMP" --leg mtls --caCert "$TWORK/ca.crt" --serverName 127.0.0.1 --clientCert "$TWORK/client.crt" --clientKey "$TWORK/client.key" || RC=1
    echo "=== exit=$RC ==="
    exit $RC
fi

STARTED_BROKER=0
cleanup() {
    if [ "$STARTED_BROKER" = "1" ]; then
        echo "--- Done: stopping the cluster started by this script (finding pids via jps) ---"
        local pids
        pids=$(jps -l 2>/dev/null | awk '/NamesrvStartup|BrokerStartup/ {print $1}')
        if [ -n "$pids" ]; then
            # shellcheck disable=SC2086
            kill $pids 2>/dev/null || true
            sleep 5
            pids=$(jps -l 2>/dev/null | awk '/NamesrvStartup|BrokerStartup/ {print $1}')
            if [ -n "$pids" ]; then
                # shellcheck disable=SC2086
                kill -9 $pids 2>/dev/null || true
            fi
        fi
    fi
}
trap cleanup EXIT

if ! port_open "$NS_PORT" || ! port_open "$BROKER_PORT"; then
    echo "=== cluster not listening (${NS_PORT}/${BROKER_PORT}), starting a local test cluster ==="
    [ -d "$DIST" ] || { echo "RocketMQ distribution directory not found: ${DIST} (override with ROCKETMQ_DIST)" >&2; exit 2; }
    mkdir -p "$WORK/store"
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
    export JAVA_OPT_EXT="-Xms1g -Xmx2g -Xmn768m"
    nohup "$DIST/bin/mqnamesrv" > "$WORK/ns.log" 2>&1 &
    nohup "$DIST/bin/mqbroker" -c "$WORK/broker.conf" > "$WORK/broker.log" 2>&1 &
    STARTED_BROKER=1
    for i in $(seq 1 60); do
        if port_open "$NS_PORT" && port_open "$BROKER_PORT"; then
            echo "=== cluster ready (waited ${i}x1s) ==="
            sleep 2
            break
        fi
        sleep 1
        if [ "$i" = "60" ]; then
            echo "=== cluster not ready within 60s, giving up (see $WORK/ns.log / $WORK/broker.log) ===" >&2
            exit 2
        fi
    done
    # Port open ≠ TBW102 registered: before the route reaches the nameserver, createTopic dies
    # with "No route info of default topic TBW102". Wait for a real route query to succeed.
    for i in $(seq 1 60); do
        if "$NODE_BIN" --experimental-strip-types --no-warnings -e "
            const { MQClient } = await import('$NODEJS_DIR/src/client/mq_client.ts');
            const { RemotingCommand } = await import('$NODEJS_DIR/src/remoting/remotingCommand.ts');
            const { RequestCode, ResponseCode } = await import('$NODEJS_DIR/src/remoting/codes.ts');
            const c = new MQClient('route-wait', '$NS');
            const req = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, null);
            req.addExtField('topic', 'TBW102');
            try {
              const resp = await c.remotingClient.invokeSync('$NS', req, 2000);
              process.exit(resp.code === ResponseCode.SUCCESS ? 0 : 1);
            } catch (e) { process.exit(1); }
        " 2>/dev/null; then
            echo "=== TBW102 route registered ==="
            break
        fi
        sleep 1
        if [ "$i" = "60" ]; then
            echo "=== broker not registered into nameserver within 60s, giving up ===" >&2
            exit 2
        fi
    done
fi

echo "=== node live: $EXAMPLE (ns=$NS) ==="
cd "$NODEJS_DIR"
"$NODE_BIN" --experimental-strip-types "$EXAMPLE" --ns "$NS" --stamp "$STAMP"
RC=$?
echo "=== exit=$RC ==="
exit $RC
