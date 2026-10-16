#!/bin/bash
# Live verification entry point for the PHP client (mirrors the approach of run_go_*_live.sh / run_node_live.sh).
#
# Usage: bash scripts/run_php_live.sh redelivery|admin|compression|pop|tls|request_reply|pull|lite_qc|registration [namesrv]
#   redelivery   php/examples/live_redelivery.php  (optional third arg legs=all|s1,s2,s3,s4)
#   admin        php/examples/live_admin.php
#   pull         php/examples/live_pull.php        (optional third arg legs=all|s1,s2,s3;
#                 pull consumer: queues / balanced view fetchMessageQueuesInBalance / manual pull /
#                 offset commit read-back)
#   lite_qc      php/examples/live_lite_topic_queue_change.php
#                 (LitePull topic queue-set change listener: check period tightened to 1s + real
#                  scale-up/down => proves each comparison round queries routes on the fly; PHP has
#                  no threads, the main loop is driven by tick()+poll() each round)
#   compression  php/examples/live_compression.php (smoke only: a single php->php leg;
#                 the cross-language matrix goes through the php_* legs of scripts/compression_matrix.sh)
#   request_reply php/examples/live_request_reply.php (326 request-reply: responder + requester round trip)
#   registration php/examples/live_producer_registration.php (broker-side view of producer
#                 registration: GET_PRODUCER_CONNECTION_LIST(204) must list the clientId of a
#                 producer that only called start(), the 30s tick pump must keep it listed, and
#                 shutdown() must drop it)
#   pop          php/examples/live_pop.php         (optional third arg legs=all|s1,s2;
#                 **requires an exclusive cluster**: needs the four POP-specific broker.conf
#                 settings, see below)
#   tls          php/examples/live_tls.php         (three legs plain_tls/ca_verify/mtls;
#                 **requires an exclusive cluster**: namesrv+broker both need -Dtls.enable=true —
#                 tlsEnable on PHP is a process-wide global switch too, so the namesrv connection
#                 also goes through TLS; the script generates CA/server(SAN:IP:127.0.0.1,DNS:localhost)
#                 /client certs via openssl, leg1+2 use one broker round, leg3 restarts with
#                 tls.client.authServer=true)
#
# ⚠ Difference from run_node_live.sh: rmq_test_broker.sh **cannot** be used — it locates
#   pids via /bin/ps, and /bin/ps is disabled by the sandbox in this environment. This script
#   starts the cluster itself and finds pids with jps to clean up, stopping only the instance
#   it started; any already-running cluster is left untouched.
#
# ⚠ Cluster start + waiting for ports + running verification + kill must all be chained inside
#   this script: when a sandboxed foreground command returns, the background JVMs are reaped.
set -u

WHICH=${1:-admin}
NS=${2:-${ROCKETMQ_NAMESRV:-127.0.0.1:9876}}
LEGS=${3:-all}
BROKER_PORT=${ROCKETMQ_BROKER_PORT:-10911}
NS_PORT=${ROCKETMQ_NS_PORT:-9876}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PHP_BIN=${PHP_BIN:-php}
DIST=${ROCKETMQ_DIST:-/Users/haizai/project/jingsai/roocketmq/zhaohai666-rocketmq/distribution/target/rocketmq-5.5.1/rocketmq-5.5.1}
WORK=/tmp/rmq_php_live
STAMP=$(date +%s)

case "$WHICH" in
  redelivery)    EXAMPLE="examples/live_redelivery.php"     ;;
  admin)         EXAMPLE="examples/live_admin.php"          ;;
  pull)          EXAMPLE="examples/live_pull.php"           ;;
  lite_qc)       EXAMPLE="examples/live_lite_topic_queue_change.php" ;;
  compression)   EXAMPLE="examples/live_compression.php"    ;;
  pop)           EXAMPLE="examples/live_pop.php"            ;;
  tls)           EXAMPLE="examples/live_tls.php"            ;;
  request_reply) EXAMPLE="examples/live_request_reply.php"  ;;
  registration)  EXAMPLE="examples/live_producer_registration.php" ;;
  static_topic)  EXAMPLE="examples/live_static_topic.php"   ;;
  *) echo "unknown example: $WHICH (redelivery|admin|compression|pop|tls|request_reply|pull|lite_qc|static_topic|registration)" >&2; exit 2 ;;
esac

port_open() {
    "$PHP_BIN" -r 'exit(@fsockopen("127.0.0.1",'"$2"',$e,$s,500)?0:1);' 2>/dev/null
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
            if [ -n "$pids" ]; then
                # shellcheck disable=SC2086
                kill -9 $pids 2>/dev/null || true
            fi
        fi
    fi
}
trap cleanup EXIT

# ---------------------------------------------------------------- tls branch (self-managed two broker rounds)
# tlsEnable on PHP is a **process-wide global** switch: the namesrv connection also goes
# through TLS, so namesrv+broker both need -Dtls.enable=true + the same server certificate (permissive).
if [ "$WHICH" = "tls" ]; then
    if port_open x "$NS_PORT" || port_open x "$BROKER_PORT"; then
        echo "=== the tls branch needs a TLS-dedicated cluster, but a cluster is already running on ${NS_PORT}/${BROKER_PORT}" >&2
        echo "    (most likely plaintext; the TLS client handshake would surely fail). Stop it first before running tls. ===" >&2
        exit 2
    fi
    command -v openssl >/dev/null 2>&1 || { echo "openssl is not available" >&2; exit 2; }
    TWORK=/tmp/rmq_php_live_tls
    rm -rf "$TWORK"
    mkdir -p "$TWORK/store"
    echo "=== generating test certificates (CA / server(SAN: IP:127.0.0.1,DNS:localhost) / client) ==="
    (cd "$TWORK" \
        && openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 2 -subj "/CN=rmq-php-test-ca" >/dev/null 2>&1 \
        && openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/CN=127.0.0.1" >/dev/null 2>&1 \
        && printf "subjectAltName=IP:127.0.0.1,DNS:localhost\n" > server.ext \
        && openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 2 -extfile server.ext >/dev/null 2>&1 \
        && openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr -subj "/CN=rmq-php-test-client" >/dev/null 2>&1 \
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
    # leg1+2: namesrv+broker both run permissive TLS (no client certificate requirement).
    # ⚠ When tls.test.mode.enable stays at its default true, the server **ignores certPath** and
    #   generates a temporary self-signed certificate on the fly — leg1 (trusts everything) passes,
    #   but leg2's CA verification always fails. Test mode must be explicitly disabled so the
    #   server loads server.crt/keyPath.
    export JAVA_OPT_EXT="-Xms1g -Xmx2g -Xmn768m -Dtls.enable=true -Dtls.test.mode.enable=false -Dtls.server.certPath=$TWORK/server.crt -Dtls.server.keyPath=$TWORK/server.key"
    nohup "$DIST/bin/mqnamesrv" > "$TWORK/ns.log" 2>&1 &
    nohup "$DIST/bin/mqbroker" -c "$TWORK/broker.conf" > "$TWORK/broker.log" 2>&1 &
    STARTED=1
    for i in $(seq 1 60); do
        if port_open x "$NS_PORT" && port_open x "$BROKER_PORT"; then
            sleep 2
            break
        fi
        sleep 1
        if [ "$i" = "60" ]; then
            echo "=== TLS cluster not ready within 60s (see $TWORK/ns.log / $TWORK/broker.log) ===" >&2
            exit 2
        fi
    done
    if ! "$PHP_BIN" "$ROOT/php/examples/wait_broker_route.php" "127.0.0.1:$NS_PORT" 60; then
        echo "=== broker not registered into nameserver within 60s ===" >&2
        exit 2
    fi

    cd "$ROOT/php" || exit 1
    RC=0
    echo "=== [leg 1/3] plain_tls (test-mode: trust self-signed) ==="
    "$PHP_BIN" "$EXAMPLE" "$NS" plain_tls "$TWORK/ca.crt" 127.0.0.1 || RC=1
    echo "=== [leg 2/3] ca_verify (verify certificate chain + hostname SAN) ==="
    "$PHP_BIN" "$EXAMPLE" "$NS" ca_verify "$TWORK/ca.crt" 127.0.0.1 || RC=1

    echo "=== restarting broker (adding -Dtls.client.authServer=true → mTLS) ==="
    pids=$(jps -l 2>/dev/null | awk '/BrokerStartup/ {print $1}')
    # shellcheck disable=SC2086
    [ -n "$pids" ] && kill $pids 2>/dev/null
    for i in $(seq 1 15); do
        port_open x "$BROKER_PORT" || break
        sleep 1
        if [ "$i" = "15" ]; then
            pids=$(jps -l 2>/dev/null | awk '/BrokerStartup/ {print $1}')
            # shellcheck disable=SC2086
            [ -n "$pids" ] && kill -9 $pids 2>/dev/null
            sleep 2
        fi
    done
    # mTLS: for the broker (server side) to require a client certificate, the correct switch is
    # tls.server.authClient (tls.client.authServer means "client authenticates the server";
    # setting it poisons the broker→namesrv registration channel and broker registration fails
    # outright); trustCertPath points at the CA that issued the client certificate.
    export JAVA_OPT_EXT="-Xms1g -Xmx2g -Xmn768m -Dtls.enable=true -Dtls.test.mode.enable=false -Dtls.server.certPath=$TWORK/server.crt -Dtls.server.keyPath=$TWORK/server.key -Dtls.server.authClient=true -Dtls.server.trustCertPath=$TWORK/ca.crt"
    nohup "$DIST/bin/mqbroker" -c "$TWORK/broker.conf" > "$TWORK/broker2.log" 2>&1 &
    for i in $(seq 1 60); do
        if port_open x "$BROKER_PORT"; then
            sleep 2
            break
        fi
        sleep 1
        if [ "$i" = "60" ]; then
            echo "=== mTLS broker not ready within 60s (see $TWORK/broker2.log) ===" >&2
            exit 2
        fi
    done
    if ! "$PHP_BIN" "$ROOT/php/examples/wait_broker_route.php" "127.0.0.1:$NS_PORT" 60; then
        echo "=== mTLS broker not registered into nameserver within 60s ===" >&2
        exit 2
    fi
    echo "=== [leg 3/3] mtls (present client certificate, mutual authentication) ==="
    "$PHP_BIN" "$EXAMPLE" "$NS" mtls "$TWORK/ca.crt" 127.0.0.1 "$TWORK/client.crt" "$TWORK/client.key" || RC=1

    echo "=== exit=$RC ==="
    exit $RC
fi

# ---------------------------------------------------------------- pop branch exclusive-cluster check
# POP requires four hard broker.conf settings (timerWheelEnable etc.). An already-running
# cluster is mostly plain conf; silently reusing it would always hard-fail at S1 — rejecting
# outright beats a mismatched run.
if [ "$WHICH" = "pop" ] && { port_open x "$NS_PORT" || port_open x "$BROKER_PORT"; }; then
    echo "=== the pop branch needs a POP-dedicated cluster (timerWheelEnable=true and three more settings)," >&2
    echo "    but a cluster is already running on ${NS_PORT}/${BROKER_PORT}. Stop it first before running pop. ===" >&2
    exit 2
fi
if [ "$WHICH" = "pop" ]; then
    WORK=/tmp/rmq_php_live_pop
fi

if ! port_open x "$NS_PORT" || ! port_open x "$BROKER_PORT"; then
    echo "=== cluster not listening (${NS_PORT}/${BROKER_PORT}), starting a local test cluster ==="
    if [ ! -d "$DIST" ]; then
        echo "RocketMQ distribution directory not found: ${DIST} (override with ROCKETMQ_DIST)" >&2
        exit 2
    fi
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
    if [ "$WHICH" = "pop" ]; then
        cat >> "$WORK/broker.conf" <<EOF

# POP hard prerequisites (broker.conf needs these for the POP path to run)
timerWheelEnable = true
defaultMessageRequestMode = PULL
popResponseReturnActualRetryTopic = false
enablePopBatchAck = false
EOF
    fi
    export ROCKETMQ_HOME="$DIST"
    export JAVA_OPT_EXT="-Xms1g -Xmx2g -Xmn768m"
    nohup "$DIST/bin/mqnamesrv" > "$WORK/ns.log" 2>&1 &
    nohup "$DIST/bin/mqbroker" -c "$WORK/broker.conf" > "$WORK/broker.log" 2>&1 &
    STARTED=1

    for i in $(seq 1 60); do
        if port_open x "$NS_PORT" && port_open x "$BROKER_PORT"; then
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
    # Port open ≠ route registered: before TBW102 reaches the nameserver, every createTopic/send
    # dies with "No route info of default topic TBW102", so we must also wait for broker registration.
    if ! "$PHP_BIN" "$ROOT/php/examples/wait_broker_route.php" "127.0.0.1:$NS_PORT" 60; then
        echo "=== broker not registered into nameserver within 60s, giving up ===" >&2
        exit 2
    fi
fi

echo "=== php live: $EXAMPLE (ns=$NS) ==="
cd "$ROOT/php" || exit 1
if [ "$WHICH" = "redelivery" ] || [ "$WHICH" = "pop" ] || [ "$WHICH" = "pull" ]; then
    "$PHP_BIN" "$EXAMPLE" "$NS" "$LEGS"
elif [ "$WHICH" = "request_reply" ]; then
    # Two-process orchestration: the responder (consumer + producer.reply) starts in the
    # background; once READY is seen, run the requester (producer.request). The responder
    # exits on its own after receiving expect messages.
    RR_TOPIC="PhpReqRep_${STAMP}"
    RR_EXPECT=${RR_EXPECT:-3}
    "$PHP_BIN" "$EXAMPLE" responder "$NS" "$RR_TOPIC" "$RR_EXPECT" > "$WORK/rr_responder.log" 2>&1 &
    RESPONDER_PID=$!
    RESPONDER_OK=0
    for i in $(seq 1 40); do
        if ! kill -0 "$RESPONDER_PID" 2>/dev/null; then
            break
        fi
        if /usr/bin/grep -q RESPONDER_READY "$WORK/rr_responder.log" 2>/dev/null; then
            RESPONDER_OK=1
            break
        fi
        sleep 1
    done
    if [ "$RESPONDER_OK" != "1" ]; then
        echo "=== responder not ready within 40s (see $WORK/rr_responder.log) ===" >&2
        kill "$RESPONDER_PID" 2>/dev/null || true
        cat "$WORK/rr_responder.log"
        exit 2
    fi
    "$PHP_BIN" "$EXAMPLE" requester "$NS" "$RR_TOPIC" "$RR_EXPECT"
    RC=$?
    wait "$RESPONDER_PID" || true
    echo "--- responder output ($WORK/rr_responder.log) ---"
    cat "$WORK/rr_responder.log"
    exit $RC
elif [ "$WHICH" = "compression" ]; then
    "$PHP_BIN" "$EXAMPLE" send "PhpCompressSmoke_$STAMP" "GID_PhpCompressSmoke_$STAMP" 8192 "$NS" zlib \
      && "$PHP_BIN" "$EXAMPLE" recv "PhpCompressSmoke_$STAMP" "GID_PhpCompressSmokeR_$STAMP" 8192 "$NS"
else
    "$PHP_BIN" "$EXAMPLE" "$NS"
fi
RC=$?
echo "=== exit=$RC ==="
exit $RC
