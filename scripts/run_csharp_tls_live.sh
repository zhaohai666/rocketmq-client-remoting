#!/bin/bash
# C# 端 TLS 严格校验真机验证（对标 run_php_live.sh tls / run_node_live.sh tls）：
# 自管证书（CA/server/client）+ 自起 TLS 集群，跑三腿：
#   leg1 plain      tlsEnable=True，信任自签（test-mode）
#   leg2 ca_verify  TlsOptions.CaCert —— 严格校验证书链 + 主机名 SAN
#   leg3 mtls       重启 broker（-Dtls.server.authClient=true）+ 客户端证书双向认证
#
# 用法: bash scripts/run_csharp_tls_live.sh [namesrv]
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
        echo "--- 收工：停掉本脚本起的集群（jps 找 pid） ---"
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
    echo "=== tls 分支需要独占集群，但 ${NS_PORT}/${BROKER_PORT} 已有集群在跑。请先停掉它。 ===" >&2
    exit 2
fi
[ -d "$DIST" ] || { echo "找不到 RocketMQ 分发目录: ${DIST}（用 ROCKETMQ_DIST 覆盖）" >&2; exit 2; }
command -v openssl >/dev/null 2>&1 || { echo "openssl 不可用" >&2; exit 2; }

mkdir -p "$WORK/store"
echo "=== 生成测试证书（CA / server(SAN: IP:127.0.0.1,DNS:localhost) / client） ==="
(cd "$WORK" \
    && openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 2 -subj "/CN=rmq-cs-test-ca" >/dev/null 2>&1 \
    && openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/CN=127.0.0.1" >/dev/null 2>&1 \
    && printf "subjectAltName=IP:127.0.0.1,DNS:localhost\n" > server.ext \
    && openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 2 -extfile server.ext >/dev/null 2>&1 \
    && openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr -subj "/CN=rmq-cs-test-client" >/dev/null 2>&1 \
    && openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 2 >/dev/null 2>&1) \
    || { echo "证书生成失败" >&2; exit 2; }
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
echo "=== 起集群（namesrv+broker 双 permissive TLS） ==="
nohup "$DIST/bin/mqnamesrv" > "$WORK/ns.log" 2>&1 &
nohup "$DIST/bin/mqbroker" -c "$WORK/broker.conf" > "$WORK/broker.log" 2>&1 &
STARTED=1
for i in $(seq 1 60); do
    if port_open "$NS_PORT" && port_open "$BROKER_PORT"; then sleep 2; break; fi
    sleep 1
    if [ "$i" = "60" ]; then
        echo "=== TLS 集群 60s 未就绪（见 $WORK/ns.log / $WORK/broker.log） ===" >&2
        exit 2
    fi
done

echo "=== 构建 C# examples ==="
dotnet build "$PROJ" >/dev/null 2>&1 || { echo "dotnet build 失败" >&2; exit 2; }
PROG=$(ls "$CS_DIR"/examples/RocketMQ.Examples/bin/Debug/*/rmq.dll 2>/dev/null | head -1)
[ -z "$PROG" ] && PROG=$(ls "$CS_DIR"/examples/RocketMQ.Examples/bin/Debug/*/*.dll 2>/dev/null | head -1)
[ -z "$PROG" ] && { echo "找不到构建产物" >&2; exit 2; }

RC=0
echo "=== [leg 1/3] plain（test-mode：信任自签） ==="
dotnet "$PROG" tls "$NS" "CsTls_plain_$STAMP" "GID_CsTls_plain_$STAMP" plain || RC=1
echo "=== [leg 2/3] ca_verify（真校验证书链 + 主机名 SAN） ==="
dotnet "$PROG" tls "$NS" "CsTls_ca_$STAMP" "GID_CsTls_ca_$STAMP" ca_verify "" "$WORK/ca.crt" || RC=1

echo "=== 重启 broker（追加 -Dtls.server.authClient=true → mTLS） ==="
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
# 端口关了 ≠ store 文件锁已释放，给 3s 缓冲。
sleep 3
export JAVA_OPT_EXT="-Xms1g -Xmx2g -Xmn768m -Dtls.enable=true -Dtls.test.mode.enable=false -Dtls.server.certPath=$WORK/server.crt -Dtls.server.keyPath=$WORK/server.key -Dtls.server.authClient=true -Dtls.server.trustCertPath=$WORK/ca.crt"
nohup "$DIST/bin/mqbroker" -c "$WORK/broker.conf" > "$WORK/broker2.log" 2>&1 &
for i in $(seq 1 60); do
    if port_open "$BROKER_PORT"; then sleep 2; break; fi
    sleep 1
    if [ "$i" = "60" ]; then echo "=== mTLS broker 60s 未就绪（见 $WORK/broker2.log） ===" >&2; exit 2; fi
done
echo "=== [leg 3/3] mtls（客户端证书双向认证） ==="
dotnet "$PROG" tls "$NS" "CsTls_mtls_$STAMP" "GID_CsTls_mtls_$STAMP" mtls "" "$WORK/ca.crt" "$WORK/client.crt" "$WORK/client.key" || RC=1
echo "=== exit=$RC ==="
exit $RC
