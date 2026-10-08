#!/bin/bash
# C++ 端 TLS 严格校验真机验证（对标 run_php_live.sh tls / run_node_live.sh tls）：
# 自管证书（CA/server/client）+ 自起 TLS 集群，跑三腿：
#   leg1 plain_tls  setTlsEnable(true)，信任自签（test-mode）
#   leg2 ca_verify  TlsOptions.caCert —— 严格校验证书链 + 主机名 SAN
#   leg3 mtls       重启 broker（-Dtls.server.authClient=true）+ 客户端证书双向认证
#
# 用法: bash scripts/run_cpp_tls_live.sh [namesrv]
set -u

NS=${1:-${ROCKETMQ_NAMESRV:-127.0.0.1:9876}}
NS_PORT=${ROCKETMQ_NS_PORT:-9876}
BROKER_PORT=${ROCKETMQ_BROKER_PORT:-10911}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
CPP_DIR="$ROOT/cpp"
PROG="$CPP_DIR/build/examples/rmq_live_tls"
DIST=${ROCKETMQ_DIST:-/Users/haizai/project/jingsai/roocketmq/zhaohai666-rocketmq/distribution/target/rocketmq-5.5.1/rocketmq-5.5.1}
WORK=/tmp/rmq_cpp_live_tls
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

echo "=== 构建 C++ examples（rmq_live_tls） ==="
cmake --build "$CPP_DIR/build" --target rmq_live_tls -j8 >/dev/null 2>&1 \
    || { echo "cmake build 失败" >&2; exit 2; }
[ -x "$PROG" ] || { echo "找不到构建产物: ${PROG}" >&2; exit 2; }

mkdir -p "$WORK/store"
echo "=== 生成测试证书（CA / server(SAN: IP:127.0.0.1,DNS:localhost) / client） ==="
(cd "$WORK" \
    && openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 2 -subj "/CN=rmq-cpp-test-ca" >/dev/null 2>&1 \
    && openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/CN=127.0.0.1" >/dev/null 2>&1 \
    && printf "subjectAltName=IP:127.0.0.1,DNS:localhost\n" > server.ext \
    && openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 2 -extfile server.ext >/dev/null 2>&1 \
    && openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr -subj "/CN=rmq-cpp-test-client" >/dev/null 2>&1 \
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

# 端口通了 ≠ TBW102 已注册：路由没进 nameserver 之前 createTopic 会死于
# "No route info of default topic TBW102"，消费者先起时还会错过整个消费窗口。
# 等一个真正的 route 查询成功（node 版 run_node_live.sh 的 route-wait 同款）。
wait_tbw102() {
    for i in $(seq 1 30); do
        if "$DIST/bin/mqadmin" topicRoute -n "$NS" -t TBW102 2>/dev/null | /usr/bin/grep -qF 'broker-a'; then
            echo "=== TBW102 路由已注册 ==="
            return 0
        fi
        sleep 2
    done
    echo "=== TBW102 30 轮未注册进 nameserver ===" >&2
    return 1
}

RC=0
wait_tbw102 || exit 2
echo "=== [leg 1/3] plain_tls（test-mode：信任自签） ==="
"$PROG" "$NS" "CppTls_plain_$STAMP" "GID_CppTls_plain_$STAMP" plain_tls || RC=1
echo "=== [leg 2/3] ca_verify（真校验证书链 + 主机名 SAN） ==="
"$PROG" "$NS" "CppTls_ca_$STAMP" "GID_CppTls_ca_$STAMP" ca_verify "$WORK/ca.crt" 127.0.0.1 || RC=1

echo "=== 重启 broker（追加 -Dtls.server.authClient=true → mTLS） ==="
# 端口关了 ≠ 进程退了 ≠ store 文件锁释放了。等 jps 里 BrokerStartup 彻底消失
# 再多给 3s；拉起失败（Lock failed）时整段重试，最多 3 次。
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
        # jps 看不到 ≠ 进程死了：JVM 启动后 hsperfdata 要几秒才出现，
        # 必须连续 3 秒看不到才判死（否则会把刚拉起的 broker 误杀）。
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
        echo "=== mTLS broker 已就绪（第 ${attempt} 次尝试） ==="
        break
    fi
    if [ "$attempt" = "3" ]; then
        echo "=== mTLS broker 3 次尝试均未就绪（见 $WORK/broker2.log） ===" >&2
        exit 2
    fi
    echo "=== mTLS broker 第 ${attempt} 次尝试失败，重试 ==="
done
echo "=== [leg 3/3] mtls（客户端证书双向认证） ==="
wait_tbw102 || exit 2
"$PROG" "$NS" "CppTls_mtls_$STAMP" "GID_CppTls_mtls_$STAMP" mtls "$WORK/ca.crt" 127.0.0.1 "$WORK/client.crt" "$WORK/client.key" || RC=1
echo "=== exit=$RC ==="
exit $RC
