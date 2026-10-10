#!/bin/bash
# Node.js 端的真机验证入口（对照 run_go_*_live.sh / run_php_live.sh 的口径）。
#
# 用法: bash scripts/run_node_live.sh producer|consumer|pull|lite_pull|admin|pop|request_reply|admin_ns|acl|tls|check_config|fixes2 [namesrv]
#
# 集群：默认假定已经在跑（ns+broker 端口都通）。没起会自拉一套（jps 找 pid 收尾，
# 只停自己起来的那一次；已经在跑的不碰——rmq_test_broker.sh 靠 /bin/ps 找 pid，
# 沙箱里被禁，所以这里直接内联起 namesrv+broker）。
#
# ⚠ 集群 start + 等端口 + 跑验证 + kill 必须都在本脚本内串完：前台返回会回收后台 JVM。
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
  request_reply) EXAMPLE="examples/live_request_reply.ts"  ;;
  admin_ns)      EXAMPLE="examples/live_admin_ns.ts"       ;;
  acl)           EXAMPLE="examples/live_acl.ts"            ;;
  tls)           EXAMPLE="examples/live_tls.ts"            ;;
  check_config)  EXAMPLE="examples/live_check_client_config.ts" ;;
  fixes2)        EXAMPLE="examples/live_fixes2.ts"              ;;
  slave)         EXAMPLE="examples/live_slave_only.ts"          ;;
  *) echo "unknown example: $WHICH (producer|consumer|pull|lite_pull|admin|pop|request_reply|admin_ns|acl|tls|check_config|fixes2|slave)" >&2 ; exit 2 ;;
esac

port_open() {
    "$NODE_BIN" -e "const s=require('net').createConnection({host:'127.0.0.1',port:$1});s.on('connect',()=>{s.end();process.exit(0)});s.on('error',()=>process.exit(1));setTimeout(()=>process.exit(1),500)" 2>/dev/null
}

# ---------------------------------------------------------------- tls 分支（独占集群 + 自管证书/三轮 broker）
# nodeJs 的 tlsEnable 是**进程级全局**开关：namesrv 连接也走 TLS，
# 所以 namesrv+broker 都要 -Dtls.enable=true + 同一套 server 证书（permissive）。
# 三腿：plain_tls（信任自签）/ ca_verify（严格 CA 校验）/ mtls（broker 要求客户端证书）。
if [ "$WHICH" = "tls" ]; then
    if port_open "$NS_PORT" || port_open "$BROKER_PORT"; then
        echo "=== tls 分支需要 TLS 专用集群，但 ${NS_PORT}/${BROKER_PORT} 已有集群在跑（多半是明文）。请先停掉它。 ===" >&2
        exit 2
    fi
    command -v openssl >/dev/null 2>&1 || { echo "openssl 不可用" >&2; exit 2; }
    TWORK=/tmp/rmq_node_live_tls
    rm -rf "$TWORK"
    mkdir -p "$TWORK/store"
    echo "=== 生成测试证书（CA / server(SAN: IP:127.0.0.1,DNS:localhost) / client） ==="
    (cd "$TWORK" \
        && openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 2 -subj "/CN=rmq-node-test-ca" >/dev/null 2>&1 \
        && openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/CN=127.0.0.1" >/dev/null 2>&1 \
        && printf "subjectAltName=IP:127.0.0.1,DNS:localhost\n" > server.ext \
        && openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 2 -extfile server.ext >/dev/null 2>&1 \
        && openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr -subj "/CN=rmq-node-test-client" >/dev/null 2>&1 \
        && openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 2 >/dev/null 2>&1) \
        || { echo "证书生成失败" >&2; exit 2; }
    # RocketMQ 的 PemReader 只吃 PKCS#8（"BEGIN PRIVATE KEY"）；老 LibreSSL 默认吐
    # PKCS#1（"BEGIN RSA PRIVATE KEY"）时显式转一道。
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
            echo "=== TLS 集群 60s 未就绪（见 $TWORK/ns.log / $TWORK/broker.log） ===" >&2
            exit 2
        fi
    done
    # 等 broker 注册进 nameserver（路由查询在 permissive 服务端上走明文即可）。
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
        if [ "$i" = "60" ]; then echo "=== broker 60s 未注册进 nameserver ===" >&2; exit 2; fi
    done

    cd "$NODEJS_DIR" || exit 1
    RC=0
    echo "=== [leg 1/3] plain_tls（test-mode：信任自签） ==="
    "$NODE_BIN" --experimental-strip-types "$EXAMPLE" --ns "$NS" --stamp "$STAMP" --leg plain_tls --caCert "$TWORK/ca.crt" --serverName 127.0.0.1 || RC=1
    echo "=== [leg 2/3] ca_verify（真校验证书链 + 主机名 SAN） ==="
    "$NODE_BIN" --experimental-strip-types "$EXAMPLE" --ns "$NS" --stamp "$STAMP" --leg ca_verify --caCert "$TWORK/ca.crt" --serverName 127.0.0.1 || RC=1

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
    # 端口关了 ≠ store 文件锁（$WORK/store/lock）已释放：老 JVM 干净退出还要删锁文件，
    # 抢跑会 "Lock failed, MQ already started"。给 3s 缓冲。
    sleep 3
    # mTLS：server 侧要求客户端证书的正确开关是 tls.server.authClient
    # （tls.client.authServer 是「client 认证 server」，写它会毒到 broker→namesrv 通道）。
    export JAVA_OPT_EXT="-Xms1g -Xmx2g -Xmn768m -Dtls.enable=true -Dtls.test.mode.enable=false -Dtls.server.certPath=$TWORK/server.crt -Dtls.server.keyPath=$TWORK/server.key -Dtls.server.authClient=true -Dtls.server.trustCertPath=$TWORK/ca.crt"
    nohup "$DIST/bin/mqbroker" -c "$TWORK/broker.conf" > "$TWORK/broker2.log" 2>&1 &
    for i in $(seq 1 60); do
        if port_open "$BROKER_PORT"; then sleep 2; break; fi
        sleep 1
        if [ "$i" = "60" ]; then echo "=== mTLS broker 60s 未就绪（见 $TWORK/broker2.log） ===" >&2; exit 2; fi
    done
    # 等 broker 重新注册进 nameserver（createTopic 的 TBW102 路由就绪才算就绪）。
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
        if [ "$i" = "60" ]; then echo "=== mTLS broker 60s 未注册进 nameserver ===" >&2; exit 2; fi
    done
    echo "=== [leg 3/3] mtls（客户端证书双向认证） ==="
    "$NODE_BIN" --experimental-strip-types "$EXAMPLE" --ns "$NS" --stamp "$STAMP" --leg mtls --caCert "$TWORK/ca.crt" --serverName 127.0.0.1 --clientCert "$TWORK/client.crt" --clientKey "$TWORK/client.key" || RC=1
    echo "=== exit=$RC ==="
    exit $RC
fi

STARTED_BROKER=0
cleanup() {
    if [ "$STARTED_BROKER" = "1" ]; then
        echo "--- 收工：停掉本脚本起的集群（jps 找 pid） ---"
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
    echo "=== 集群未监听（${NS_PORT}/${BROKER_PORT}），拉起本地测试集群 ==="
    [ -d "$DIST" ] || { echo "找不到 RocketMQ 分发目录: ${DIST}（用 ROCKETMQ_DIST 覆盖）" >&2; exit 2; }
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
            echo "=== 集群就绪（等待 ${i}x1s） ==="
            sleep 2
            break
        fi
        sleep 1
        if [ "$i" = "60" ]; then
            echo "=== 集群 60s 未就绪，放弃（见 $WORK/ns.log / $WORK/broker.log） ===" >&2
            exit 2
        fi
    done
    # 端口通了 ≠ TBW102 已注册：路由没进 nameserver 之前 createTopic 会死于
    # "No route info of default topic TBW102"。等一个真正的 route 查询成功。
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
            echo "=== TBW102 路由已注册 ==="
            break
        fi
        sleep 1
        if [ "$i" = "60" ]; then
            echo "=== broker 60s 未注册进 nameserver，放弃 ===" >&2
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
