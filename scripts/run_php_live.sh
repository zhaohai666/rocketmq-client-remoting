#!/bin/bash
# PHP 端的真机验证入口（对照 run_go_*_live.sh / run_node_live.sh 的口径）。
#
# 用法: bash scripts/run_php_live.sh redelivery|admin|compression|pop|tls|request_reply|pull|lite_qc [namesrv]
#   redelivery   php/examples/live_redelivery.php  （可选第三参 legs=all|s1,s2,s3,s4）
#   admin        php/examples/live_admin.php
#   pull         php/examples/live_pull.php        （可选第三参 legs=all|s1,s2,s3；
#                 拉模式消费者：队列 / 平衡视图 fetchMessageQueuesInBalance / 手动拉取 /
#                 位点提交回读）
#   lite_qc      php/examples/live_lite_topic_queue_change.php
#                 （LitePull topic 队列集合变更监听：检查周期压到 1s + 真实扩缩容 ⇒
#                  证明比对趟次现查路由；PHP 无线程，主循环每轮 tick()+poll() 驱动）
#   compression  php/examples/live_compression.php （仅 smoke：php→php 一条腿；
#                 跨语言矩阵走 scripts/compression_matrix.sh 的 php_* 腿）
#   request_reply php/examples/live_request_reply.php（326 请求-Reply：应答方 + 发起方往返）
#   pop          php/examples/live_pop.php         （可选第三参 legs=all|s1,s2；
#                 **必须独占集群**：要求 POP 专用 broker.conf 四件配置，见下）
#   tls          php/examples/live_tls.php         （三腿 plain_tls/ca_verify/mtls；
#                 **必须独占集群**：namesrv+broker 都要 -Dtls.enable=true——PHP 端
#                 PHP 的 tlsEnable 同样是进程级全局开关，namesrv 连接也走
#                 TLS；脚本自带 openssl 生成 CA/server(SAN:IP:127.0.0.1,DNS:localhost)
#                 /client 证书，leg1+2 一轮 broker，leg3 重启加 tls.client.authServer=true）
#
# ⚠ 与 run_node_live.sh 的差异：**不能**用 rmq_test_broker.sh——它靠 /bin/ps 找
#   pid，本环境 /bin/ps 被沙箱禁用。这里自己起集群、用 jps 找 pid 收尾，只停
#   自己起来的那一次；已经在跑的集群不碰。
#
# ⚠ 集群 start + 等端口 + 跑验证 + kill 必须都在本脚本内串完：沙箱前台命令返回
#   会回收后台 JVM。
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
  *) echo "unknown example: $WHICH (redelivery|admin|compression|pop|tls|request_reply|pull|lite_qc)" >&2; exit 2 ;;
esac

port_open() {
    "$PHP_BIN" -r 'exit(@fsockopen("127.0.0.1",'"$2"',$e,$s,500)?0:1);' 2>/dev/null
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
            if [ -n "$pids" ]; then
                # shellcheck disable=SC2086
                kill -9 $pids 2>/dev/null || true
            fi
        fi
    fi
}
trap cleanup EXIT

# ---------------------------------------------------------------- tls 分支（自管两轮 broker）
# PHP 的 tlsEnable 是**进程级全局**开关：namesrv 连接也走 TLS，
# 所以 namesrv+broker 都要 -Dtls.enable=true + 同一套 server 证书（permissive）。
if [ "$WHICH" = "tls" ]; then
    if port_open x "$NS_PORT" || port_open x "$BROKER_PORT"; then
        echo "=== tls 分支需要 TLS 专用集群，但 ${NS_PORT}/${BROKER_PORT} 已有集群在跑" >&2
        echo "    （多半是明文，TLS 客户端握手必失败）。请先停掉它再跑 tls。 ===" >&2
        exit 2
    fi
    command -v openssl >/dev/null 2>&1 || { echo "openssl 不可用" >&2; exit 2; }
    TWORK=/tmp/rmq_php_live_tls
    rm -rf "$TWORK"
    mkdir -p "$TWORK/store"
    echo "=== 生成测试证书（CA / server(SAN: IP:127.0.0.1,DNS:localhost) / client） ==="
    (cd "$TWORK" \
        && openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 2 -subj "/CN=rmq-php-test-ca" >/dev/null 2>&1 \
        && openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/CN=127.0.0.1" >/dev/null 2>&1 \
        && printf "subjectAltName=IP:127.0.0.1,DNS:localhost\n" > server.ext \
        && openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 2 -extfile server.ext >/dev/null 2>&1 \
        && openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr -subj "/CN=rmq-php-test-client" >/dev/null 2>&1 \
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
    # leg1+2：namesrv+broker 双双 permissive TLS（无客户端证书要求）。
    # ⚠ tls.test.mode.enable 默认 true 时服务端**无视 certPath**、现场生成临时自签
    #   证书——leg1（信任一切）能过，leg2 的 CA 校验必挂。必须显式关掉测试模式，
    #   服务端才会加载 server.crt/keyPath。
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
            echo "=== TLS 集群 60s 未就绪（见 $TWORK/ns.log / $TWORK/broker.log） ===" >&2
            exit 2
        fi
    done
    if ! "$PHP_BIN" "$ROOT/php/examples/wait_broker_route.php" "127.0.0.1:$NS_PORT" 60; then
        echo "=== broker 60s 未注册进 nameserver ===" >&2
        exit 2
    fi

    cd "$ROOT/php" || exit 1
    RC=0
    echo "=== [leg 1/3] plain_tls（test-mode：信任自签） ==="
    "$PHP_BIN" "$EXAMPLE" "$NS" plain_tls "$TWORK/ca.crt" 127.0.0.1 || RC=1
    echo "=== [leg 2/3] ca_verify（真校验证书链 + 主机名 SAN） ==="
    "$PHP_BIN" "$EXAMPLE" "$NS" ca_verify "$TWORK/ca.crt" 127.0.0.1 || RC=1

    echo "=== 重启 broker（追加 -Dtls.client.authServer=true → mTLS） ==="
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
    # mTLS：broker（server 侧）要求客户端出示证书 → 正确开关是
    # tls.server.authClient（tls.client.authServer 是「client 认证 server」，
    # 写它会毒到 broker→namesrv 的注册通道，broker 注册直接失败）；
    # trustCertPath 指向签发 client 证书的 CA。
    export JAVA_OPT_EXT="-Xms1g -Xmx2g -Xmn768m -Dtls.enable=true -Dtls.test.mode.enable=false -Dtls.server.certPath=$TWORK/server.crt -Dtls.server.keyPath=$TWORK/server.key -Dtls.server.authClient=true -Dtls.server.trustCertPath=$TWORK/ca.crt"
    nohup "$DIST/bin/mqbroker" -c "$TWORK/broker.conf" > "$TWORK/broker2.log" 2>&1 &
    for i in $(seq 1 60); do
        if port_open x "$BROKER_PORT"; then
            sleep 2
            break
        fi
        sleep 1
        if [ "$i" = "60" ]; then
            echo "=== mTLS broker 60s 未就绪（见 $TWORK/broker2.log） ===" >&2
            exit 2
        fi
    done
    if ! "$PHP_BIN" "$ROOT/php/examples/wait_broker_route.php" "127.0.0.1:$NS_PORT" 60; then
        echo "=== mTLS broker 60s 未注册进 nameserver ===" >&2
        exit 2
    fi
    echo "=== [leg 3/3] mtls（出示客户端证书，双向认证） ==="
    "$PHP_BIN" "$EXAMPLE" "$NS" mtls "$TWORK/ca.crt" 127.0.0.1 "$TWORK/client.crt" "$TWORK/client.key" || RC=1

    echo "=== exit=$RC ==="
    exit $RC
fi

# ---------------------------------------------------------------- pop 分支独占检查
# POP 要求 broker.conf 四件硬配置（timerWheelEnable 等）。已在跑的集群多半是普通
# conf，静默复用必然硬失败在 S1——直接拒绝比错配好。
if [ "$WHICH" = "pop" ] && { port_open x "$NS_PORT" || port_open x "$BROKER_PORT"; }; then
    echo "=== pop 分支需要 POP 专用集群（timerWheelEnable=true 等四件配置），" >&2
    echo "    但 ${NS_PORT}/${BROKER_PORT} 已有集群在跑。请先停掉它再跑 pop。 ===" >&2
    exit 2
fi
if [ "$WHICH" = "pop" ]; then
    WORK=/tmp/rmq_php_live_pop
fi

if ! port_open x "$NS_PORT" || ! port_open x "$BROKER_PORT"; then
    echo "=== 集群未监听（${NS_PORT}/${BROKER_PORT}），拉起本地测试集群 ==="
    if [ ! -d "$DIST" ]; then
        echo "找不到 RocketMQ 分发目录: ${DIST}（用 ROCKETMQ_DIST 覆盖）" >&2
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

# POP 硬前提（broker.conf 需要这三项才能跑 POP 链路）
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
    # 端口通了 ≠ 路由已注册：TBW102 没进 nameserver 之前一切 createTopic/send 都会
    # 死于 "No route info of default topic TBW102"，所以必须再等 broker 注册。
    if ! "$PHP_BIN" "$ROOT/php/examples/wait_broker_route.php" "127.0.0.1:$NS_PORT" 60; then
        echo "=== broker 60s 未注册进 nameserver，放弃 ===" >&2
        exit 2
    fi
fi

echo "=== php live: $EXAMPLE (ns=$NS) ==="
cd "$ROOT/php" || exit 1
if [ "$WHICH" = "redelivery" ] || [ "$WHICH" = "pop" ] || [ "$WHICH" = "pull" ]; then
    "$PHP_BIN" "$EXAMPLE" "$NS" "$LEGS"
elif [ "$WHICH" = "request_reply" ]; then
    # 双进程编排：responder（consumer + producer.reply）后台起，等 READY 后跑
    # requester（producer.request）。responder 收满 expect 条自行退出。
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
        echo "=== responder 40s 未就绪（见 $WORK/rr_responder.log） ===" >&2
        kill "$RESPONDER_PID" 2>/dev/null || true
        cat "$WORK/rr_responder.log"
        exit 2
    fi
    "$PHP_BIN" "$EXAMPLE" requester "$NS" "$RR_TOPIC" "$RR_EXPECT"
    RC=$?
    wait "$RESPONDER_PID" || true
    echo "--- responder 输出（$WORK/rr_responder.log） ---"
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
