#!/bin/sh
# 本地测试集群（RocketMQ 5.5.1）的 **master** broker 开关：真机验证里 "broker 重启 /
# 主备切换 / master 掉线" 这一类故障注入要用它，四个语言的 live 用例共用同一份口径，
# 别在各自脚本里散落。**只碰 master（-c $BROKER_CONF 的那个进程）**，同机的从节点
# 不归它管 —— 停 master 后路由里 broker-a 只剩 brokerId=1，正是要验的场景。
#
#   sh scripts/rmq_test_broker.sh status   # master 在不在（从节点起了不算 UP）
#   sh scripts/rmq_test_broker.sh stop     # 优雅停掉（SIGTERM → unregisterBrokerAll），等进程真的消失
#   sh scripts/rmq_test_broker.sh start    # 后台拉起，等到日志出现 boot success
#
# 只碰 /tmp/rmq_rust_live 这套一次性测试 store，不会删数据：start 不清 store，
# 重启后消息和位点都还在（live 用例正是靠这一点验"断连后同一实例能恢复发送"）。
# 不下载任何东西：MQ_HOME 指向本地已经构建好的发行包目录。
set -eu

MQ_HOME="${MQ_HOME:-/Users/haizai/project/jingsai/roocketmq/zhaohai666-rocketmq/distribution/target/rocketmq-5.5.1/rocketmq-5.5.1}"
RMQ_TEST_HOME="${RMQ_TEST_HOME:-/tmp/rmq_rust_live}"
BROKER_CONF="${BROKER_CONF:-$RMQ_TEST_HOME/broker.conf}"
NAMESRV_ADDR="${NAMESRV_ADDR:-127.0.0.1:9876}"
BROKER_PORT="${BROKER_PORT:-10911}"
# 与首次拉起时一致的堆参数：不给 -Xms/-Xmx 会沿用 runbroker.sh 的 8g，本机吃不下。
JAVA_OPT_EXT="${JAVA_OPT_EXT:--Xms4g -Xmx4g -Duser.home=$RMQ_TEST_HOME}"

broker_pid() {
    # 只认 **master** 的 java 进程：同机还有从节点（brokerId=1）跑同一个主类，
    # 用 -c 的配置文件区分；只匹配 java 本体（sh 外壳的 SIGTERM 传不到子进程）。
    ps -eo pid=,command= | awk -v conf="$BROKER_CONF" '
        /bin\/java/ && /org\.apache\.rocketmq\.broker\.BrokerStartup/ \
            && index($0, "-c " conf) { print $1; exit }'
}

wait_down() {
    i=0
    while [ "$i" -lt 120 ]; do
        [ -z "$(broker_pid)" ] && return 0
        sleep 0.5
        i=$((i + 1))
    done
    echo "broker 进程没退干净" >&2
    return 1
}

case "${1:-status}" in
    status)
        pid="$(broker_pid)"
        if [ -n "$pid" ]; then
            echo "broker UP pid=$pid port=$BROKER_PORT"
        else
            echo "broker DOWN port=$BROKER_PORT"
            exit 1
        fi
        ;;
    stop)
        # 不用 mqshutdown：它按主类名抓**全部** BrokerStartup 进程，会把从节点一起杀掉
        # （"主备切换 / master 掉线只剩从节点"这类验证必须只停 master）。SIGTERM 与
        # mqshutdown 等价：broker 的 shutdown hook 里做 unregisterBrokerAll，名字服务里
        # brokerId=0 立刻消失、从节点仍注册着 —— 正是要验的那条路由形状。
        pid="$(broker_pid)"
        [ -n "$pid" ] && kill -15 "$pid"
        wait_down
        echo "broker DOWN"
        ;;
    start)
        [ -n "$(broker_pid)" ] && { echo "broker 已经在跑"; exit 0; }
        # 每次拉起单独记一份日志：wait-up 只认这份新日志里的 boot success，
        # 旧日志里的成功行会把还没起来的 broker 判成 UP。
        START_LOG="$RMQ_TEST_HOME/broker_restart.log"
        : > "$START_LOG"
        (cd "$MQ_HOME" && ROCKETMQ_HOME="$MQ_HOME" JAVA_OPT_EXT="$JAVA_OPT_EXT" \
            nohup sh bin/mqbroker -c "$BROKER_CONF" -n "$NAMESRV_ADDR" \
            </dev/null >> "$START_LOG" 2>&1 &)
        i=0
        while [ "$i" -lt 240 ]; do
            if grep -q "boot success" "$START_LOG" 2>/dev/null; then
                # 端口先于日志就绪会让下一个用例打空；日志出现后再确认一次监听
                j=0
                while [ "$j" -lt 40 ] && ! nc -G 2 -z 127.0.0.1 "$BROKER_PORT" 2>/dev/null; do
                    sleep 0.25
                    j=$((j + 1))
                done
                echo "broker UP $(grep -m1 'boot success' "$START_LOG")"
                exit 0
            fi
            if [ -z "$(broker_pid)" ] && [ "$i" -gt 20 ]; then
                echo "broker 启动失败，看 $START_LOG" >&2
                tail -20 "$START_LOG" >&2
                exit 1
            fi
            sleep 0.5
            i=$((i + 1))
        done
        echo "等不到 broker boot success，看 $START_LOG" >&2
        tail -20 "$START_LOG" >&2
        exit 1
        ;;
    *)
        echo "用法: $0 {status|stop|start}" >&2
        exit 2
        ;;
esac
