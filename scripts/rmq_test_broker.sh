#!/bin/sh
# Start/stop switch for the **master** broker of the local test cluster (RocketMQ 5.5.1):
# used by live verification for fault injection such as "broker restart / master-slave
# switchover / master going down", shared by all four languages' live cases under one
# consistent approach instead of being scattered across individual scripts. **Only touches
# the master (the process with -c $BROKER_CONF)**; the slave on the same machine is not
# managed by it — after stopping the master, the route for broker-a is left with only
# brokerId=1, which is exactly the scenario to verify.
#
#   sh scripts/rmq_test_broker.sh status   # is the master up (a running slave does not count as UP)
#   sh scripts/rmq_test_broker.sh stop     # stop gracefully (SIGTERM -> unregisterBrokerAll), wait until the process is really gone
#   sh scripts/rmq_test_broker.sh start    # start in the background, wait until the log shows boot success
#
# Only touches the disposable test store under /tmp/rmq_rust_live, never deletes data: start
# does not clear the store, so messages and offsets survive a restart (live cases rely exactly
# on this to verify "the same instance can resume sending after a disconnect").
# ⚠ **Never start a second broker on the same store**: the broker's JVM shutdown hook unconditionally
#   sends unregisterBrokerAll to the namesrv and **does not check whether it actually started
#   successfully**. Even if the second process fails to start and exits immediately, merely
#   because of `MessageRocksDBStorage init error: ... LOCK: Resource temporarily
#   unavailable`, it still deregisters broker-a from the namesrv on its way out — afterwards
#   every client fails to get routes for any topic ("Can not find Message Queue for topic")
#   for about 30 seconds, until the real broker's next 30s registration cycle restores it.
#   Live cases across all five clients falling inside that window will **fail en masse**
#   (language-independent; measured 2026-09-30: 7 of 22 legs in the compression matrix failed
#   in a row, two non-Go senders failed together).
#   The `broker_pid` check before start relies on `ps`, which is disabled in some sandboxes ->
#   "already running" gets misjudged as "not running" and another one is started — so never
#   blindly call start inside a sandbox.
#
# Downloads nothing: MQ_HOME points at a locally pre-built distribution directory.
set -eu

MQ_HOME="${MQ_HOME:-/Users/haizai/project/jingsai/roocketmq/zhaohai666-rocketmq/distribution/target/rocketmq-5.5.1/rocketmq-5.5.1}"
RMQ_TEST_HOME="${RMQ_TEST_HOME:-/tmp/rmq_rust_live}"
BROKER_CONF="${BROKER_CONF:-$RMQ_TEST_HOME/broker.conf}"
NAMESRV_ADDR="${NAMESRV_ADDR:-127.0.0.1:9876}"
BROKER_PORT="${BROKER_PORT:-10911}"
# Same heap parameters as the first startup: without -Xms/-Xmx it would inherit runbroker.sh's
# 8g, which this machine cannot afford.
JAVA_OPT_EXT="${JAVA_OPT_EXT:--Xms4g -Xmx4g -Duser.home=$RMQ_TEST_HOME}"

broker_pid() {
    # Only recognize the **master** java process: a slave (brokerId=1) on the same machine runs
    # the same main class, distinguished by its -c config file; match the java binary only
    # (a SIGTERM to the sh wrapper does not reach the child process).
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
    echo "broker process did not exit cleanly" >&2
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
        # Do not use mqshutdown: it grabs **all** BrokerStartup processes by main class name and
        # would kill the slave too (verifications like "master-slave switchover / master down with
        # only the slave left" require stopping only the master). SIGTERM is equivalent to
        # mqshutdown: the broker's shutdown hook performs unregisterBrokerAll, so brokerId=0
        # disappears from the name service immediately while the slave stays registered — exactly
        # the route shape to verify.
        pid="$(broker_pid)"
        [ -n "$pid" ] && kill -15 "$pid"
        wait_down
        echo "broker DOWN"
        ;;
    start)
        [ -n "$(broker_pid)" ] && { echo "broker is already running"; exit 0; }
        # Keep a separate log for each start: wait-up only trusts boot success in this fresh log;
        # success lines in an old log would mark a not-yet-started broker as UP.
        START_LOG="$RMQ_TEST_HOME/broker_restart.log"
        : > "$START_LOG"
        (cd "$MQ_HOME" && ROCKETMQ_HOME="$MQ_HOME" JAVA_OPT_EXT="$JAVA_OPT_EXT" \
            nohup sh bin/mqbroker -c "$BROKER_CONF" -n "$NAMESRV_ADDR" \
            </dev/null >> "$START_LOG" 2>&1 &)
        i=0
        while [ "$i" -lt 240 ]; do
            if grep -q "boot success" "$START_LOG" 2>/dev/null; then
                # The port can open before the log line appears, leaving the next case firing at
                # nothing; confirm the listener once more after the log shows up
                j=0
                while [ "$j" -lt 40 ] && ! nc -G 2 -z 127.0.0.1 "$BROKER_PORT" 2>/dev/null; do
                    sleep 0.25
                    j=$((j + 1))
                done
                echo "broker UP $(grep -m1 'boot success' "$START_LOG")"
                exit 0
            fi
            if [ -z "$(broker_pid)" ] && [ "$i" -gt 20 ]; then
                echo "broker failed to start, see $START_LOG" >&2
                tail -20 "$START_LOG" >&2
                exit 1
            fi
            sleep 0.5
            i=$((i + 1))
        done
        echo "timed out waiting for broker boot success, see $START_LOG" >&2
        tail -20 "$START_LOG" >&2
        exit 1
        ;;
    *)
        echo "Usage: $0 {status|stop|start}" >&2
        exit 2
        ;;
esac
