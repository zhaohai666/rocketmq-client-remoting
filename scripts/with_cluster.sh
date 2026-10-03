#!/usr/bin/env bash
# with_cluster.sh — start a real RocketMQ 5.5.0 cluster, run a command against
# it, then shut it down again.
#
#   scripts/with_cluster.sh <command> [args...]
#
# Why this exists: the sandbox reaps child processes as soon as the launching
# command returns, so a cluster started in one Bash call is already gone by the
# next one. Everything therefore has to happen inside a single invocation:
# start nameserver -> wait for 9876 -> start broker -> wait for 10911 and for
# the broker to register -> run the caller command -> kill both.
#
# JAVA_HOME and ROCKETMQ_HOME are exported because the broker refuses to boot
# without ROCKETMQ_HOME, and the launch classpath is built by hand: the
# distribution's runserver.sh/runbroker.sh expand the classpath with shell
# syntax that Git Bash on Windows cannot execute.
set -u

ROCKETMQ_HOME_DEFAULT='D:\zhaohai666-rocketmq\rocketmq\distribution\target\rocketmq-5.5.0\rocketmq-5.5.0'
export ROCKETMQ_HOME="${ROCKETMQ_HOME:-$ROCKETMQ_HOME_DEFAULT}"

HOME_DIR="$(cygpath -u "$ROCKETMQ_HOME")"
NS_PORT=9876
BROKER_PORT=10911

log() { echo "[cluster $(date +%H:%M:%S)] $*"; }

port_up() {
  netstat -ano 2>/dev/null | grep -E ":$1 .*LISTENING" | head -1
}

wait_port() {
  local port="$1" name="$2" tries=${3:-40} i
  for ((i = 0; i < tries; i++)); do
    if port_up "$port" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  log "ERROR: $name did not open port $port within ${tries}s"
  return 1
}
wait_broker_registered() {
  # "boot success" only means the local store is open. Registration with the
  # nameserver happens on a separate thread and lags behind by several seconds,
  # so wait until the nameserver answers EXAMINE_BROKER_CLUSTER_INFO(25) with a
  # non-empty broker table. Polling the nameserver log would be unreliable: it
  # accumulates entries across runs, so an old line would satisfy the check
  # immediately.
  #
  # $1 is the probe command; it must exit 0 once the cluster is usable. The
  # probe's OWN timeout must stay small (-timeout 8): this loop calls it ~40
  # times, so a probe that waits 45s per attempt turns the whole wait into
  # half an hour.
  local probe="$1" i
  for ((i = 0; i < 40; i++)); do
    if bash -c "$probe" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  log "ERROR: broker never became visible to the nameserver"
  bash -c "$probe" 2>&1 | tail -3
  return 1
}

cleanup() {
  log "shutting down"
  for port in "$BROKER_PORT" "$NS_PORT"; do
    pids=$(netstat -ano 2>/dev/null | grep -E ":$port .*LISTENING" | awk '{print $NF}' | sort -u)
    for pid in $pids; do
      taskkill //F //PID "$pid" >/dev/null 2>&1
    done
  done
}
trap cleanup EXIT

cd "$HOME_DIR" || exit 2
CP=$(ls lib/*.jar | tr '\n' ';' | sed 's/;$//')

# The broker takes ~60s to boot here, mostly replaying whatever the shared
# store dir happens to contain from earlier runs. Point it at a throwaway
# store so each run starts from an empty one: that cuts boot to a few seconds
# and guarantees the topic table reflects only what the test itself created.
STORE_DIR="${WITH_CLUSTER_STORE:-/tmp/rmq-verify-store}"
rm -rf "$STORE_DIR"
mkdir -p "$STORE_DIR/commitlog"
BROKER_CONF="$HOME_DIR/conf/broker-verify.conf"
sed -e "s#^storePathRootDir .*#storePathRootDir = $(cygpath -m "$STORE_DIR")#" \
    -e "s#^storePathCommitLog .*#storePathCommitLog = $(cygpath -m "$STORE_DIR/commitlog")#" \
    conf/broker.conf > "$BROKER_CONF"
log "using throwaway store $STORE_DIR"

log "starting namesrv"
# The distribution's own scripts pass the conf with `-c`, a PROGRAM ARGUMENT.
# `-Drocketmq.broker.conf=...` is silently ignored by BrokerStartup/NamesrvStartup
# (no code reads that property), which is exactly how a broker "boots success"
# yet never registers: it never learns namesrvAddr from the conf. Symptom seen
# before this fix: boot line prints the LAN address (192.168.x.x) instead of
# brokerIP1=127.0.0.1, and the nameserver's cluster table stays empty.
# The nameserver gets NO -c: the distribution ships no conf/namesrv.conf, and
# parseCommandlineAndConfigFile dies on the missing file before binding.
nohup java -Xms512m -Xmx512m -Xmn256m \
  -Drocketmq.home="$ROCKETMQ_HOME" \
  -cp "$CP" org.apache.rocketmq.namesrv.NamesrvStartup \
  > logs/namesrv.out 2>&1 &
NS_PID=$!

wait_port "$NS_PORT" nameserver 90 || { tail -20 logs/namesrv.out; exit 3; }
log "namesrv up on $NS_PORT"

log "starting broker"
nohup java -Xms512m -Xmx512m -Xmn256m \
  -Drocketmq.home="$ROCKETMQ_HOME" \
  -cp "$CP" org.apache.rocketmq.broker.BrokerStartup \
  -c "$(cygpath -m "$BROKER_CONF")" \
  > logs/broker.out 2>&1 &
BROKER_PID=$!

# Boot on a clean store is fast, but the first run also pays JVM startup and
# the store's initial replay, so allow a generous window.
wait_port "$BROKER_PORT" broker 150 || { tail -30 logs/broker.out; exit 4; }
# Prefer a prebuilt probe binary when one is present: `go run` recompiles the
# whole client on every cluster start, which dominates the wait.
PROBE="/tmp/wait_cluster.exe"
[ -x "$PROBE" ] || PROBE="cd /d/project/rocketmq-client-remoting/go && go run ./examples/wait_cluster -ns 127.0.0.1:9876"
wait_broker_registered "${PROBE} -ns 127.0.0.1:9876 -timeout 8" \
  || { tail -30 logs/broker.out; exit 5; }
log "broker up on $BROKER_PORT and registered"
log "running: $*"
"$@"
STATUS=$?
log "command exited with $STATUS"
exit $STATUS
