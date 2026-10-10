#!/bin/bash
# 跨语言压缩矩阵：A 端发（自动压缩 8192B 确定性载荷）→ B 端收并核对 CRC32。
#
# 为什么要有这个脚本：各语言单测只能证明「自己压自己解」，证明不了
# **A 端压出来的字节 B 端能不能解开** —— 而压缩解错的失败模式是静默数据损坏
# （拿到压缩字节当正文，不报错），只有真机 + 真跨客户端才暴露得出来。
#
# 各端（python / cpp / csharp / rust / go / php / nodeJs）载荷由各自本地按同一配方重建（同一行文本重复后
# 截断），所以判定只看接收端打印的 `match=1`，**不要**比两边打印的 CRC 数字
# （服务端统计口径的 crc32 会 & 0x7FFFFFFF，本仓库各端一律用标准 CRC-32）。
#
# 用法：scripts/compression_matrix.sh [codec]      codec = zlib（默认）| lz4 | zstd
# 只有**发送端**关心 codec；接收端按 sysFlag 的类型位自动解压，所以「B 能解 A 压的」
# 正是矩阵要证明的部分。
#
# 前置条件（先构建好各端，本脚本只自己 build go）：
#   python/.venv 已装 lz4（zstandard 可选，缺了 zstd 的 py 两只会打 SKIP）
#   cpp/build/examples/rmq_compression_live
#   csharp 示例已 build（用 dotnet run --no-build）
#   rust example: cargo build --example live_compression_matrix
#   php 端无需构建（解释执行），但 zstd 若没装 `zstd` CLI 会退回纯 PHP Raw/RLE 腿
#   nodeJs 端无需构建（node --experimental-strip-types 直接跑 .ts）
#   go 端由本脚本自己 build（零第三方依赖，标准库编译即可）
# 以及一个 autoCreateTopicEnable=true 的 nameServer(9876)+broker(10911)。
#
# ⚠ 各端的 LZ4/ZSTD 都不走第三方包（零第三方依赖是硬约束）：python/cpp/csharp/rust/go/php
#   全部手写帧编解码，nodeJs 的 zlib/zstd 取自 node:zlib（Node 自带 stdlib）、LZ4 手写。
#   所以矩阵同时是这套手写字节的互操作验收：任何一端的帧头、HC/校验位或块格式写错，
#   表现都是**对端解出坏正文或明确报错**。
#
# ⚠ python 端没有 zstandard 时**明确抛错**而不是静默透传（message_decoder._zstd），
# 所以 zstd 矩阵里 python 必然失败 —— 那不是互通性问题，脚本直接 SKIP 掉，
# zstd 的跨语言互通由 cpp / csharp / rust / go / php / nodeJs 六端互测覆盖。
set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
NS=${ROCKETMQ_NAMESRV:-127.0.0.1:9876}
PY="$ROOT/python/.venv/bin/python"
SIZE=8192
CODEC=${1:-zlib}
STAMP=$(date +%s)
BIN=$(mktemp -d)
TARGET=${CARGO_TARGET_DIR:-$ROOT/rust/target}
trap 'rm -rf "$BIN"' EXIT

command -v timeout >/dev/null || { echo "需要 GNU coreutils 的 timeout（macOS: brew install coreutils）" >&2; exit 2; }

cat > "$BIN/py_send" <<EOF
#!/bin/bash
cd "$ROOT/python" || exit 1
export ROCKETMQ_NAMESRV=$NS
exec "$PY" verify_compression_live.py send "\$1" "\$2" $SIZE $CODEC
EOF
cat > "$BIN/py_recv" <<EOF
#!/bin/bash
cd "$ROOT/python" || exit 1
export ROCKETMQ_NAMESRV=$NS
exec "$PY" verify_compression_live.py recv "\$1" "\$2" $SIZE
EOF
cat > "$BIN/cpp_send" <<EOF
#!/bin/bash
exec "$ROOT/cpp/build/examples/rmq_compression_live" send $NS "\$1" "\$2" $SIZE $CODEC
EOF
cat > "$BIN/cpp_recv" <<EOF
#!/bin/bash
exec "$ROOT/cpp/build/examples/rmq_compression_live" recv $NS "\$1" "\$2" $SIZE
EOF
cat > "$BIN/net_send" <<EOF
#!/bin/bash
cd "$ROOT/csharp" || exit 1
exec dotnet run --no-build --project examples/RocketMQ.Examples -- compression-live send $NS "\$1" "\$2" $SIZE $CODEC
EOF
cat > "$BIN/net_recv" <<EOF
#!/bin/bash
cd "$ROOT/csharp" || exit 1
exec dotnet run --no-build --project examples/RocketMQ.Examples -- compression-live recv $NS "\$1" "\$2" $SIZE
EOF
# rust 端：cargo 造出来的 example 二进制（参数顺序 send|recv <topic> <group> <size> [namesrv] [codec]）
RSBIN="$TARGET/debug/examples/live_compression_matrix"
cat > "$BIN/rs_send" <<EOF
#!/bin/bash
exec $RSBIN send "\$1" "\$2" $SIZE $NS $CODEC
EOF
cat > "$BIN/rs_recv" <<EOF
#!/bin/bash
exec $RSBIN recv "\$1" "\$2" $SIZE $NS
EOF
# go 端：现场 build 成二进制（和 rust 一样，避免每条腿都重新编译一次）。
GO=${RMQ_GO:-go}
(cd "$ROOT/go" && $GO build -o "$BIN/go_compression" ./examples/live_compression_matrix) \
  || { echo "go 端 build 失败（需要 go 工具链）" >&2; exit 2; }
cat > "$BIN/go_send" <<EOF
#!/bin/bash
exec $BIN/go_compression send "\$1" "\$2" $SIZE $NS $CODEC
EOF
cat > "$BIN/go_recv" <<EOF
#!/bin/bash
exec $BIN/go_compression recv "\$1" "\$2" $SIZE $NS
EOF
# php 端：PHP 8.1 直跑，零第三方依赖（脚本参数 send|recv <topic> <group>，size/ns/codec 内联）。
PHP_BIN=${PHP_BIN:-php}
cat > "$BIN/php_send" <<EOF
#!/bin/bash
cd "$ROOT/php" || exit 1
exec $PHP_BIN examples/live_compression.php send "\$1" "\$2" $SIZE "$NS" $CODEC
EOF
cat > "$BIN/php_recv" <<EOF
#!/bin/bash
cd "$ROOT/php" || exit 1
exec $PHP_BIN examples/live_compression.php recv "\$1" "\$2" $SIZE "$NS"
EOF
# nodeJs 端：.ts 直跑（--experimental-strip-types，零构建）。zlib/zstd 走 node:zlib，
# LZ4 是本项目手写的帧编解码（src/common/compress.ts）。
NODE_BIN=${NODE_BIN:-node}
# node:zlib 的 zstd 绑定是 Node 23.8 才有的；没有时本端只会解自写的 Raw/RLE 帧，
# 对端的真实压缩块属于「本端能力缺失」，不是互通性失败 → 探测一次，zstd 腿据此 SKIP。
NODE_ZSTD=1
$NODE_BIN -e 'const z=require("node:zlib");process.exit(typeof z.zstdCompressSync==="function"&&typeof z.zstdDecompressSync==="function"?0:1)' || NODE_ZSTD=0
cat > "$BIN/node_send" <<EOF
#!/bin/bash
cd "$ROOT/nodeJs" || exit 1
exec $NODE_BIN --experimental-strip-types --no-warnings examples/live_compression.ts send "\$1" "\$2" $SIZE "$NS" $CODEC
EOF
cat > "$BIN/node_recv" <<EOF
#!/bin/bash
cd "$ROOT/nodeJs" || exit 1
exec $NODE_BIN --experimental-strip-types --no-warnings examples/live_compression.ts recv "\$1" "\$2" $SIZE "$NS"
EOF
chmod +x "$BIN"/*

fail=0

run_pair() { # label  sender  receiver
  local topic="XCompress_${CODEC}_${STAMP}_$1" group="XCompressG_${STAMP}_$1"
  echo "----- $1 [$CODEC]: $2 send -> $3 recv"
  # 本端缺 codec 能力时 SKIP，别记成互通失败（详见各自 compress 实现的降级说明）。
  if [[ $CODEC == zstd ]]; then
    case "$2$3" in
      *py_*)
        echo "  RESULT=SKIPPED (python venv has no zstandard; see message_decoder._zstd)"
        return ;;
      *node_*)
        if [[ $NODE_ZSTD == 0 ]]; then
          echo "  RESULT=SKIPPED (node:zlib gained zstd bindings in Node 23.8)"
          return
        fi ;;
    esac
  fi
  # Go 端口 2026-10-09 起带手写 LZ4 Frame / ZSTD 编解码（common/lz4.go、
  # common/zstd.go，零第三方依赖），因此 Go 腿在所有 codec 下正常参与矩阵。
  local sout rout lout line
  sout=$(timeout 180 "$BIN/$2" "$topic" "$group" 2>&1); rout=$?
  echo "$sout" | grep -o "SEND_[A-Z]*.*" | head -1
  if [[ $rout -ne 0 ]]; then
    echo "  RESULT=SEND_FAILED exit=$rout"; echo "$sout" | tail -4; fail=1; return
  fi
  lout=$(timeout 240 "$BIN/$3" "$topic" "$group" 2>&1)
  line=$(echo "$lout" | grep -o "RECV_[A-Z]*.*" | head -1)
  if [[ "$line" == *"match=1"* ]]; then
    echo "  RESULT=PASS $line"
  else
    echo "  RESULT=FAIL ${line:-no-RECV-line}"; echo "$lout" | tail -6; fail=1
  fi
}

run_pair py2cpp py_send cpp_recv
run_pair cpp2py cpp_send py_recv
run_pair py2net py_send net_recv
run_pair net2py net_send py_recv
run_pair cpp2net cpp_send net_recv
run_pair net2cpp net_send cpp_recv
# 加上 rust（各取一对，覆盖四个方向的发送与接收）
run_pair py2rs py_send rs_recv
run_pair rs2py rs_send py_recv
run_pair cpp2rs cpp_send rs_recv
run_pair rs2cpp rs_send cpp_recv
run_pair net2rs net_send rs_recv
run_pair rs2net rs_send net_recv
run_pair rs2rs rs_send rs_recv
# Go 腿（每个方向都覆盖：发送与接收各一条）
run_pair go2go go_send go_recv
run_pair go2py go_send py_recv
run_pair py2go py_send go_recv
run_pair go2cpp go_send cpp_recv
run_pair cpp2go cpp_send go_recv
run_pair go2net go_send net_recv
run_pair net2go net_send go_recv
run_pair go2rs go_send rs_recv
run_pair rs2go rs_send go_recv
# PHP 腿（覆盖双向互通 + php 自环；lz4/zstd 由 CompressionCodec 纯实现参与）
run_pair php2php php_send php_recv
run_pair php2py php_send py_recv
run_pair py2php py_send php_recv
run_pair php2cpp php_send cpp_recv
run_pair cpp2php cpp_send php_recv
# nodeJs 腿（2026-10-09 起补齐：此前矩阵完全没有 node 端，手写 LZ4 帧从未被
# 对端验证过）。双向各一条，覆盖 node 与六个端互通。
run_pair node2node node_send node_recv
run_pair node2py node_send py_recv
run_pair py2node py_send node_recv
run_pair node2cpp node_send cpp_recv
run_pair cpp2node cpp_send node_recv
run_pair node2net node_send net_recv
run_pair net2node net_send node_recv
run_pair node2rs node_send rs_recv
run_pair rs2node rs_send node_recv
run_pair node2go node_send go_recv
run_pair go2node go_send node_recv
run_pair node2php node_send php_recv
run_pair php2node php_send node_recv
echo "MATRIX_DONE codec=$CODEC stamp=$STAMP fail=$fail"
exit $fail
