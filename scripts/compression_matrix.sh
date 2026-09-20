#!/bin/bash
# 跨语言压缩矩阵：A 端发（自动压缩 8192B 确定性载荷）→ B 端收并核对 CRC32。
#
# 为什么要有这个脚本：各语言单测只能证明「自己压自己解」，证明不了
# **A 端压出来的字节 B 端能不能解开** —— 而压缩解错的失败模式是静默数据损坏
# （拿到压缩字节当正文，不报错），只有真机 + 真跨客户端才暴露得出来。
#
# 四端（python / cpp / dotnet / rust）载荷由各自本地按同一配方重建（同一行文本重复后
# 截断），所以判定只看接收端打印的 `match=1`，**不要**比两边打印的 CRC 数字
# （Java 口径的 UtilAll.crc32 会 & 0x7FFFFFFF，本仓库四端都用标准 CRC-32）。
#
# 用法：scripts/compression_matrix.sh [codec]      codec = zlib（默认）| lz4 | zstd
# 只有**发送端**关心 codec；接收端按 sysFlag 的类型位自动解压，所以「B 能解 A 压的」
# 正是矩阵要证明的部分。
#
# 前置条件（先构建好四端，本脚本不触发构建）：
#   python/.venv 已装 lz4（zstandard 可选，缺了 zstd 的 py 两只会打 SKIP）
#   cpp/build/examples/rmq_compression_live
#   dotnet 示例已 build（用 dotnet run --no-build）
#   rust example: cargo build --example live_compression_matrix
# 以及一个 autoCreateTopicEnable=true 的 nameServer(9876)+broker(10911)。
#
# ⚠ python 端没有 zstandard 时**明确抛错**而不是静默透传（message_decoder._zstd），
# 所以 zstd 矩阵里 python 必然失败 —— 那不是互通性问题，脚本直接 SKIP 掉，
# zstd 的跨语言互通由 cpp / dotnet / rust 三端互测覆盖。
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
cd "$ROOT/dotnet" || exit 1
exec dotnet run --no-build --project examples/RocketMQ.Examples -- compression-live send $NS "\$1" "\$2" $SIZE $CODEC
EOF
cat > "$BIN/net_recv" <<EOF
#!/bin/bash
cd "$ROOT/dotnet" || exit 1
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
chmod +x "$BIN"/*

fail=0

run_pair() { # label  sender  receiver
  local topic="XCompress_${CODEC}_${STAMP}_$1" group="XCompressG_${STAMP}_$1"
  echo "----- $1 [$CODEC]: $2 send -> $3 recv"
  if [[ $CODEC == zstd && ( $2 == py_* || $3 == py_* ) ]]; then
    echo "  RESULT=SKIPPED (python venv has no zstandard; see message_decoder._zstd)"
    return
  fi
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
echo "MATRIX_DONE codec=$CODEC stamp=$STAMP fail=$fail"
exit $fail
