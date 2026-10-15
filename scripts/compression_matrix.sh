#!/bin/bash
# Cross-language compression matrix: side A sends (auto-compressing a deterministic 8192B payload) -> side B receives and verifies CRC32.
#
# Why this script exists: per-language unit tests only prove "compress and decompress on your own";
# they cannot prove **that the bytes compressed by side A can be decompressed by side B** — and
# the failure mode of a wrong decompression is silent data corruption (compressed bytes treated
# as the body, no error raised), which only real machines + genuinely cross-client runs expose.
#
# The payloads on each side (python / cpp / csharp / rust / go / php / nodeJs) are rebuilt locally
# by each client with the same recipe (the same line of text repeated then truncated), so the
# verdict relies solely on the receiver printing `match=1`; **never** compare the CRC numbers the
# two sides print (the server-side stats crc32 is & 0x7FFFFFFF, whereas every client in this repo
# uses standard CRC-32).
#
# Usage: scripts/compression_matrix.sh [codec]      codec = zlib (default) | lz4 | zstd
# Only the **sender** cares about codec; the receiver decompresses automatically based on the
# type bits in sysFlag, so "B can decompress what A compressed" is exactly what the matrix proves.
#
# Prerequisites (build each side first; this script only builds go itself):
#   python/.venv has lz4 installed (zstandard optional; without zstd the two py legs print SKIP)
#   cpp/build/examples/rmq_compression_live
#   csharp examples built (run with dotnet run --no-build)
#   rust example: cargo build --example live_compression_matrix
#   php needs no build (interpreted), but without the `zstd` CLI the zstd leg falls back to pure-PHP Raw/RLE
#   nodeJs needs no build (node --experimental-strip-types runs .ts directly)
#   go is built by this script itself (zero third-party deps, compiles with the standard library)
# Plus a nameServer(9876)+broker(10911) with autoCreateTopicEnable=true.
#
# ⚠ No side's LZ4/ZSTD uses third-party packages (zero third-party deps is a hard constraint):
#   python/cpp/csharp/rust/go/php all hand-write the frame codecs; nodeJs's zlib/zstd comes from
#   node:zlib (Node's own stdlib) and LZ4 is hand-written.
#   So the matrix doubles as the interop acceptance of these hand-written byte formats: any
#   wrong frame header, HC/checksum bit, or block format on one side shows up as **the peer
#   decompressing a bad body or raising a clear error**.
#
# ⚠ When the python side lacks zstandard it **raises an explicit error** instead of silently
# passing through (message_decoder._zstd), so python always fails in the zstd matrix — that is
# not an interop problem, the script just SKIPs it; zstd cross-language interop is covered by
# the six-way cross-testing among cpp / csharp / rust / go / php / nodeJs.
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

command -v timeout >/dev/null || { echo "GNU coreutils timeout is required (macOS: brew install coreutils)" >&2; exit 2; }

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
# rust side: the example binary produced by cargo (arg order send|recv <topic> <group> <size> [namesrv] [codec])
RSBIN="$TARGET/debug/examples/live_compression_matrix"
cat > "$BIN/rs_send" <<EOF
#!/bin/bash
exec $RSBIN send "\$1" "\$2" $SIZE $NS $CODEC
EOF
cat > "$BIN/rs_recv" <<EOF
#!/bin/bash
exec $RSBIN recv "\$1" "\$2" $SIZE $NS
EOF
# go side: built on the fly into a binary (like rust, avoiding a recompile per leg).
GO=${RMQ_GO:-go}
(cd "$ROOT/go" && $GO build -o "$BIN/go_compression" ./examples/live_compression_matrix) \
  || { echo "go build failed (go toolchain required)" >&2; exit 2; }
cat > "$BIN/go_send" <<EOF
#!/bin/bash
exec $BIN/go_compression send "\$1" "\$2" $SIZE $NS $CODEC
EOF
cat > "$BIN/go_recv" <<EOF
#!/bin/bash
exec $BIN/go_compression recv "\$1" "\$2" $SIZE $NS
EOF
# php side: runs directly on PHP 8.1, zero third-party deps (script args send|recv <topic> <group>, size/ns/codec inlined).
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
# nodeJs side: .ts run directly (--experimental-strip-types, zero build). zlib/zstd via node:zlib,
# LZ4 is this project's hand-written frame codec (src/common/compress.ts).
NODE_BIN=${NODE_BIN:-node}
# node:zlib's zstd bindings only exist since Node 23.8; without them this side can only
# decompress its own Raw/RLE frames, so the peer's genuinely compressed blocks count as
# "missing local capability", not an interop failure -> probe once, and SKIP the zstd legs accordingly.
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
  # SKIP when this side lacks the codec capability; do not record it as an interop failure
  # (see each compress implementation's degradation notes for details).
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
  # The Go side has hand-written LZ4 Frame / ZSTD codecs since 2026-10-09 (common/lz4.go,
  # common/zstd.go, zero third-party deps), so Go legs now join the matrix under all codecs.
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
# Add rust (one pair per counterpart, covering all four send/receive directions)
run_pair py2rs py_send rs_recv
run_pair rs2py rs_send py_recv
run_pair cpp2rs cpp_send rs_recv
run_pair rs2cpp rs_send cpp_recv
run_pair net2rs net_send rs_recv
run_pair rs2net rs_send net_recv
run_pair rs2rs rs_send rs_recv
# Go legs (covering every direction: one send and one receive each)
run_pair go2go go_send go_recv
run_pair go2py go_send py_recv
run_pair py2go py_send go_recv
run_pair go2cpp go_send cpp_recv
run_pair cpp2go cpp_send go_recv
run_pair go2net go_send net_recv
run_pair net2go net_send go_recv
run_pair go2rs go_send rs_recv
run_pair rs2go rs_send go_recv
# PHP legs (covering bidirectional interop + php self-loop; lz4/zstd join via the pure CompressionCodec implementation)
run_pair php2php php_send php_recv
run_pair php2py php_send py_recv
run_pair py2php py_send php_recv
run_pair php2cpp php_send cpp_recv
run_pair cpp2php cpp_send php_recv
# nodeJs legs (added 2026-10-09: the matrix previously had no node side at all, so the
# hand-written LZ4 frames were never peer-verified). One each direction, covering node
# interop with all six other sides.
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
