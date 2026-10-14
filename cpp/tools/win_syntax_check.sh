#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Semantic check of Windows code paths (alternative validation when no Windows toolchain is present).
#
# This host cannot and does not run Windows binaries (no mingw toolchain, no wine). However, the
# **real mingw-w64 headers** serve as authoritative Windows API declarations; combined with
# the host SDK C/C++ standard library headers they enable `-fsyntax-only` on the source. This is not
# "able to execute" but "able to compile under Windows semantics" — enough to catch winsock API
# signature misuse, type errors, missing includes, and most porting issues (min/max, isnan/isinf macro conflicts).
#
# Known environment differences (**not** code problems, already smoothed out by a shim):
#   * With mingw headers + Apple libc++, libc++'s win32 locale requires MSVC's
#     `_*_l` functions → declarations supplied by tools/wincompat_shim.h.
#
# Usage:
#   bash tools/win_syntax_check.sh                    # check all of src/
#   bash tools/win_syntax_check.sh src/client/admin.cpp
# ---------------------------------------------------------------------------
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CPP_ROOT="$(dirname "$HERE")"
MINGW_DEST="${RMQ_MINGW_HDR:-/tmp/rmq_mingw_hdr}"

bash "$HERE/win_prepare_headers.sh" "$MINGW_DEST" || exit 1

HDR="$MINGW_DEST/mingw-w64-headers/include"
CRT="$MINGW_DEST/mingw-w64-headers/crt"
CLANG_RES="$(clang -print-resource-dir)/include"
CXX_V1="${RMQ_LIBCXX_DIR:-/Library/Developer/CommandLineTools/usr/include/c++/v1}"
SDK="$(xcrun --show-sdk-path)"

if [ ! -d "$CXX_V1" ]; then
    echo "libc++ header directory not found: $CXX_V1 (override with RMQ_LIBCXX_DIR)" >&2
    exit 1
fi

# Include order: project headers -> mingw Windows headers -> C++ standard library -> host C library/SDK.
INCS=(-I"$CPP_ROOT/include" -I"$CPP_ROOT/src" -I"$HDR" -I"$CRT"
      -isystem "$CXX_V1" -isystem "$CLANG_RES" -isystem "$SDK/usr/include")

# Do not add -fms-compatibility: it makes char16_t/char32_t no longer keywords, which causes libc++
# cstddef to fail to compile (pure environment issue, unrelated to the code under test).
FLAGS=(--target=x86_64-w64-windows-gnu -D_WIN32 -D_WIN64 -DWIN32
       -D_WIN32_WINNT=0x0601 -DWINVER=0x0601
       -Wno-pragma-pack -Wno-ignored-attributes -Wno-unknown-pragmas
       -include "$HERE/wincompat_shim.h"
       -std=c++17 -fsyntax-only)

if [ "$#" -eq 0 ]; then
    # macOS ships bash 3.2, which has no mapfile; collect the list with while-read.
    FILES=()
    while IFS= read -r line; do
        FILES+=("$line")
    done < <(cd "$CPP_ROOT" && find src examples tests -name '*.cpp' | sort)
else
    FILES=("$@")
fi

if [ "${#FILES[@]}" -eq 0 ]; then
    echo "No .cpp files found to check" >&2
    exit 1
fi

total_err=0
fail=0
for f in "${FILES[@]}"; do
    case "$f" in
        /*) path="$f" ;;
        *)  path="$CPP_ROOT/$f" ;;
    esac
    out=$(clang++ "${FLAGS[@]}" "${INCS[@]}" "$path" 2>&1)
    n=$(printf '%s' "$out" | grep -c 'error:')
    if [ "$n" -gt 0 ]; then
        echo "=== FAIL ($n errors): ${f#"$CPP_ROOT"/} ==="
        printf '%s\n' "$out" | grep -E 'error:' | head -12
        total_err=$((total_err + n))
        fail=$((fail + 1))
    else
        echo "=== OK: ${f#"$CPP_ROOT"/} ==="
    fi
done

echo ""
echo "---- windows syntax check: files=${#FILES[@]} failed=$fail errors=$total_err ----"
[ "$fail" -eq 0 ]
