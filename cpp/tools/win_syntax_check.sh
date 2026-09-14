#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Windows 代码路径的语义检查（无 Windows 工具链时的替代验证）。
#
# 本机不会、也不能跑 Windows 二进制（没有 mingw 工具链、没有 wine）。但可以把
# **真实的 mingw-w64 头文件**当作 Windows API 的权威声明，配合本机 SDK 的 C/C++
# 标准库头，对源码做 `-fsyntax-only`。这不是"能跑起来"，而是"能按 Windows 语义
# 通过编译"——足以抓出 winsock API 签名误用、类型错误、缺失 include、
# min/max 与 isnan/isinf 这类宏冲突等绝大多数移植问题。
#
# 已知的环境差异（**不是**代码问题，脚本已用 shim 抹平）：
#   * 用 mingw 头 + Apple libc++ 时，libc++ 的 win32 locale 需要 MSVC 的
#     `_*_l` 函数 → 由 tools/wincompat_shim.h 补声明。
#
# 用法：
#   bash tools/win_syntax_check.sh                    # 检查 src/ 全部
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
    echo "找不到 libc++ 头目录：$CXX_V1（可用 RMQ_LIBCXX_DIR 覆盖）" >&2
    exit 1
fi

# 包含顺序：自有头 → mingw 的 Windows 头 → C++ 标准库 → 本机 C 库/SDK。
INCS=(-I"$CPP_ROOT/include" -I"$CPP_ROOT/src" -I"$HDR" -I"$CRT"
      -isystem "$CXX_V1" -isystem "$CLANG_RES" -isystem "$SDK/usr/include")

# 不要加 -fms-compatibility：它会让 char16_t/char32_t 不再是关键字，导致 libc++
# 的 cstddef 编译失败（纯环境问题，与被测代码无关）。
FLAGS=(--target=x86_64-w64-windows-gnu -D_WIN32 -D_WIN64 -DWIN32
       -D_WIN32_WINNT=0x0601 -DWINVER=0x0601
       -Wno-pragma-pack -Wno-ignored-attributes -Wno-unknown-pragmas
       -include "$HERE/wincompat_shim.h"
       -std=c++17 -fsyntax-only)

if [ "$#" -eq 0 ]; then
    # 注意：macOS 自带 bash 3.2，没有 mapfile，用 while-read 收集。
    FILES=()
    while IFS= read -r line; do
        FILES+=("$line")
    done < <(cd "$CPP_ROOT" && find src examples tests -name '*.cpp' | sort)
else
    FILES=("$@")
fi

if [ "${#FILES[@]}" -eq 0 ]; then
    echo "没有找到要检查的 .cpp 文件" >&2
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
