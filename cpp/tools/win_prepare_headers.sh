#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 准备 mingw-w64 头文件树（供 win_syntax_check.sh 使用）。
#
# 为什么需要这个：本机没有、也无法安装 Windows 交叉工具链（brew 的 mingw-w64
# 只有源码包，构建需要 sandbox-exec，而当前环境禁止 sandbox_apply）。但我们仍能
# 拿到**真实的 Windows 头文件**——mingw-w64 源码里的 mingw-w64-headers 就是
# winsock2.h / windows.h 的权威声明。有了它就能对 Windows 代码路径做真正的
# 语义检查（`-fsyntax-only`），这是我们能做的最强验证。
#
# 头文件树是"半成品"：`_mingw.h` 是 configure 生成的，需要把 @...@ 占位符替换掉。
# 只有两个占位符：DEFAULT_MSVCRT_VERSION / DEFAULT_WIN32_WINNT。
#
# 用法：bash win_prepare_headers.sh [目标目录]     默认 /tmp/rmq_mingw_hdr
# ---------------------------------------------------------------------------
set -euo pipefail

DEST="${1:-/tmp/rmq_mingw_hdr}"
HDR="$DEST/mingw-w64-headers/include"
CRT="$DEST/mingw-w64-headers/crt"

if [ -f "$HDR/winsock2.h" ] && [ -f "$CRT/_mingw.h" ]; then
    echo "头文件树已就绪：$DEST"
    exit 0
fi

# 找一个 mingw-w64 源码包：优先 Homebrew 下载缓存，其次再联网取。
TARBALL=""
for cand in "$HOME"/Library/Caches/Homebrew/downloads/*mingw-w64*.tar.bz2; do
    [ -f "$cand" ] && TARBALL="$cand" && break
done

if [ -z "$TARBALL" ]; then
    echo "本地缓存里没有 mingw-w64 源码包。"
    echo "请先执行： brew fetch mingw-w64"
    echo "（brew fetch 只下载不编译，因此不需要 sandbox）"
    exit 1
fi

echo "使用源码包：$TARBALL"
rm -rf "$DEST"
mkdir -p "$DEST"
tar xjf "$TARBALL" -C "$DEST" --strip-components=1 mingw-w64-v*/mingw-w64-headers

if [ ! -f "$HDR/winsock2.h" ]; then
    echo "解包后找不到 $HDR/winsock2.h —— 包结构可能变了。" >&2
    exit 1
fi

# 生成 configure 才会产出的 _mingw.h
if [ -f "$CRT/_mingw.h.in" ]; then
    sed -e 's/@DEFAULT_MSVCRT_VERSION@/0x0601/' \
        -e 's/@DEFAULT_WIN32_WINNT@/0x0601/' \
        "$CRT/_mingw.h.in" > "$CRT/_mingw.h"
fi

# 注意：`grep -c` 在 0 命中时会返回非零，配合 set -e/pipefail 会直接终止脚本，
# 所以这里必须 `|| true` 兜住。
left=$(grep -c '@[A-Za-z_0-9]*@' "$CRT/_mingw.h" 2>/dev/null | head -1 || true)
[ -z "$left" ] && left=0
if [ "$left" != "0" ]; then
    echo "警告：_mingw.h 里仍有 $left 个未替换的占位符，可能会影响检查结果。" >&2
fi

echo "头文件树已生成："
echo "  include: $HDR"
echo "  crt    : $CRT"
