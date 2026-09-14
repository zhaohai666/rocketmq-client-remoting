#pragma once
// ---------------------------------------------------------------------------
// 仅供 win_syntax_check.sh 使用（通过 -include 强制先包含）。
//
// 背景：本机 Apple libc++ 的 win32 locale 支持
// （__support/win32/locale_win32.h）假定 CRT 会像 MSVC 那样提供带下划线前缀的
// `_islower_l` / `_isupper_l` / `_isdigit_l` / `_isxdigit_l`。mingw-w64 的头文件
// **没有**这些声明，于是 libc++ 的 <locale> 无法编译。
//
// 这是"mingw 头 + 面向 MSVC 的 libc++"这一组合的产物，**与被检查的代码无关**：
// 真正的 mingw 工具链用 mingw 自己的 libstdc++，不存在此问题。
// 补上声明只是为了让自有代码能通过完整体检。
//
// 先包含 <locale.h> 以获得 mingw 的 `_locale_t` 定义。
// ---------------------------------------------------------------------------
#include <locale.h>

#ifdef __cplusplus
extern "C" {
#endif

int _islower_l(int c, _locale_t loc);
int _isupper_l(int c, _locale_t loc);
int _isdigit_l(int c, _locale_t loc);
int _isxdigit_l(int c, _locale_t loc);

#ifdef __cplusplus
}
#endif
