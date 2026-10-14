#pragma once
// ---------------------------------------------------------------------------
// Used only by win_syntax_check.sh (force-included via -include).
//
// Background: the host Apple libc++ win32 locale support
// (__support/win32/locale_win32.h) assumes the CRT provides underscore-prefixed
// `_islower_l` / `_isupper_l` / `_isdigit_l` / `_isxdigit_l` the way MSVC does. mingw-w64 headers
// **do not** carry these declarations, so libc++'s <locale> fails to compile.
//
// This is an artifact of the "mingw headers + MSVC-targeted libc++" combination and is
// **unrelated to the code under check**: a real mingw toolchain uses mingw's own libstdc++,
// so the problem does not arise there. Adding the declarations only lets our code pass the full check.
//
// Include <locale.h> first to get mingw's `_locale_t` definition.
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
