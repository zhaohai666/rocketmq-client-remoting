# -*- coding: utf-8 -*-
"""命令行入口（顶层模块 ``python/__main__.py``）。

支持子命令:
    python selfcheck.py            运行协议编解码回环自检（推荐，在 python/ 目录下执行）
    python __main__.py selfcheck   等价写法

注：``client`` / ``common`` / ``remoting`` 已平铺在 ``python/`` 下作为顶层包，
旧的 ``python -m rocketmq ...`` 形式随嵌套包一起取消。
"""
from __future__ import annotations

import sys

__all__ = ["main"]


def main(argv=None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] in ("-h", "--help"):
        print(__doc__)
        return 0

    cmd = args[0]
    if cmd == "selfcheck":
        from selfcheck import run_selfcheck
        return run_selfcheck()

    print("unknown subcommand: %s" % cmd, file=sys.stderr)
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
