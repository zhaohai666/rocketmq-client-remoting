# -*- coding: utf-8 -*-
"""rocketmq - 命令行入口。

支持子命令:
    python -m rocketmq selfcheck   运行协议编解码回环自检
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
        from .selfcheck import run_selfcheck
        return run_selfcheck()

    print("unknown subcommand: %s" % cmd, file=sys.stderr)
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())