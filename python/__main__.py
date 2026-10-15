# -*- coding: utf-8 -*-
"""The command line entry point (the top-level module ``python/__main__.py``).

Supported subcommands:
    python selfcheck.py            run the protocol codec round-trip selfcheck (recommended, execute it in the python/ directory)
    python __main__.py selfcheck   the equivalent form

Note: ``client`` / ``common`` / ``remoting`` are now flattened under ``python/`` as top-level
packages, and the old ``python -m rocketmq ...`` form was removed together with the nested
packages.
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
