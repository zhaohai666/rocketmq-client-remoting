# -*- coding: utf-8 -*-
from __future__ import annotations

import logging as _logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

LOGGER_NAME = "rocketmq.client"

# Environment variable configuration: ROCKETMQ_CLIENT_LOG_* control the directory / file name / level /
# backup count / whether to write to stdout
_DEFAULT_LOG_DIR = os.path.join(os.getcwd(), "logs", "rocketmqlogs")
_LOG_DIR = os.environ.get("ROCKETMQ_CLIENT_LOG_DIR") or _DEFAULT_LOG_DIR
_LOG_FILE = os.environ.get("ROCKETMQ_CLIENT_LOG_FILE") or "rocketmq_py_client.log"
_LOG_LEVEL_NAME = (os.environ.get("ROCKETMQ_CLIENT_LOG_LEVEL") or "INFO").upper()
_LOG_MAX_INDEX = int(os.environ.get("ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX") or "10")
_LOG_USE_STDOUT = (os.environ.get("ROCKETMQ_CLIENT_LOG_USE_STDOUT") or "true").lower() != "false"

_LEVEL_ALIASES = {"WARN": "WARNING", "TRACE": "DEBUG"}

FORMAT = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"


def _level_from_name(raw: str) -> int:
    pass
    name = _LEVEL_ALIASES.get(raw, raw)
    level = getattr(_logging, name, None)
    return level if isinstance(level, int) else _logging.INFO


def _build_handlers():
    """Build the client's own list of handlers (file + optional console)."""
    handlers = []
    # File handler: rolls over daily, file name rocketmq_py_client.log.YYYY-MM-DD, keeps at most N copies
    try:
        os.makedirs(_LOG_DIR, exist_ok=True)
        fh = TimedRotatingFileHandler(
            os.path.join(_LOG_DIR, _LOG_FILE),
            when="midnight",
            backupCount=_LOG_MAX_INDEX,
            encoding="utf-8",
        )
        fh.suffix = "%Y-%m-%d"  # the backup name looks like rocketmq_py_client.log.YYYY-MM-DD
        fh.setFormatter(_logging.Formatter(FORMAT))
        handlers.append(fh)
    except Exception as e:  # noqa: BLE001
        # Degrade gracefully when the file cannot be written: only print a warning to stderr and do not
        # block the client
        sys.stderr.write("[rocketmq] client file log disabled: %s\n" % e)
    if _LOG_USE_STDOUT:
        sh = _logging.StreamHandler()
        sh.setFormatter(_logging.Formatter(FORMAT))
        handlers.append(sh)
    return handlers


def get_logger(name: str = LOGGER_NAME) -> _logging.Logger:
    logger = _logging.getLogger(name)
    # The host program has already configured logging (root already has a handler): reuse its
    # configuration instead of adding handlers on our own initiative
    if logger.handlers or (logger.parent is not None and logger.parent.handlers):
        return logger
    for h in _build_handlers():
        logger.addHandler(h)
    logger.setLevel(_level_from_name(_LOG_LEVEL_NAME))
    logger.propagate = False  # we bring our own console+file handler; propagating up to root would duplicate output
    return logger


class AbstractLogger:
    pass

    def debug(self, msg, *args, **kwargs):
        get_logger().debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        get_logger().info(msg, *args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        get_logger().warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        get_logger().error(msg, *args, **kwargs)


__all__ = ["get_logger", "AbstractLogger"]
