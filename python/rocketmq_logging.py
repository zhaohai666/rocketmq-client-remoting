# -*- coding: utf-8 -*-
from __future__ import annotations

import logging as _logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

LOGGER_NAME = "rocketmq.client"

# 环境变量配置：ROCKETMQ_CLIENT_LOG_* 分别控制目录/文件名/级别/备份数/是否打 stdout
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
    """构造客户端自己的 handler 列表（文件 + 可选控制台）。"""
    handlers = []
    # 文件 handler：按天滚动，文件名 rocketmq_py_client.log.YYYY-MM-DD，最多保留 N 份
    try:
        os.makedirs(_LOG_DIR, exist_ok=True)
        fh = TimedRotatingFileHandler(
            os.path.join(_LOG_DIR, _LOG_FILE),
            when="midnight",
            backupCount=_LOG_MAX_INDEX,
            encoding="utf-8",
        )
        fh.suffix = "%Y-%m-%d"  # 备份名形如 rocketmq_py_client.log.YYYY-MM-DD
        fh.setFormatter(_logging.Formatter(FORMAT))
        handlers.append(fh)
    except Exception as e:  # noqa: BLE001
        # 无法写文件时优雅降级：仅打警告到 stderr，不阻断客户端
        sys.stderr.write("[rocketmq] client file log disabled: %s\n" % e)
    if _LOG_USE_STDOUT:
        sh = _logging.StreamHandler()
        sh.setFormatter(_logging.Formatter(FORMAT))
        handlers.append(sh)
    return handlers


def get_logger(name: str = LOGGER_NAME) -> _logging.Logger:
    logger = _logging.getLogger(name)
    # 宿主程序已配置过 logging（root 已有 handler）：复用其配置，不再自作主张加 handler
    if logger.handlers or (logger.parent is not None and logger.parent.handlers):
        return logger
    for h in _build_handlers():
        logger.addHandler(h)
    logger.setLevel(_level_from_name(_LOG_LEVEL_NAME))
    logger.propagate = False  # 自带 console+file handler，避免再向上抛给 root 造成重复
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
