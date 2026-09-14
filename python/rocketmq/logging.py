# -*- coding: utf-8 -*-
"""轻量日志桥接：把 rocketmq 内部日志接到标准 logging。

默认行为参考 Java RocketMQ 客户端的客户端日志能力：
  - 日志文件落在 ``~/logs/rocketmqlogs/rocketmq_py_client.log``（可通过环境变量改路径/级别/备份数）；
  - 按天滚动，备份文件名为 ``rocketmq_py_client.log.YYYY-MM-DD``，最多保留 ``ROCKETMQ_CLIENT_LOG_MAX_INDEX`` 份；
  - 同时输出到控制台（stderr），方便无配置文件时直接观察。

**文件名为什么不叫 Java 的 ``rocketmq_client.log``**：两者轮转策略不同（Java logback 按大小 64MB
滚动并 gzip，Python 按天重命名），若同一台机器上同时跑 Java 客户端与 Python 客户端并落到同一文件，
会互相插行；更糟的是 Python 在午夜会把 ``rocketmq_client.log`` **改名**，而 JVM 仍持有旧 fd，
后续 Java 日志会写进已 unlink 的 inode 而静默消失。故默认文件名刻意区分。
需要强制与 Java 一致时，设 ``ROCKETMQ_CLIENT_LOG_FILE=rocketmq_client.log`` 即可。

若宿主程序已经自行配置了 Python logging（root logger 已有 handler），则**不再**添加任何 handler，
完全复用宿主配置（与 Java 客户端 ``logUseSlf4j`` 模式一致），避免重复输出。
"""
from __future__ import annotations

import logging as _logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

LOGGER_NAME = "rocketmq.client"

# 环境变量配置（对齐 Java 客户端日志相关系统属性）
_LOG_DIR = os.environ.get("ROCKETMQ_CLIENT_LOG_DIR") or os.path.expanduser("~/logs/rocketmqlogs")
_LOG_FILE = os.environ.get("ROCKETMQ_CLIENT_LOG_FILE") or "rocketmq_py_client.log"
_LOG_LEVEL_NAME = (os.environ.get("ROCKETMQ_CLIENT_LOG_LEVEL") or "INFO").upper()
_LOG_MAX_INDEX = int(os.environ.get("ROCKETMQ_CLIENT_LOG_MAX_INDEX") or "10")
_LOG_USE_STDOUT = (os.environ.get("ROCKETMQ_CLIENT_LOG_USE_STDOUT") or "true").lower() != "false"

FORMAT = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"


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
    logger.setLevel(getattr(_logging, _LOG_LEVEL_NAME, _logging.INFO))
    logger.propagate = False  # 自带 console+file handler，避免再向上抛给 root 造成重复
    return logger


class AbstractLogger:
    """对应 org.apache.rocketmq.logging.AbstractLogger（内部统一日志入口）。"""

    def debug(self, msg, *args, **kwargs):
        get_logger().debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        get_logger().info(msg, *args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        get_logger().warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        get_logger().error(msg, *args, **kwargs)


__all__ = ["get_logger", "AbstractLogger"]
