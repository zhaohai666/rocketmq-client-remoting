# -*- coding: utf-8 -*-
"""轻量日志桥接：把 rocketmq 内部日志接到标准 logging。"""
from __future__ import annotations

import logging as _logging

LOGGER_NAME = "rocketmq.client"


def get_logger(name: str = LOGGER_NAME) -> _logging.Logger:
    logger = _logging.getLogger(name)
    if not logger.handlers and not logger.parent.handlers:
        handler = _logging.StreamHandler()
        handler.setFormatter(_logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(_logging.INFO)
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