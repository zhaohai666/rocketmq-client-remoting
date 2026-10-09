# -*- coding: utf-8 -*-
"""pytest 公共 fixture：把 python/ 根目录加入 sys.path，
保证 ``import client`` / ``import common`` / ``import remoting`` 可用。

同时把客户端日志钉到临时目录：rocketmq_logging 默认按**当前工作目录**落盘
（见 python/rocketmq_logging.py 的取舍说明），不钉住的话跑一次测试就会在
python/logs/rocketmqlogs 里长出日志文件。
"""
import os
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

os.environ.setdefault(
    "ROCKETMQ_CLIENT_LOG_DIR",
    os.path.join(tempfile.gettempdir(), "rmq_py_test_logs"),
)

