# -*- coding: utf-8 -*-
"""Shared pytest fixture: adds the python/ root to sys.path so that
``import client`` / ``import common`` / ``import remoting`` work.

It also pins the client logs to a temporary directory: rocketmq_logging writes to the
**current working directory** by default (see the trade-off notes in
python/rocketmq_logging.py); without pinning them, a single test run would grow log
files under python/logs/rocketmqlogs.
"""
import os
import sys
import tempfile

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

os.environ.setdefault(
    "ROCKETMQ_CLIENT_LOG_DIR",
    os.path.join(tempfile.gettempdir(), "rmq_py_test_logs"),
)


@pytest.fixture(autouse=True)
def _reset_mq_client_manager():
    pass
    from client.mq_client_manager import MQClientManager

    MQClientManager.get_instance().reset()
    yield
    MQClientManager.get_instance().reset()

