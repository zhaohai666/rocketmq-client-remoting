# -*- coding: utf-8 -*-
"""客户端日志配置回归守卫。

核心是**文件名不能和 Java 客户端撞车**：Java 的客户端日志是 ``rocketmq_client.log``
（logback 按大小 64MB 滚动 + gzip 到 other_days/），Python 侧是 TimedRotatingFileHandler
按天重命名。同机同时跑两套客户端时若落同一文件：轻则互相插行，重则 Python 在午夜把文件改走，
JVM 仍持有旧 fd，后续 Java 日志写进已 unlink 的 inode 而静默消失。

所以断言默认文件名**不等于** ``rocketmq_client.log``，并断言环境变量仍可覆盖。
"""
from __future__ import annotations

import importlib
from logging.handlers import TimedRotatingFileHandler

import pytest

JAVA_CLIENT_LOG = "rocketmq_client.log"
PY_CLIENT_LOG = "rocketmq_py_client.log"

_KEYS = (
    "ROCKETMQ_CLIENT_LOG_DIR",
    "ROCKETMQ_CLIENT_LOG_FILE",
    "ROCKETMQ_CLIENT_LOG_LEVEL",
    "ROCKETMQ_CLIENT_LOG_USE_STDOUT",
)


def _reload(monkeypatch, env=None):
    """在干净的环境变量下重新加载 rocketmq_logging（其配置是 import 期求值的）。"""
    for k in _KEYS:
        monkeypatch.delenv(k, raising=False)
    for k, v in (env or {}).items():
        monkeypatch.setenv(k, v)
    import rocketmq_logging as rl

    return importlib.reload(rl)


def test_default_filename_does_not_collide_with_java(monkeypatch):
    rl = _reload(monkeypatch)
    assert rl._LOG_FILE == PY_CLIENT_LOG
    assert rl._LOG_FILE != JAVA_CLIENT_LOG


def test_default_filename_env_override(monkeypatch):
    rl = _reload(monkeypatch, {"ROCKETMQ_CLIENT_LOG_FILE": JAVA_CLIENT_LOG})
    # 显式要求与 Java 一致时必须生效（向后兼容：老用户就是靠这个变量改名的）
    assert rl._LOG_FILE == JAVA_CLIENT_LOG


def test_default_dir_is_rocketmqlogs(monkeypatch):
    rl = _reload(monkeypatch)
    assert rl._LOG_DIR.endswith("logs/rocketmqlogs")


def test_default_dir_follows_cwd_not_home(monkeypatch, tmp_path):
    """默认目录是 <cwd>/logs/rocketmqlogs，**刻意不写用户 HOME**。

    Python 客户端常被当脚本嵌进别人的进程里跑，在 $HOME 下悄悄建目录写文件是越界
    副作用（与 php/src/Client/Logger.php 同一取舍）；这里用 chdir 把两者分开验证。
    """
    home = tmp_path / "fakehome"
    home.mkdir()
    cwd = tmp_path / "deploy"
    cwd.mkdir()
    monkeypatch.delenv("ROCKETMQ_CLIENT_LOG_DIR", raising=False)
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))
    monkeypatch.chdir(cwd)
    rl = _reload(monkeypatch)  # _reload 会先删掉 LOG_DIR，正是在测默认推导
    assert rl._LOG_DIR == str(cwd / "logs" / "rocketmqlogs")
    assert not (home / "logs").exists()


def test_level_aliases(monkeypatch):
    """go/nodeJs/php 口径的 WARN/TRACE 也必须认（Python 原生级别名是 WARNING）。"""
    rl = _reload(monkeypatch)
    assert rl._level_from_name("WARN") == rl._logging.WARNING
    assert rl._level_from_name("WARNING") == rl._logging.WARNING
    assert rl._level_from_name("TRACE") == rl._logging.DEBUG
    assert rl._level_from_name("DEBUG") == rl._logging.DEBUG
    assert rl._level_from_name("NOT-A-LEVEL") == rl._logging.INFO


def test_env_level_reaches_logger(monkeypatch, tmp_path):
    """ROCKETMQ_CLIENT_LOG_LEVEL=WARN 要真的作用到 logger 级别上，而不只是存进变量。"""
    rl = _reload(monkeypatch, {
        "ROCKETMQ_CLIENT_LOG_DIR": str(tmp_path),
        "ROCKETMQ_CLIENT_LOG_LEVEL": "WARN",
        "ROCKETMQ_CLIENT_LOG_USE_STDOUT": "false",
    })
    ours = rl._logging.getLogger(rl.LOGGER_NAME)
    saved = list(ours.handlers)
    saved_propagate = ours.propagate
    root_saved = list(rl._logging.getLogger().handlers)
    for h in saved:
        h.close()
    ours.handlers = []
    rl._logging.getLogger().handlers = []
    try:
        lg = rl.get_logger()
        assert lg.level == rl._logging.WARNING
    finally:
        # get_logger() 会挂自己的 handler 并把 propagate 关掉（caplog 依赖向 root 冒泡），
        # 用例必须原样退回，否则污染后面的套件。
        for h in list(lg.handlers):
            h.close()
        ours.handlers = saved
        ours.propagate = saved_propagate
        rl._logging.getLogger().handlers = root_saved


def test_build_handlers_writes_py_client_log(monkeypatch, tmp_path):
    rl = _reload(monkeypatch, {"ROCKETMQ_CLIENT_LOG_DIR": str(tmp_path)})
    handlers = rl._build_handlers()
    try:
        file_handlers = [h for h in handlers if isinstance(h, TimedRotatingFileHandler)]
        assert len(file_handlers) == 1
        fh = file_handlers[0]
        assert fh.baseFilename == str(tmp_path / PY_CLIENT_LOG)
        # 备份名后缀形如 rocketmq_py_client.log.2026-09-14
        assert fh.suffix == "%Y-%m-%d"
        assert fh.backupCount == 10

        fh.emit(rl._logging.LogRecord("rocketmq.client", 20, __file__, 1, "hello", None, None))
        fh.flush()
        assert (tmp_path / PY_CLIENT_LOG).read_text(encoding="utf-8").find("hello") >= 0
    finally:
        for h in handlers:
            h.close()


def test_unwritable_dir_degrades_to_stderr(monkeypatch, tmp_path, capsys):
    """目录不可写时必须优雅降级（仅告警），不能抛异常阻断客户端启动。"""
    blocker = tmp_path / "blocked"
    blocker.write_text("not a directory", encoding="utf-8")
    rl = _reload(monkeypatch, {"ROCKETMQ_CLIENT_LOG_DIR": str(blocker / "sub")})
    handlers = rl._build_handlers()
    try:
        assert not [h for h in handlers if isinstance(h, TimedRotatingFileHandler)]
        assert capsys.readouterr().err.find("client file log disabled") >= 0
    finally:
        for h in handlers:
            h.close()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
