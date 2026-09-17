# -*- coding: utf-8 -*-
"""动态 name server（TopAddressing / DefaultTopAddressing）单测 —— 本地 mock HTTP server。

Java 语义锚点（common/namesrv/DefaultTopAddressing.java + MixAll.getWSAddr +
client/impl/MQClientAPIImpl.fetchNameServerAddr，5.5.1 逐条核对）：
  * WS 地址：``http://<domain>:8080/rocketmq/<subgroup>``；domain 自带端口（含 ':'）
    时不追加 :8080；
  * unitName 非空白 → URL 追加 ``-<unitName>?nofix=1``；para 非空 → ``?k=v&...``；
  * HTTP GET（超时 3000ms）code==200 → 响应体 ``clearNewLine``（trim 后截断到第一个
    \\r 或 \\n）作为 NS 地址串；否则 None；
  * ``fetchNameServerAddr``：地址**变化才应用**（按 ';' 切分更新地址表）；
  * ``MQClientInstance``：只在未配置静态地址时，start() fetch 一次 + 10s/2min 周期刷新。

mock HTTP server 用标准库 http.server 起在 127.0.0.1 随机端口，不依赖外网。
"""
from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from rocketmq.client.consumer import DefaultMQPushConsumer
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.client.top_addressing import DefaultTopAddressing, clear_new_line


# ------------------------------------------------------------------ mock server

class _MockAddrServer:
    """极简地址服务器：可编程返回状态码与响应体。"""

    def __init__(self):
        self.status = 200
        self.body = "127.0.0.1:9876"
        self.hit_paths: list = []
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._make_handler())
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def port(self) -> int:
        return self._server.server_address[1]

    def _make_handler(self):
        outer = self

        class H(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                outer.hit_paths.append(self.path)
                body = outer.body.encode("utf-8")
                self.send_response(outer.status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):  # 静音
                pass

        return H

    def stop(self):
        self._server.shutdown()
        self._server.server_close()


@pytest.fixture()
def addr_server():
    s = _MockAddrServer()
    yield s
    s.stop()


def _top_for(server: _MockAddrServer, **kw) -> DefaultTopAddressing:
    """指向 mock server 的 TopAddressing（domain 带端口 → 不追加 :8080）。"""
    return DefaultTopAddressing(domain="127.0.0.1:%d" % server.port, **kw)


# ------------------------------------------------------------------ URL 构造

class TestWsAddr:
    def test_default_port_appended(self):
        # Java MixAll.getWSAddr：domain 无端口 → 补 :8080
        assert DefaultTopAddressing.get_ws_addr("jmenv.tbsite.net") == \
            "http://jmenv.tbsite.net:8080/rocketmq/nsaddr"

    def test_domain_with_port_skips_default(self):
        assert DefaultTopAddressing.get_ws_addr("host:12345") == \
            "http://host:12345/rocketmq/nsaddr"

    def test_custom_subgroup(self):
        assert DefaultTopAddressing.get_ws_addr("h", "mygrp") == \
            "http://h:8080/rocketmq/mygrp"


class TestBuildUrl:
    def test_plain(self):
        ta = DefaultTopAddressing(ws_addr="http://h:8080/rocketmq/nsaddr")
        assert ta.build_url() == "http://h:8080/rocketmq/nsaddr"

    def test_unit_name(self):
        ta = DefaultTopAddressing(ws_addr="http://h:8080/rocketmq/nsaddr", unit_name="unitA")
        assert ta.build_url() == "http://h:8080/rocketmq/nsaddr-unitA?nofix=1"

    def test_para_only(self):
        ta = DefaultTopAddressing(ws_addr="http://h:8080/rocketmq/nsaddr",
                                  para={"k1": "v1", "k2": "v2"})
        url = ta.build_url()
        assert url.startswith("http://h:8080/rocketmq/nsaddr?")
        assert "k1=v1&k2=v2" in url or "k2=v2&k1=v1" in url

    def test_unit_name_with_para(self):
        # Java：unitName + para → "-unit?nofix=1&k=v..."（末尾 & 去掉）
        ta = DefaultTopAddressing(ws_addr="http://h:8080/rocketmq/nsaddr",
                                  unit_name="u", para={"k": "v"})
        assert ta.build_url() == "http://h:8080/rocketmq/nsaddr-u?nofix=1&k=v"

    def test_blank_unit_name_is_ignored(self):
        ta = DefaultTopAddressing(ws_addr="http://h:8080/rocketmq/nsaddr", unit_name="   ")
        assert ta.build_url() == "http://h:8080/rocketmq/nsaddr"


# ------------------------------------------------------------------ clearNewLine

class TestClearNewLine:
    def test_trim_and_cut_at_cr(self):
        assert clear_new_line("  1.2.3.4:9876\r\nrest") == "1.2.3.4:9876"

    def test_cut_at_lf(self):
        assert clear_new_line("a:9876\nb:9877") == "a:9876"

    def test_no_newline(self):
        assert clear_new_line("  a:9876  ") == "a:9876"

    def test_empty(self):
        # Java trim() 会把前导 \r\n 一并去掉，所以这里 trim 后就是 "x"（与 Java 一致）
        assert clear_new_line("   \r\n x") == "x"
        assert clear_new_line("\r\n") == ""


# ------------------------------------------------------------------ 取址行为

class TestFetchNsAddr:
    def test_200_returns_body(self, addr_server):
        addr_server.body = "10.0.0.1:9876;10.0.0.2:9876\nextra"
        ta = _top_for(addr_server)
        assert ta.fetch_ns_addr(verbose=False) == "10.0.0.1:9876;10.0.0.2:9876"
        # URL 走的是 /rocketmq/<subgroup>
        assert addr_server.hit_paths[0].startswith("/rocketmq/nsaddr")

    def test_non_200_returns_none(self, addr_server):
        addr_server.status = 500
        ta = _top_for(addr_server)
        assert ta.fetch_ns_addr(verbose=False) is None

    def test_connection_error_returns_none(self):
        # 没有服务监听的端口 → 连接失败 → None（Java catch IOException）
        ta = DefaultTopAddressing(domain="127.0.0.1:1", timeout_millis=300)
        assert ta.fetch_ns_addr(verbose=False) is None

    def test_no_domain_means_disabled(self):
        ta = DefaultTopAddressing()          # 未给 domain、env 未设
        assert ta.ws_addr == ""
        assert ta.fetch_ns_addr() is None

    def test_unreachable_at_start_is_reported(self):
        ta = DefaultTopAddressing(domain="127.0.0.1:1", timeout_millis=300)
        # verbose=True 走 error 日志但不抛（Java 语义）
        assert ta.fetch_ns_addr(verbose=True) is None


class TestFetchAndApply:
    def test_applies_change_only(self, addr_server):
        ta = _top_for(addr_server)
        assert ta.fetch_and_apply() == "127.0.0.1:9876"      # 第一次：变化 → 应用
        assert ta.fetch_and_apply() is None                  # 第二次：相同 → 不应用
        addr_server.body = "10.0.0.9:9876"
        assert ta.fetch_and_apply() == "10.0.0.9:9876"       # 变了 → 应用

    def test_blank_body_not_applied(self, addr_server):
        addr_server.body = "   "
        ta = _top_for(addr_server)
        assert ta.fetch_and_apply() is None


# ------------------------------------------------------------------ 实例集成

class TestMQClientInstanceIntegration:
    def test_start_with_static_addrs_never_fetches(self, addr_server):
        ta = _top_for(addr_server)
        mqc = MQClientInstance("c@dyn", ["127.0.0.1:9876"])
        mqc.top_addressing = ta
        mqc.start()
        try:
            assert addr_server.hit_paths == []               # 配了静态地址就不该问地址服务器
        finally:
            mqc.shutdown()

    def test_start_with_empty_addrs_fetches_once(self, addr_server):
        ta = _top_for(addr_server)
        mqc = MQClientInstance("c@dyn2", [])
        mqc.top_addressing = ta
        mqc.start()
        try:
            assert mqc.name_server_addrs == ["127.0.0.1:9876"]
            assert len(addr_server.hit_paths) == 1
        finally:
            mqc.shutdown()

    def test_start_fails_when_address_server_returns_none(self, addr_server):
        addr_server.status = 500
        mqc = MQClientInstance("c@dyn3", [])
        mqc.top_addressing = _top_for(addr_server)
        with pytest.raises(Exception):
            mqc.start()

    def test_periodic_refresh_picks_up_change(self, addr_server):
        ta = _top_for(addr_server)
        mqc = MQClientInstance("c@dyn4", [])
        mqc.top_addressing = ta
        mqc.start()
        try:
            addr_server.body = "10.0.0.9:9876"
            mqc.fetch_name_server_addr()
            assert mqc.name_server_addrs == ["10.0.0.9:9876"]
        finally:
            mqc.shutdown()


# ------------------------------------------------------------------ 消费者守卫

class TestConsumerGuard:
    def test_empty_addrs_rejected_without_domain(self, monkeypatch):
        monkeypatch.delenv("ROCKETMQ_NAMESRV_DOMAIN", raising=False)
        c = DefaultMQPushConsumer("GID_DynNsUnit")
        c.subscription_data = {"T": None}  # 只为让守卫先卡在地址上
        with pytest.raises(Exception, match="name server address"):
            c.start()

    def test_empty_addrs_allowed_with_domain_env(self, addr_server, monkeypatch):
        monkeypatch.setenv("ROCKETMQ_NAMESRV_DOMAIN", "127.0.0.1:%d" % addr_server.port)
        c = DefaultMQPushConsumer("GID_DynNsUnit")
        c.name_server_addrs = []
        # 守卫放行后走到 MQClientInstance.start：地址服务器返回的地址可用 → 不抛
        mqc = MQClientInstance("c@guardenv", [])
        mqc.top_addressing = DefaultTopAddressing()
        mqc.start()
        try:
            assert mqc.name_server_addrs == ["127.0.0.1:9876"]
        finally:
            mqc.shutdown()
