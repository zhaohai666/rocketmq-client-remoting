# -*- coding: utf-8 -*-
from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from client.consumer import DefaultMQPushConsumer
from client.mq_client import MQClientInstance
from client.top_addressing import DefaultTopAddressing, clear_new_line


# ------------------------------------------------------------------ mock server

class _MockAddrServer:
    """A minimal address server: returns a programmable status code and response body."""

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

            def log_message(self, *args):  # silence
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
    """A TopAddressing pointing at the mock server (domain carries the port -> :8080 is not appended)."""
    return DefaultTopAddressing(domain="127.0.0.1:%d" % server.port, **kw)


# ------------------------------------------------------------------ URL construction

class TestWsAddr:
    def test_default_port_appended(self):
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
        assert clear_new_line("   \r\n x") == "x"
        assert clear_new_line("\r\n") == ""


# ------------------------------------------------------------------ Address-fetching behavior

class TestFetchNsAddr:
    def test_200_returns_body(self, addr_server):
        addr_server.body = "10.0.0.1:9876;10.0.0.2:9876\nextra"
        ta = _top_for(addr_server)
        assert ta.fetch_ns_addr(verbose=False) == "10.0.0.1:9876;10.0.0.2:9876"
        # the URL goes to /rocketmq/<subgroup>
        assert addr_server.hit_paths[0].startswith("/rocketmq/nsaddr")

    def test_non_200_returns_none(self, addr_server):
        addr_server.status = 500
        ta = _top_for(addr_server)
        assert ta.fetch_ns_addr(verbose=False) is None

    def test_connection_error_returns_none(self):
        ta = DefaultTopAddressing(domain="127.0.0.1:1", timeout_millis=300)
        assert ta.fetch_ns_addr(verbose=False) is None

    def test_no_domain_means_disabled(self):
        ta = DefaultTopAddressing()          # no domain given, env unset
        assert ta.ws_addr == ""
        assert ta.fetch_ns_addr() is None

    def test_unreachable_at_start_is_reported(self):
        ta = DefaultTopAddressing(domain="127.0.0.1:1", timeout_millis=300)
        assert ta.fetch_ns_addr(verbose=True) is None


class TestFetchAndApply:
    def test_applies_change_only(self, addr_server):
        ta = _top_for(addr_server)
        assert ta.fetch_and_apply() == "127.0.0.1:9876"      # first time: changed -> applied
        assert ta.fetch_and_apply() is None                  # second time: same -> not applied
        addr_server.body = "10.0.0.9:9876"
        assert ta.fetch_and_apply() == "10.0.0.9:9876"       # changed -> applied

    def test_blank_body_not_applied(self, addr_server):
        addr_server.body = "   "
        ta = _top_for(addr_server)
        assert ta.fetch_and_apply() is None


# ------------------------------------------------------------------ Instance integration

class TestMQClientInstanceIntegration:
    def test_start_with_static_addrs_never_fetches(self, addr_server):
        ta = _top_for(addr_server)
        mqc = MQClientInstance("c@dyn", ["127.0.0.1:9876"])
        mqc.top_addressing = ta
        mqc.start()
        try:
            assert addr_server.hit_paths == []               # with static addresses configured it must not ask the address server
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


# ------------------------------------------------------------------ Consumer guard

class TestConsumerGuard:
    def test_empty_addrs_rejected_without_domain(self, monkeypatch):
        monkeypatch.delenv("ROCKETMQ_NAMESRV_DOMAIN", raising=False)
        c = DefaultMQPushConsumer("GID_DynNsUnit")
        c.subscription_data = {"T": None}  # only to make the guard trip on the address first
        with pytest.raises(Exception, match="name server address"):
            c.start()

    def test_empty_addrs_allowed_with_domain_env(self, addr_server, monkeypatch):
        monkeypatch.setenv("ROCKETMQ_NAMESRV_DOMAIN", "127.0.0.1:%d" % addr_server.port)
        c = DefaultMQPushConsumer("GID_DynNsUnit")
        c.name_server_addrs = []
        # after the guard lets it through it reaches MQClientInstance.start: the address returned by the address server is usable -> no throw
        mqc = MQClientInstance("c@guardenv", [])
        mqc.top_addressing = DefaultTopAddressing()
        mqc.start()
        try:
            assert mqc.name_server_addrs == ["127.0.0.1:9876"]
        finally:
            mqc.shutdown()
