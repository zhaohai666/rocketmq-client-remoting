# -*- coding: utf-8 -*-
"""动态 name server 取址（对应 org.apache.rocketmq.common.namesrv.TopAddressing /
DefaultTopAddressing 与 MixAll.getWSAddr）。

Java 语义（5.5.1 源码逐条核对）：

1. ``MixAll.getWSAddr()``::

       wsDomainName    = System.getProperty("rocketmq.namesrv.domain", "jmenv.tbsite.net")
       wsDomainSubgroup= System.getProperty("rocketmq.namesrv.domain.subgroup", "nsaddr")
       wsAddr = "http://" + domain + ":8080/rocketmq/" + subgroup
       if domain 含 ':'（自带端口）→ 去掉 ":8080"，即 "http://" + domain + "/rocketmq/" + subgroup

2. ``fetchNSAddr(true, 3000)``：HTTP GET（超时 3000ms），unitName 非空白则 URL 追加
   ``-<unitName>?nofix=1``；para 非空则 ``?k=v&...``（末尾 & 去掉）。**code==200** 时对
   响应体做 ``clearNewLine``（trim 后截断到第一个 \\r 或 \\n）作为 NS 地址串；否则返回 null。

3. ``MQClientAPIImpl.fetchNameServerAddr()``：取到的串与上次**不同**才应用
   （``updateNameServerAddressList``，按 ``;`` 切分）；相同则什么都不做。

4. ``MQClientInstance``：**当且仅当**配置里没写 namesrvAddr 时，start() 里先 fetch 一次，
   并调度 ``scheduleAtFixedRate(fetchNameServerAddr, 10s, 2min)`` 周期刷新。

与本仓库的**有意差异**：Java 默认 domain 就是 jmenv.tbsite.net（依赖 /etc/hosts 绑定），
我们若照抄默认值，没配域名的用户启动时会被一个必然解析失败的域名拖 3s 还拿不到地址。
所以这里 domain 必须显式给出（构造参数或环境变量 ``ROCKETMQ_NAMESRV_DOMAIN``），
未配置 = 动态取址关闭，``fetch_ns_addr()`` 直接返回 None —— 行为可预期。
"""
from __future__ import annotations

import os
import urllib.request
from typing import Dict, Optional

from ..logging import get_logger

logger = get_logger(__name__)

DEFAULT_NAMESRV_ADDR_LOOKUP = "jmenv.tbsite.net"   # Java MixAll.DEFAULT_NAMESRV_ADDR_LOOKUP
DEFAULT_DOMAIN_SUBGROUP = "nsaddr"                 # Java 默认 subgroup


def clear_new_line(content: str) -> str:
    """Java DefaultTopAddressing.clearNewLine：trim 后截断到第一个 \\r 或 \\n。"""
    s = content.strip()
    idx = s.find("\r")
    if idx != -1:
        return s[:idx]
    idx = s.find("\n")
    if idx != -1:
        return s[:idx]
    return s


class DefaultTopAddressing:
    """地址服务器取址器（Java DefaultTopAddressing 的等价物，零第三方依赖）。"""

    def __init__(self, ws_addr: Optional[str] = None, unit_name: str = "",
                 para: Optional[Dict[str, str]] = None, timeout_millis: int = 3000,
                 domain: Optional[str] = None, subgroup: Optional[str] = None) -> None:
        self._timeout_millis = int(timeout_millis)
        self._unit_name = unit_name or ""
        self._para = dict(para) if para else None
        if ws_addr:
            self.ws_addr = ws_addr
        else:
            dom = domain or os.environ.get("ROCKETMQ_NAMESRV_DOMAIN", "")
            self.ws_addr = self.get_ws_addr(dom, subgroup) if dom else ""
        # Java 的 nameSrvAddr 缓存：上次成功应用到客户端的地址串
        self.ns_addr: Optional[str] = None

    # ---------------- URL 构造（MixAll.getWSAddr / fetchNSAddr 语义）----------------

    @staticmethod
    def get_ws_addr(domain: str, subgroup: Optional[str] = None) -> str:
        """对应 ``MixAll.getWSAddr``：domain 自带端口（含 ':'）时不追加默认 :8080。"""
        grp = subgroup or DEFAULT_DOMAIN_SUBGROUP
        if ":" in domain:
            return "http://%s/rocketmq/%s" % (domain, grp)
        return "http://%s:8080/rocketmq/%s" % (domain, grp)

    def build_url(self) -> str:
        """对应 ``fetchNSAddr`` 里的 URL 拼装（unitName / para 规则逐条照抄）。"""
        url = self.ws_addr
        if self._para:
            if self._unit_name.strip():
                url = "%s-%s?nofix=1&" % (url, self._unit_name)
            else:
                url = url + "?"
            parts = ["%s=%s" % (k, v) for k, v in self._para.items()]
            url = url + "&".join(parts)
        else:
            if self._unit_name.strip():
                url = "%s-%s?nofix=1" % (url, self._unit_name)
        return url

    @staticmethod
    def is_configured() -> bool:
        """动态取址是否可用（domain 已显式配置）。"""
        return bool(os.environ.get("ROCKETMQ_NAMESRV_DOMAIN", ""))

    # ---------------- 取址 ----------------

    def fetch_ns_addr(self, verbose: bool = True) -> Optional[str]:
        """取一次 NS 地址串；不可用 / 非 200 / 网络失败都返回 None（Java 语义）。"""
        if not self.ws_addr:
            return None
        url = self.build_url()
        try:
            body = self._http_get(url, self._timeout_millis / 1000.0)
            if body is not None:
                return clear_new_line(body)
            if verbose:
                logger.error("fetch nameserver address failed, statusCode!=200 url=%s", url)
        except Exception as e:  # noqa: BLE001 —— Java catch (IOException) 后返回 null
            if verbose:
                logger.debug("fetch name server address exception url=%s: %s", url, e)
        return None

    def fetch_and_apply(self) -> Optional[str]:
        """Java ``MQClientAPIImpl.fetchNameServerAddr``：**地址变化才返回并应用**。"""
        addrs = self.fetch_ns_addr()
        if addrs and addrs.strip():
            if addrs != self.ns_addr:
                logger.info("name server address changed, old=%s, new=%s", self.ns_addr, addrs)
                self.ns_addr = addrs
                return self.ns_addr
        return None

    # ---------------- 传输 ----------------

    def _http_get(self, url: str, timeout_seconds: float) -> Optional[str]:
        """HTTP GET（Java HttpTinyClient.httpGet 的零依赖等价物）。

        返回响应体文本；非 200 抛异常（由 fetch_ns_addr 统一按失败处理）。
        单测会覆写本方法注入 mock，不真正联网。
        """
        req = urllib.request.Request(url, headers={"Accept": "*/*"})
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:  # noqa: S310
            if resp.status != 200:
                raise OSError("http status %d" % resp.status)
            raw = resp.read()
            return raw.decode("utf-8", errors="replace")
