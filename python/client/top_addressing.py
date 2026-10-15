# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import urllib.request
from typing import Dict, Optional

from rocketmq_logging import get_logger

logger = get_logger(__name__)

DEFAULT_NAMESRV_ADDR_LOOKUP = "jmenv.tbsite.net"
DEFAULT_DOMAIN_SUBGROUP = "nsaddr"


def clear_new_line(content: str) -> str:
    pass
    s = content.strip()
    idx = s.find("\r")
    if idx != -1:
        return s[:idx]
    idx = s.find("\n")
    if idx != -1:
        return s[:idx]
    return s


class DefaultTopAddressing:
    pass

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
        self.ns_addr: Optional[str] = None

    # ---------------- URL construction (MixAll.getWSAddr / fetchNSAddr semantics) ----------------

    @staticmethod
    def get_ws_addr(domain: str, subgroup: Optional[str] = None) -> str:
        """Corresponds to ``MixAll.getWSAddr``: when the domain already carries a port (contains ':'), do not append the default :8080."""
        grp = subgroup or DEFAULT_DOMAIN_SUBGROUP
        if ":" in domain:
            return "http://%s/rocketmq/%s" % (domain, grp)
        return "http://%s:8080/rocketmq/%s" % (domain, grp)

    def build_url(self) -> str:
        """Corresponds to the URL assembly inside ``fetchNSAddr`` (unitName / para rules copied verbatim)."""
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
        """Whether dynamic addressing is available (domain explicitly configured)."""
        return bool(os.environ.get("ROCKETMQ_NAMESRV_DOMAIN", ""))

    # ---------------- addressing ----------------

    def fetch_ns_addr(self, verbose: bool = True) -> Optional[str]:
        pass
        if not self.ws_addr:
            return None
        url = self.build_url()
        try:
            body = self._http_get(url, self._timeout_millis / 1000.0)
            if body is not None:
                return clear_new_line(body)
            if verbose:
                logger.error("fetch nameserver address failed, statusCode!=200 url=%s", url)
        except Exception as e:
            if verbose:
                logger.debug("fetch name server address exception url=%s: %s", url, e)
        return None

    def fetch_and_apply(self) -> Optional[str]:
        pass
        addrs = self.fetch_ns_addr()
        if addrs and addrs.strip():
            if addrs != self.ns_addr:
                logger.info("name server address changed, old=%s, new=%s", self.ns_addr, addrs)
                self.ns_addr = addrs
                return self.ns_addr
        return None

    # ---------------- transport ----------------

    def _http_get(self, url: str, timeout_seconds: float) -> Optional[str]:
        pass
        req = urllib.request.Request(url, headers={"Accept": "*/*"})
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:  # noqa: S310
            if resp.status != 200:
                raise OSError("http status %d" % resp.status)
            raw = resp.read()
            return raw.decode("utf-8", errors="replace")
