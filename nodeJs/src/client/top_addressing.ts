// -*- coding: utf-8 -*-
// HTTP namesrv addressing (org.apache.rocketmq.common.namesrv.TopAddressing).
// Faithful port of python/client/top_addressing.py.
//
// Resolves the namesrv address list from an HTTP "ws" endpoint. The domain must be supplied
// explicitly via the constructor or the ROCKETMQ_NAMESRV_DOMAIN env var (there is intentionally
// NO built-in default domain, mirroring the Python port).
import os from 'node:os';

export function clearNewLine(s: string | null): string | null {
  if (s == null) return null;
  return s.replace(/[\r\n]/g, '').trim();
}

export class DefaultTopAddressing {
  wsAddr: string | null;
  unitName: string | null;

  constructor(wsAddr: string | null = null, unitName: string | null = null) {
    this.wsAddr = wsAddr;
    this.unitName = unitName;
  }

  // Domain used to build the ws url. Falls back to the ROCKETMQ_NAMESRV_DOMAIN env var.
  getWsAddr(domain: string | null, subgroup: string = 'nsaddr'): string | null {
    if (!domain) return null;
    let wsUrl = `http://${domain}:8080/rocketmq/${subgroup}`;
    if (this.unitName) wsUrl += `?unit=${this.unitName}`;
    return wsUrl;
  }

  buildUrl(subgroup: string = 'nsaddr'): string | null {
    const domain = this.wsAddr || process.env['ROCKETMQ_NAMESRV_DOMAIN'] || '';
    if (!domain) return null;
    let wsUrl = `http://${domain}/rocketmq/${subgroup}`;
    if (this.unitName) wsUrl += `?unit=${this.unitName}`;
    return wsUrl;
  }

  isConfigured(): boolean {
    return !!process.env['ROCKETMQ_NAMESRV_DOMAIN'];
  }

  async fetchNsAddr(subgroup: string = 'nsaddr'): Promise<string | null> {
    const url = this.buildUrl(subgroup);
    if (!url) return null;
    try {
      const resp = await fetch(url);
      if (!resp.ok) return null;
      const text = await resp.text();
      return clearNewLine(text);
    } catch (e) {
      return null;
    }
  }

  // Best-effort: fetch and return the namesrv address string (null on any failure).
  async fetchAndApply(subgroup: string = 'nsaddr'): Promise<string | null> {
    return this.fetchNsAddr(subgroup);
  }
}

export default { DefaultTopAddressing, clearNewLine };
