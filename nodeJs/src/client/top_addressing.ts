// -*- coding: utf-8 -*-
// HTTP namesrv addressing: resolves the namesrv address list from an HTTP "ws"
// endpoint.
//
// The domain must be given explicitly via the constructor or the
// ROCKETMQ_NAMESRV_DOMAIN environment variable — there is deliberately NO
// built-in default domain.
import os from 'node:os';

export function clearNewLine(s: string | null): string | null {
  if (s == null) return null;
  return s.replace(/[\r\n]/g, '').trim();
}

export class DefaultTopAddressing {
  wsAddr: string | null;
  unitName: string | null;
  // Change callback: fires when the address list returned by the HTTP endpoint DIFFERS from the one last seen.
  private changeCallBack: ((addrs: string) => string | null) | null = null;
  private lastAddrs: string | null = null;

  constructor(wsAddr: string | null = null, unitName: string | null = null) {
    this.wsAddr = wsAddr;
    this.unitName = unitName;
  }

  registerChangeCallBack(cb: (addrs: string) => string | null): void {
    this.changeCallBack = cb;
  }

  clearChangeCallBack(): void {
    this.changeCallBack = null;
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
      const addrs = clearNewLine(text);
      // Only changed values go through the change callback (onNameServerAddressChange -> updateNameServerAddressList); unchanged answers stay silent.
      if (addrs != null && addrs !== '' && addrs !== this.lastAddrs) {
        const prev = this.lastAddrs;
        this.lastAddrs = addrs;
        if (this.changeCallBack != null) {
          try { this.changeCallBack(addrs); } catch (e) { /* never break polling */ }
        }
      }
      return addrs;
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
