// -*- coding: utf-8 -*-
// HTTP namesrv addressing: resolves the namesrv address list from an HTTP "ws"
// endpoint.
//
// The domain comes from an explicit setting (the facades' setNamesrvDomain /
// the instance's namesrvDomain option — Java reads the rocketmq.namesrv.domain
// system property) or the ROCKETMQ_NAMESRV_DOMAIN environment variable; there
// is deliberately NO built-in default domain (Java's default jmenv.tbsite.net
// only applies when the property is set, and unset means addressing is off).
// The subgroup follows the same chain: explicit > ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP
// env (Java: rocketmq.namesrv.domain.subgroup) > "nsaddr".
import os from 'node:os';

const NAMESRV_DOMAIN_ENV = 'ROCKETMQ_NAMESRV_DOMAIN';
const NAMESRV_DOMAIN_SUBGROUP_ENV = 'ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP';
const DEFAULT_DOMAIN_SUBGROUP = 'nsaddr';

// Java MixAll.getWSAddr rule: a domain that already carries a port (contains
// ':') keeps it as-is, otherwise the default :8080 is appended.
export function wsAddrForDomain(domain: string, subgroup: string): string {
  return domain.includes(':')
    ? `http://${domain}/rocketmq/${subgroup}`
    : `http://${domain}:8080/rocketmq/${subgroup}`;
}

// Java DefaultTopAddressing.clearNewLine: trim, then truncate at the first \r
// or \n. The address-server body is "addr1;addr2" (possibly line-wrapped).
export function clearNewLine(content: string): string {
  const s = content.trim();
  const cr = s.indexOf('\r');
  if (cr >= 0) return s.slice(0, cr);
  const lf = s.indexOf('\n');
  if (lf >= 0) return s.slice(0, lf);
  return s;
}

// Explicit subgroup wins over the ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP environment
// variable, which wins over the Java default "nsaddr".
export function resolveSubgroup(explicit?: string | null, fallback?: string | null): string {
  if (explicit != null && explicit.trim() !== '') return explicit.trim();
  if (fallback != null && fallback.trim() !== '') return fallback.trim();
  const env = process.env[NAMESRV_DOMAIN_SUBGROUP_ENV];
  if (env != null && env.trim() !== '') return env.trim();
  return DEFAULT_DOMAIN_SUBGROUP;
}

export class DefaultTopAddressing {
  wsAddr: string | null;
  unitName: string | null;
  // Explicit subgroup setting (null = resolve from the env / Java default).
  subgroup: string | null;
  // Change callback: fires when the address list returned by the HTTP endpoint DIFFERS from the one last seen.
  private changeCallBack: ((addrs: string) => string | null) | null = null;
  private lastAddrs: string | null = null;

  constructor(
    wsAddr: string | null = null,
    unitName: string | null = null,
    opts: { domain?: string | null; subgroup?: string | null } = {},
  ) {
    this.unitName = unitName;
    this.subgroup = opts.subgroup != null && opts.subgroup.trim() !== '' ? opts.subgroup.trim() : null;
    if (wsAddr != null && wsAddr !== '') {
      this.wsAddr = wsAddr;
    } else {
      // Explicit domain setting wins over the ROCKETMQ_NAMESRV_DOMAIN env var
      // (Java: the rocketmq.namesrv.domain system property beats nothing else —
      // unset means the addressing branch stays off).
      const domain = opts.domain != null && opts.domain.trim() !== '' ? opts.domain.trim() : process.env[NAMESRV_DOMAIN_ENV] || '';
      this.wsAddr = domain !== '' ? wsAddrForDomain(domain, this.effectiveSubgroup()) : null;
    }
  }

  private effectiveSubgroup(): string {
    return resolveSubgroup(null, this.subgroup);
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
    const grp = subgroup !== 'nsaddr' ? subgroup : this.effectiveSubgroup();
    let wsUrl = `http://${domain}:8080/rocketmq/${grp}`;
    if (this.unitName) wsUrl += `?unit=${this.unitName}`;
    return wsUrl;
  }

  buildUrl(subgroup?: string): string | null {
    const domain = this.wsAddr || process.env[NAMESRV_DOMAIN_ENV] || '';
    if (!domain) return null;
    const grp = resolveSubgroup(subgroup, this.subgroup);
    let wsUrl = domain.includes(':')
      ? `http://${domain}/rocketmq/${grp}`
      : `http://${domain}:8080/rocketmq/${grp}`;
    if (this.unitName) wsUrl += `?unit=${this.unitName}`;
    return wsUrl;
  }

  isConfigured(): boolean {
    return this.wsAddr != null && this.wsAddr !== '' || !!process.env[NAMESRV_DOMAIN_ENV];
  }

  async fetchNsAddr(subgroup?: string): Promise<string | null> {
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
  async fetchAndApply(subgroup?: string): Promise<string | null> {
    return this.fetchNsAddr(subgroup);
  }
}

export default { DefaultTopAddressing, clearNewLine, wsAddrForDomain, resolveSubgroup };
