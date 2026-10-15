// -*- coding: utf-8 -*-
// Infrastructure RPC hooks and their registration order.
//
// The registration order is a load-bearing constraint (the classic remoting
// protocol's client-side convention):
//
//   1. Namespace hook (writes the nsd/ns fields)
//   2. StreamType hook (registered only when the stream request type is enabled)
//   3. User hooks (for example ACL signing)
//   4. Dynamic extended-field hooks
//
// Hooks run in registration order, and ACL signing only covers the extFields
// that already exist when it executes, so the namespace (and ReqT) fields must be
// written before the ACL hook — otherwise the broker sees unsigned fields and
// rejects the request ("reserve field signature").
import { MixAll } from '../common/mixAll.ts';
import { RemotingCommand } from './remotingCommand.ts';

// The contract RemotingClient's hook invocation relies on (duck-typed there,
// typed here; AclRPCHook already satisfies it).
export interface RpcHook {
  doBeforeRequest(addr: string, cmd: RemotingCommand): void;
  doAfterResponse(addr: string, request: RemotingCommand | null, response: RemotingCommand | null): void;
}

// NamespaceRpcHook: when namespace-v2 is configured (an Alibaba-Cloud-style
// serverless instance id), every request carries
//
//	nsd = "true", ns = <namespaceV2>
//
// This is the server-side namespace mechanism (the broker parses the real topic
// from the request header) — a different mechanism from the classic `namespace`
// field (which rewrites resource names on the client side).
export class NamespaceRpcHook implements RpcHook {
  // namespaceV2 is re-read on every request; the provider function keeps that
  // freshness (plain strings are also accepted and behave as a snapshot).
  private readonly namespaceV2Provider: () => string | null | undefined;

  constructor(namespaceV2: string | null | undefined | (() => string | null | undefined)) {
    this.namespaceV2Provider = typeof namespaceV2 === 'function' ? namespaceV2 : () => namespaceV2;
  }

  doBeforeRequest(_addr: string, cmd: RemotingCommand): void {
    const ns = this.namespaceV2Provider();
    // Empty-value guard: with no namespace configured the request's extFields must
    // stay untouched — the hook adds nothing.
    if (ns == null || ns === '') return;
    cmd.addExtField(MixAll.RPC_REQUEST_HEADER_NAMESPACED_FIELD, 'true');
    cmd.addExtField(MixAll.RPC_REQUEST_HEADER_NAMESPACE_FIELD, ns);
  }

  // doAfterResponse is a no-op.
  doAfterResponse(_addr: string, _request: RemotingCommand | null, _response: RemotingCommand | null): void {}
}

// StreamTypeRPCHook: stamps ReqT = RequestType.STREAM, whose enum code is 0, so
// the field falls inside the ACL signing coverage (this hook registers before
// the user hooks).
export class StreamTypeRPCHook implements RpcHook {
  doBeforeRequest(_addr: string, cmd: RemotingCommand): void {
    cmd.addExtField(MixAll.REQ_T, '0');
  }

  doAfterResponse(_addr: string, _request: RemotingCommand | null, _response: RemotingCommand | null): void {}
}

export interface RpcHookChainOptions {
  // Either a string snapshot or a live getter (namespaceV2).
  namespaceV2?: string | null | (() => string | null | undefined);
  // Whether the stream request type is enabled.
  enableStreamRequestType?: boolean;
  // The user's own hook — for example an ACL signer.
  userHook?: RpcHook | null;
}

// buildRpcHooks returns the hook chain in the conventional order:
// Namespace -> Stream (only when enabled) -> user hooks.
// The Namespace hook is always installed and is a no-op when namespaceV2 is empty.
export function buildRpcHooks(opts: RpcHookChainOptions = {}): RpcHook[] {
  const hooks: RpcHook[] = [];
  hooks.push(new NamespaceRpcHook(opts.namespaceV2 != null ? opts.namespaceV2 : null));
  if (opts.enableStreamRequestType === true) hooks.push(new StreamTypeRPCHook());
  if (opts.userHook != null) hooks.push(opts.userHook);
  return hooks;
}

// registerRpcHooks installs the hook chain on the RemotingClient in the above
// order and returns it (tests assert that order).
export function registerRpcHooks(
  rc: { registerRpcHook(hook: any): void },
  opts: RpcHookChainOptions = {},
): RpcHook[] {
  const hooks = buildRpcHooks(opts);
  for (const hook of hooks) rc.registerRpcHook(hook);
  return hooks;
}

export default { NamespaceRpcHook, StreamTypeRPCHook, buildRpcHooks, registerRpcHooks };
