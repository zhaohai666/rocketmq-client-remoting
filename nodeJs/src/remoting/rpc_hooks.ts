// -*- coding: utf-8 -*-
// Infrastructure RPC hooks + the Java registration order.
//
// Port of org.apache.rocketmq.client.rpchook.NamespaceRpcHook and
// org.apache.rocketmq.remoting.rpchook.StreamTypeRPCHook. The registration
// ORDER is load-bearing — it mirrors MQClientAPIImpl's constructor
// (MQClientAPIImpl.java:329-335):
//
//   remotingClient.registerRPCHook(new NamespaceRpcHook(clientConfig));
//   // Inject stream rpc hook first to make reserve field signature
//   if (clientConfig.isEnableStreamRequestType()) {
//       remotingClient.registerRPCHook(new StreamTypeRPCHook());
//   }
//   remotingClient.registerRPCHook(rpcHook);            // the USER's (e.g. ACL)
//   remotingClient.registerRPCHook(new DynamicalExtFieldRPCHook());
//
// Hooks run in registration order and the ACL signature covers every extField
// present when the ACL hook runs, so the namespace (and ReqT) fields MUST be
// written BEFORE it — otherwise the broker sees unsigned fields and rejects
// the request ("reserve field signature"). See go/remoting/rpchooks.go for the
// same rationale in the Go port.
import { MixAll } from '../common/mixAll.ts';
import { RemotingCommand } from './remotingCommand.ts';

// The contract RemotingClient's hook invocation relies on (duck-typed there,
// typed here; AclRPCHook already satisfies it).
export interface RpcHook {
  doBeforeRequest(addr: string, cmd: RemotingCommand): void;
  doAfterResponse(addr: string, request: RemotingCommand | null, response: RemotingCommand | null): void;
}

// NamespaceRpcHook mirrors org.apache.rocketmq.client.rpchook.NamespaceRpcHook:
// when a namespace-v2 is configured (Aliyun-style serverless instance id),
// every request carries
//
//	nsd = "true", ns = <namespaceV2>
//
// This is the SERVER-side namespace mechanism (the broker resolves the real
// topic from the header) — a different mechanism from the classic `namespace`
// field, which mangles resource names client-side.
export class NamespaceRpcHook implements RpcHook {
  // Java re-reads clientConfig.getNamespaceV2() on EVERY request; a provider
  // function keeps that liveness (a plain string is accepted too and behaves
  // like a snapshot).
  private readonly namespaceV2Provider: () => string | null | undefined;

  constructor(namespaceV2: string | null | undefined | (() => string | null | undefined)) {
    this.namespaceV2Provider = typeof namespaceV2 === 'function' ? namespaceV2 : () => namespaceV2;
  }

  doBeforeRequest(_addr: string, cmd: RemotingCommand): void {
    const ns = this.namespaceV2Provider();
    // Java StringUtils.isNotEmpty guard: with no namespace configured the
    // request's extFields must stay EXACTLY as they were — the hook adds
    // nothing (NamespaceRpcHookTest parity).
    if (ns == null || ns === '') return;
    cmd.addExtField(MixAll.RPC_REQUEST_HEADER_NAMESPACED_FIELD, 'true');
    cmd.addExtField(MixAll.RPC_REQUEST_HEADER_NAMESPACE_FIELD, ns);
  }

  // doAfterResponse is empty, exactly like Java.
  doAfterResponse(_addr: string, _request: RemotingCommand | null, _response: RemotingCommand | null): void {}
}

// StreamTypeRPCHook mirrors org.apache.rocketmq.remoting.rpchook.StreamTypeRPCHook:
// stamps ReqT = String.valueOf(RequestType.STREAM.getCode()) — the enum code
// is 0 — so the field lands INSIDE the ACL signature (Java registers this hook
// before the user hook).
export class StreamTypeRPCHook implements RpcHook {
  doBeforeRequest(_addr: string, cmd: RemotingCommand): void {
    cmd.addExtField(MixAll.REQ_T, '0');
  }

  doAfterResponse(_addr: string, _request: RemotingCommand | null, _response: RemotingCommand | null): void {}
}

export interface RpcHookChainOptions {
  // string snapshot or live getter (Java ClientConfig#namespaceV2).
  namespaceV2?: string | null | (() => string | null | undefined);
  // Java ClientConfig#enableStreamRequestType.
  enableStreamRequestType?: boolean;
  // The user's hook — e.g. the AclRPCHook signer.
  userHook?: RpcHook | null;
}

// buildRpcHooks returns the chain in Java's order: Namespace -> Stream (only
// when enabled) -> user hook. The Namespace hook is ALWAYS installed (like
// Java), it is a no-op while namespaceV2 is empty.
export function buildRpcHooks(opts: RpcHookChainOptions = {}): RpcHook[] {
  const hooks: RpcHook[] = [];
  hooks.push(new NamespaceRpcHook(opts.namespaceV2 != null ? opts.namespaceV2 : null));
  if (opts.enableStreamRequestType === true) hooks.push(new StreamTypeRPCHook());
  if (opts.userHook != null) hooks.push(opts.userHook);
  return hooks;
}

// registerRpcHooks installs the Java-ordered chain on a RemotingClient and
// returns it (tests assert the order).
export function registerRpcHooks(
  rc: { registerRpcHook(hook: any): void },
  opts: RpcHookChainOptions = {},
): RpcHook[] {
  const hooks = buildRpcHooks(opts);
  for (const hook of hooks) rc.registerRpcHook(hook);
  return hooks;
}

export default { NamespaceRpcHook, StreamTypeRPCHook, buildRpcHooks, registerRpcHooks };
