// -*- coding: utf-8 -*-
// 基础设施 RPC 钩子与注册顺序。
//
// 注册顺序是承重约束（经典 remoting 协议客户端侧约定）：
//
//   1. Namespace 钩子（写入 nsd/ns 字段）
//   2. StreamType 钩子（仅在启用 stream 请求类型时注册）
//   3. 用户钩子（例如 ACL 签名）
//   4. 动态扩展字段钩子
//
// 钩子按注册顺序执行，ACL 签名只覆盖其执行时已存在的 extField，因此
// namespace（和 ReqT）字段必须在 ACL 钩子之前写入——否则 broker 会看到
// 未签名的字段并拒绝请求（"reserve field signature"）。
import { MixAll } from '../common/mixAll.ts';
import { RemotingCommand } from './remotingCommand.ts';

// The contract RemotingClient's hook invocation relies on (duck-typed there,
// typed here; AclRPCHook already satisfies it).
export interface RpcHook {
  doBeforeRequest(addr: string, cmd: RemotingCommand): void;
  doAfterResponse(addr: string, request: RemotingCommand | null, response: RemotingCommand | null): void;
}

// NamespaceRpcHook：当配置了 namespace-v2（阿里云风格的 serverless 实例 id）时，
// 每个请求携带
//
//	nsd = "true", ns = <namespaceV2>
//
// 这是服务端侧的 namespace 机制（broker 从请求头解析真实 topic）——与经典
// `namespace` 字段（在客户端侧改写资源名）是不同的机制。
export class NamespaceRpcHook implements RpcHook {
  // 每次请求都重新读取 namespaceV2；provider 函数保持这种实时性
  // （也接受普通字符串，表现为快照）。
  private readonly namespaceV2Provider: () => string | null | undefined;

  constructor(namespaceV2: string | null | undefined | (() => string | null | undefined)) {
    this.namespaceV2Provider = typeof namespaceV2 === 'function' ? namespaceV2 : () => namespaceV2;
  }

  doBeforeRequest(_addr: string, cmd: RemotingCommand): void {
    const ns = this.namespaceV2Provider();
    // 空值守卫：未配置 namespace 时，请求的 extFields 必须保持原样——
    // 钩子不添加任何字段。
    if (ns == null || ns === '') return;
    cmd.addExtField(MixAll.RPC_REQUEST_HEADER_NAMESPACED_FIELD, 'true');
    cmd.addExtField(MixAll.RPC_REQUEST_HEADER_NAMESPACE_FIELD, ns);
  }

  // doAfterResponse 为空实现。
  doAfterResponse(_addr: string, _request: RemotingCommand | null, _response: RemotingCommand | null): void {}
}

// StreamTypeRPCHook：盖 ReqT = RequestType.STREAM 的枚举码（为 0），
// 所以该字段落在 ACL 签名覆盖范围内（此钩子注册用户钩子之前）。
export class StreamTypeRPCHook implements RpcHook {
  doBeforeRequest(_addr: string, cmd: RemotingCommand): void {
    cmd.addExtField(MixAll.REQ_T, '0');
  }

  doAfterResponse(_addr: string, _request: RemotingCommand | null, _response: RemotingCommand | null): void {}
}

export interface RpcHookChainOptions {
  // 字符串快照或实时 getter（namespaceV2）。
  namespaceV2?: string | null | (() => string | null | undefined);
  // 是否启用 stream 请求类型。
  enableStreamRequestType?: boolean;
  // 用户自己的钩子——例如 ACL 签名器。
  userHook?: RpcHook | null;
}

// buildRpcHooks 按约定顺序返回钩子链：Namespace -> Stream（仅启用时）-> 用户钩子。
// Namespace 钩子总是安装，namespaceV2 为空时它是 no-op。
export function buildRpcHooks(opts: RpcHookChainOptions = {}): RpcHook[] {
  const hooks: RpcHook[] = [];
  hooks.push(new NamespaceRpcHook(opts.namespaceV2 != null ? opts.namespaceV2 : null));
  if (opts.enableStreamRequestType === true) hooks.push(new StreamTypeRPCHook());
  if (opts.userHook != null) hooks.push(opts.userHook);
  return hooks;
}

// registerRpcHooks 按上述顺序把钩子链安装到 RemotingClient 上并返回它
// （测试断言该顺序）。
export function registerRpcHooks(
  rc: { registerRpcHook(hook: any): void },
  opts: RpcHookChainOptions = {},
): RpcHook[] {
  const hooks = buildRpcHooks(opts);
  for (const hook of hooks) rc.registerRpcHook(hook);
  return hooks;
}

export default { NamespaceRpcHook, StreamTypeRPCHook, buildRpcHooks, registerRpcHooks };
