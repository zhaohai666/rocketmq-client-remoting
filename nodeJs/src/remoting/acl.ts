// -*- coding: utf-8 -*-
// ACL signature (org.apache.rocketmq.acl.common.AclSigner) + RPC hook.
// Signing rule (verified against python/verify_acl_java_parity.py):
//   content = concat(sorted extField values, excluding "Signature") + body
//   Signature = Base64(HMAC-SHA1(secretKey, content))
//   AccessKey / SecurityToken MUST be written into extFields BEFORE signing.
//   The hook runs BEFORE cmd.encode() (sign over the exact bytes that go on the wire).
import crypto from 'node:crypto';
import { Buffer } from 'node:buffer';
import { RemotingCommand } from './remotingCommand.ts';

export function signAcl(accessKey: string, secretKey: string, extFields: Record<string, string>,
  body: Buffer | null, securityToken: string | null = null): string {
  extFields['AccessKey'] = accessKey;
  if (securityToken != null) extFields['SecurityToken'] = securityToken;
  const keys = Object.keys(extFields).sort();
  let content = '';
  for (const k of keys) {
    if (k === 'Signature') continue;
    content += extFields[k];
  }
  const buf = Buffer.concat([Buffer.from(content, 'utf8'), body || Buffer.alloc(0)]);
  const sig = crypto.createHmac('sha1', secretKey).update(buf).digest().toString('base64');
  extFields['Signature'] = sig;
  return sig;
}

export class AclRPCHook {
  accessKey: string;
  secretKey: string;
  securityToken: string | null;
  constructor(accessKey: string, secretKey: string, securityToken: string | null = null) {
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.securityToken = securityToken;
  }
  doBeforeRequest(_addr: string, cmd: RemotingCommand) {
    // Java AclClientRPCHook.doBeforeRequest calls parseRequestContent(request)
    // BEFORE signing, which internally runs makeCustomHeaderToNet(). Our
    // RemotingCommand materializes extFields lazily (only in headerEncode()),
    // so we must do the same here or the signature covers an empty field set.
    cmd.makeCustomHeaderToNet();
    signAcl(this.accessKey, this.secretKey, cmd.extFields, cmd.body, this.securityToken);
  }
  doAfterResponse(_addr: string, _request: RemotingCommand | null, _response: RemotingCommand | null) {
    // no-op
  }
}

export default { signAcl, AclRPCHook };
