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
    // extFields must be materialized before signing: this implementation's RemotingCommand generates
    // extFields lazily (only inside headerEncode()), so without first calling makeCustomHeaderToNet(),
    // the ACL signature would cover an empty field set.
    cmd.makeCustomHeaderToNet();
    signAcl(this.accessKey, this.secretKey, cmd.extFields, cmd.body, this.securityToken);
  }
  doAfterResponse(_addr: string, _request: RemotingCommand | null, _response: RemotingCommand | null) {
    // no-op
  }
}

export default { signAcl, AclRPCHook };
