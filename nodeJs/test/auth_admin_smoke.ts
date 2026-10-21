// Auth user-management and ACL-management admin RPCs (3001..3010) — offline
// wire test against a recording transport, driven through the real start()
// registration flow.
//
// Contract pinned here:
//   * request code per operation: 3001 create_user, 3002 update_user,
//     3003 delete_user, 3004 get_user, 3005 list_user, 3006 create_acl,
//     3007 update_acl, 3008 delete_acl, 3009 get_acl, 3010 list_acl;
//   * the header set is EXACTLY the fields named per operation (username /
//     filter / subject / subject+resource / subjectFilter+resourceFilter) and a
//     field with no value is absent, never "" or "null";
//   * the JSON body carries only the keys that have a value: the convenience
//     user triple has no userStatus key, the convenience acl triple has one
//     policy with no policyType key and one entry per resource repeating the
//     same actions / sourceIps / decision;
//   * the enum spellings on the wire are case-exact: Super/Normal,
//     enable/disable, Custom/Default, Allow/Deny;
//   * the request goes to the address the caller passed, unchanged — even with
//     the VIP switch on, where a neighbouring broker RPC on the same admin
//     object does move to port-2;
//   * SUCCESS is the only accepted response code; every other code raises the
//     broker error carrying that code and remark;
//   * write operations never read the reply body, getUser/getAcl decode one
//     object (empty body -> null), listUser/listAcl decode an array
//     (empty body -> []).
//
// Run: node --experimental-strip-types test/auth_admin_smoke.ts
import assert from 'node:assert';

import { MQClient } from '../src/client/mq_client.ts';
import { DefaultMQAdminExt } from '../src/client/admin.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { MQBrokerException } from '../src/remoting/exception.ts';
import {
  UserInfo, AclInfo, AclPolicy, AclEntry,
  UserType, UserStatus, PolicyType, Decision,
} from '../src/remoting/admin_body.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}
function checkEq(name: string, actual: any, expected: any) {
  let ok = false;
  try { assert.deepStrictEqual(actual, expected); ok = true; } catch { ok = false; }
  check(name, ok, `got ${JSON.stringify(actual)} want ${JSON.stringify(expected)}`);
}

const NAMESRV = '127.0.0.1:9876';
const BROKER = '127.0.0.1:10911';
const BROKER_VIP = '127.0.0.1:10909';
const USER = 'authUser';
const PASS = 'authPass123';
const SUBJECT = 'User:authUser';
const RESOURCE = 'Topic:OrderDB';
const RESOURCE2 = 'Topic:PayDB';

type Call = {
  code: number; addr: string; ext: Record<string, string>;
  body: string | null; timeout: number;
};
type Reply = { code?: number; remark?: string; body?: string };

// The transport the admin is dialled through: it records the request exactly as
// the hooks leave it (code, address, extFields, body text, timeout) and answers
// from a per-code script.
class FakeTransport {
  calls: Call[] = [];
  hooks: any[] = [];
  replies = new Map<number, Reply>();

  // The shared instance registers its built-in check/reply processors on start;
  // this transport never receives a server push, so the slots are accepted and
  // dropped.
  registerProcessor(_code: number, _handler: any): void {}
  unregisterProcessor(_code: number): void {}
  registerRpcHook(hook: any): void { this.hooks.push(hook); }
  shutdown(): void {}

  script(code: number, reply: Reply): void { this.replies.set(code, reply); }
  call(code: number): Call {
    for (let i = this.calls.length - 1; i >= 0; i--) if (this.calls[i].code === code) return this.calls[i];
    throw new Error(`no request recorded for code ${code}`);
  }

  async invokeSync(addr: string, request: RemotingCommand, timeoutMillis?: number): Promise<RemotingCommand> {
    for (const h of this.hooks) h.doBeforeRequest(addr, request);
    this.calls.push({
      code: request.code,
      addr,
      ext: { ...(request.extFields || {}) },
      body: request.body != null ? request.body.toString('utf8') : null,
      timeout: timeoutMillis as number,
    });
    const reply = this.replies.get(request.code) || {};
    const code = reply.code != null ? reply.code : ResponseCode.SUCCESS;
    const resp = RemotingCommand.createResponseCommand(code, reply.remark != null ? reply.remark : '')!;
    resp.opaque = request.opaque;
    if (reply.body != null) resp.body = Buffer.from(reply.body, 'utf8');
    for (const h of this.hooks) h.doAfterResponse(addr, request, resp);
    return resp;
  }
}

let adminSeq = 0;
// A started admin facade whose shared instance talks to the fake transport, so
// the RPCs travel the same path they take in production: facade -> instance ->
// hooked transport.
function startedAdmin(transport: FakeTransport, vipChannel = false): DefaultMQAdminExt {
  const admin = new DefaultMQAdminExt();
  admin.setNamesrvAddr(NAMESRV);
  admin.setInstanceName(`ADMIN_auth_${process.pid}_${adminSeq++}`);
  if (vipChannel) admin.setVipChannelEnabled(true);
  admin.client = new MQClient(`authPreset_${adminSeq}`, NAMESRV, transport as any);
  admin.start();
  return admin;
}

async function main() {
  // ------------------------------------------ 1. enum spellings on the wire
  {
    checkEq('user type spellings', [UserType.SUPER, UserType.NORMAL], ['Super', 'Normal']);
    checkEq('user status spellings', [UserStatus.ENABLE, UserStatus.DISABLE], ['enable', 'disable']);
    checkEq('policy type spellings', [PolicyType.CUSTOM, PolicyType.DEFAULT], ['Custom', 'Default']);
    checkEq('decision spellings', [Decision.ALLOW, Decision.DENY], ['Allow', 'Deny']);
  }

  // ------------------------------------------------ 2. create/update/delete user
  {
    const t = new FakeTransport();
    const admin = startedAdmin(t);

    // The write replies carry a body the admin must never read: a decode here
    // would throw on this text, so a clean return proves the body is untouched.
    t.script(RequestCode.AUTH_CREATE_USER, { body: 'not-a-json-body{' });
    const created = await admin.createUser(BROKER, USER, PASS, UserType.NORMAL);
    const c = t.call(RequestCode.AUTH_CREATE_USER);
    check('create_user returns without reading the reply body', created === undefined);
    checkEq('create_user request code', c.code, RequestCode.AUTH_CREATE_USER);
    checkEq('create_user header is exactly username', c.ext, { username: USER });
    checkEq('create_user convenience body has NO userStatus key',
      c.body, `{"username":"${USER}","password":"${PASS}","userType":"Normal"}`);
    check('userStatus is absent (not null, not empty) in the convenience body',
      c.body != null && !('userStatus' in (JSON.parse(c.body) as Record<string, any>)), String(c.body));
    checkEq('create_user timeout follows the admin default', c.timeout, 20000);

    await admin.createUser(BROKER, new UserInfo(USER, PASS, UserType.SUPER, UserStatus.DISABLE));
    checkEq('create_user object form serialises every set field',
      t.call(RequestCode.AUTH_CREATE_USER).body,
      `{"username":"${USER}","password":"${PASS}","userType":"Super","userStatus":"disable"}`);

    await admin.updateUser(BROKER, new UserInfo(USER, null, null, UserStatus.ENABLE), null, null, 4321);
    const u = t.call(RequestCode.AUTH_UPDATE_USER);
    checkEq('update_user request code', u.code, RequestCode.AUTH_UPDATE_USER);
    checkEq('update_user header is exactly username', u.ext, { username: USER });
    checkEq('update_user body drops the unset password and userType', u.body,
      `{"username":"${USER}","userStatus":"enable"}`);
    checkEq('update_user honours an explicit timeout', u.timeout, 4321);

    await admin.deleteUser(BROKER, USER);
    const d = t.call(RequestCode.AUTH_DELETE_USER);
    checkEq('delete_user request code', d.code, RequestCode.AUTH_DELETE_USER);
    checkEq('delete_user header is exactly username', d.ext, { username: USER });
    checkEq('delete_user sends no body', d.body, null);

    admin.shutdown();
  }

  // ------------------------------------------------------- 3. get / list user
  {
    const t = new FakeTransport();
    const admin = startedAdmin(t);
    t.script(RequestCode.AUTH_GET_USER, {
      body: `{"username":"${USER}","userType":"Normal","userStatus":"disable"}`,
    });
    const one = await admin.getUser(BROKER, USER);
    const g = t.call(RequestCode.AUTH_GET_USER);
    checkEq('get_user request code', g.code, RequestCode.AUTH_GET_USER);
    checkEq('get_user header is exactly username', g.ext, { username: USER });
    checkEq('get_user sends no body', g.body, null);
    checkEq('get_user decoded single user object', one,
      new UserInfo(USER, null, UserType.NORMAL, UserStatus.DISABLE));

    t.script(RequestCode.AUTH_LIST_USER, {
      body: `[{"username":"u1","userType":"Super","userStatus":"enable"},`
        + `{"username":"u2","password":"p2","userType":"Normal","userStatus":"disable"}]`,
    });
    const many = await admin.listUser(BROKER, 'auth*');
    const l = t.call(RequestCode.AUTH_LIST_USER);
    checkEq('list_user request code', l.code, RequestCode.AUTH_LIST_USER);
    checkEq('list_user header is exactly filter', l.ext, { filter: 'auth*' });
    checkEq('list_user decoded user array', many, [
      new UserInfo('u1', null, UserType.SUPER, UserStatus.ENABLE),
      new UserInfo('u2', 'p2', UserType.NORMAL, UserStatus.DISABLE),
    ]);

    await admin.listUser(BROKER);
    checkEq('list_user omits an unset filter instead of sending ""',
      t.call(RequestCode.AUTH_LIST_USER).ext, {});

    admin.shutdown();
  }

  // ------------------------------------------------ 4. empty bodies decode to none
  {
    const t = new FakeTransport();
    const admin = startedAdmin(t);
    // No script entry: SUCCESS with no body at all.
    checkEq('get_user with an empty body is null', await admin.getUser(BROKER, USER), null);
    checkEq('list_user with an empty body is []', await admin.listUser(BROKER, '*'), []);
    checkEq('get_acl with an empty body is null', await admin.getAcl(BROKER, SUBJECT), null);
    checkEq('list_acl with an empty body is []', await admin.listAcl(BROKER, '*', null), []);
    admin.shutdown();
  }

  // ------------------------------------------ 5. create/update/delete acl
  {
    const t = new FakeTransport();
    const admin = startedAdmin(t);
    t.script(RequestCode.AUTH_CREATE_ACL, { body: 'not-a-json-body{' });

    const created = await admin.createAcl(BROKER, SUBJECT, [RESOURCE, RESOURCE2],
      ['Pub', 'Sub'], ['192.168.0.1'], Decision.ALLOW);
    const c = t.call(RequestCode.AUTH_CREATE_ACL);
    check('create_acl returns without reading the reply body', created === undefined);
    checkEq('create_acl request code', c.code, RequestCode.AUTH_CREATE_ACL);
    checkEq('create_acl header is exactly subject', c.ext, { subject: SUBJECT });
    checkEq('create_acl convenience body: one policy without policyType, one entry per '
      + 'resource repeating the same values', c.body,
      `{"subject":"${SUBJECT}","policies":[{"entries":[`
        + `{"resource":"${RESOURCE}","actions":["Pub","Sub"],"sourceIps":["192.168.0.1"],"decision":"Allow"},`
        + `{"resource":"${RESOURCE2}","actions":["Pub","Sub"],"sourceIps":["192.168.0.1"],"decision":"Allow"}]}]}`);
    const policy = JSON.parse(c.body as string).policies[0] as Record<string, any>;
    check('policyType is absent (not null) in the convenience policy', !('policyType' in policy),
      JSON.stringify(policy));

    await admin.updateAcl(BROKER, new AclInfo(SUBJECT, [
      new AclPolicy(PolicyType.CUSTOM, [new AclEntry(RESOURCE, ['Sub'], null, Decision.DENY)]),
      new AclPolicy(PolicyType.DEFAULT, []),
    ]), null, null, null, null, 5000);
    const u = t.call(RequestCode.AUTH_UPDATE_ACL);
    checkEq('update_acl request code', u.code, RequestCode.AUTH_UPDATE_ACL);
    checkEq('update_acl header is exactly subject', u.ext, { subject: SUBJECT });
    checkEq('update_acl header carries no policy fields',
      Object.keys(u.ext).sort(), ['subject']);
    checkEq('update_acl object form body keeps policyType and drops an unset sourceIps', u.body,
      `{"subject":"${SUBJECT}","policies":[`
        + `{"policyType":"Custom","entries":[{"resource":"${RESOURCE}","actions":["Sub"],"decision":"Deny"}]},`
        + `{"policyType":"Default","entries":[]}]}`);
    checkEq('update_acl honours an explicit timeout', u.timeout, 5000);

    await admin.deleteAcl(BROKER, SUBJECT, RESOURCE);
    const d = t.call(RequestCode.AUTH_DELETE_ACL);
    checkEq('delete_acl request code', d.code, RequestCode.AUTH_DELETE_ACL);
    checkEq('delete_acl header is exactly subject + resource', d.ext,
      { subject: SUBJECT, resource: RESOURCE });
    checkEq('delete_acl header has no stray policy/decision keys',
      Object.keys(d.ext).sort(), ['resource', 'subject']);
    checkEq('delete_acl sends no body', d.body, null);

    await admin.deleteAcl(BROKER, SUBJECT);
    checkEq('delete_acl omits an unset resource',
      t.call(RequestCode.AUTH_DELETE_ACL).ext, { subject: SUBJECT });

    admin.shutdown();
  }

  // ------------------------------------------------------- 6. get / list acl
  {
    const t = new FakeTransport();
    const admin = startedAdmin(t);
    const aclJson = `{"subject":"${SUBJECT}","policies":[{"policyType":"Custom","entries":[`
      + `{"resource":"${RESOURCE}","actions":["Pub","Sub"],"sourceIps":["192.168.0.1"],"decision":"Allow"}]}]}`;
    t.script(RequestCode.AUTH_GET_ACL, { body: aclJson });
    const one = await admin.getAcl(BROKER, SUBJECT);
    const g = t.call(RequestCode.AUTH_GET_ACL);
    checkEq('get_acl request code', g.code, RequestCode.AUTH_GET_ACL);
    checkEq('get_acl header is exactly subject', g.ext, { subject: SUBJECT });
    checkEq('get_acl sends no body', g.body, null);
    checkEq('get_acl decoded policy type', one && one.policies[0].policyType, PolicyType.CUSTOM);
    checkEq('get_acl decoded nested entry', one && one.policies[0].entries[0],
      new AclEntry(RESOURCE, ['Pub', 'Sub'], ['192.168.0.1'], Decision.ALLOW));
    checkEq('get_acl re-encodes to the reply it came from', one && one.encode().toString('utf8'), aclJson);

    t.script(RequestCode.AUTH_LIST_ACL, {
      body: `[${aclJson},{"subject":"Group:authGroup","policies":[{"entries":[`
        + `{"resource":"Topic:Two","decision":"Deny"}]}]}]`,
    });
    const many = await admin.listAcl(BROKER, 'User:*', 'Topic:*');
    const l = t.call(RequestCode.AUTH_LIST_ACL);
    checkEq('list_acl request code', l.code, RequestCode.AUTH_LIST_ACL);
    checkEq('list_acl header is exactly subjectFilter + resourceFilter', l.ext,
      { subjectFilter: 'User:*', resourceFilter: 'Topic:*' });
    checkEq('list_acl decoded acl array length', many.length, 2);
    checkEq('list_acl first acl subject', many[0].subject, SUBJECT);
    checkEq('list_acl second acl keeps an absent policyType', many[1].policies[0].policyType, null);
    checkEq('list_acl entry keeps an absent actions list', many[1].policies[0].entries[0].actions, null);

    await admin.listAcl(BROKER, 'User:*');
    checkEq('list_acl omits an unset resourceFilter',
      t.call(RequestCode.AUTH_LIST_ACL).ext, { subjectFilter: 'User:*' });

    admin.shutdown();
  }

  // ---------------------------------- 7. only SUCCESS is accepted; anything else raises
  {
    const t = new FakeTransport();
    const admin = startedAdmin(t);
    t.script(RequestCode.AUTH_CREATE_USER, { code: ResponseCode.SYSTEM_ERROR, remark: 'auth write failed' });
    let err: MQBrokerException | null = null;
    try { await admin.createUser(BROKER, USER, PASS, UserType.NORMAL); } catch (e) { err = e as MQBrokerException; }
    check('a non-SUCCESS create_user rejects', err != null);
    checkEq('the raised error carries the response code', err != null ? err.responseCode : -1,
      ResponseCode.SYSTEM_ERROR);
    check('the raised error carries the broker remark',
      err != null && (err as Error).message.includes('auth write failed'),
      err != null ? (err as Error).message : '');

    t.script(RequestCode.AUTH_GET_ACL, { code: ResponseCode.POLICY_NOT_EXIST, remark: 'no such policy' });
    err = null;
    let raisedCode = -1;
    try { await admin.getAcl(BROKER, SUBJECT); } catch (e) { raisedCode = (e as MQBrokerException).responseCode; }
    checkEq('a non-SUCCESS get_acl rejects with its own code', raisedCode, ResponseCode.POLICY_NOT_EXIST);

    // The control: the same call answers SUCCESS and completes.
    t.script(RequestCode.AUTH_GET_ACL, { body: `{"subject":"${SUBJECT}","policies":[]}` });
    checkEq('the same get_acl completes once the broker says SUCCESS',
      (await admin.getAcl(BROKER, SUBJECT)).subject, SUBJECT);

    t.script(RequestCode.AUTH_DELETE_ACL, { code: ResponseCode.NO_PERMISSION, remark: 'reserve field' });
    raisedCode = -1;
    try { await admin.deleteAcl(BROKER, SUBJECT, RESOURCE); }
    catch (e) { raisedCode = (e as MQBrokerException).responseCode; }
    checkEq('a non-SUCCESS delete_acl rejects with its own code', raisedCode, ResponseCode.NO_PERMISSION);

    admin.shutdown();
  }

  // ---------------- 8. the address is the caller's, unchanged, even with the VIP switch on
  {
    const t = new FakeTransport();
    const admin = startedAdmin(t, true);
    checkEq('the VIP switch is on for this admin', admin.vipChannelEnabled, true);

    await admin.createUser(BROKER, USER, PASS, UserType.NORMAL);
    checkEq('create_user dials the caller address, not the VIP port',
      t.call(RequestCode.AUTH_CREATE_USER).addr, BROKER);
    await admin.getUser(BROKER, USER);
    checkEq('get_user dials the caller address, not the VIP port',
      t.call(RequestCode.AUTH_GET_USER).addr, BROKER);
    await admin.listAcl(BROKER, 'User:*', 'Topic:*');
    checkEq('list_acl dials the caller address, not the VIP port',
      t.call(RequestCode.AUTH_LIST_ACL).addr, BROKER);
    await admin.deleteAcl(BROKER, SUBJECT, RESOURCE);
    checkEq('delete_acl dials the caller address, not the VIP port',
      t.call(RequestCode.AUTH_DELETE_ACL).addr, BROKER);

    // Control: under the very same switch a neighbouring broker admin RPC does
    // move to port-2, so the address above is a choice, not an unset flag.
    t.script(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, {
      body: JSON.stringify({ subscriptionGroupTable: {}, forbiddenTable: {}, dataVersion: {} }),
    });
    await admin.getAllSubscriptionGroup(BROKER);
    checkEq('a neighbouring broker RPC does shift under the same switch',
      t.call(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG).addr, BROKER_VIP);

    // And the error names the address that was actually dialled.
    t.script(RequestCode.AUTH_DELETE_USER, { code: ResponseCode.SYSTEM_ERROR, remark: 'nope' });
    let addr = '';
    try { await admin.deleteUser(BROKER, USER); } catch (e) { addr = (e as MQBrokerException).brokerAddr; }
    checkEq('the error reports the caller address', addr, BROKER);

    admin.shutdown();
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
