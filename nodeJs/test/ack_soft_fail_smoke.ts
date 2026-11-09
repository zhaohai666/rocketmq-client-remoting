// ACK_MESSAGE(200051) / CHANGE_MESSAGE_INVISIBLETIME(200053) soft-fail
// contract — offline wire test (no sockets).
//
// Pinned contract (Java parity): the broker's answer to these two POP cleanup
// RPCs is a RESULT, not an error. MQClientAPIImpl#ackMessageAsync:987-993 maps
// ANY non-SUCCESS code to AckStatus.NO_EXIST and still reports success to the
// callback; #changeInvisibleTimeAsync:1018-1036 does the same, attaching the
// decoded popTime / invisibleTime / reviveQid only on SUCCESS. The push
// consumer warns on non-SUCCESS and moves on (DefaultMQPushConsumerImpl:842).
// Only transport failures reject.
//
// Run: node --experimental-strip-types test/ack_soft_fail_smoke.ts
import { MQClient } from '../src/client/mq_client.ts';
import type { ChangeInvisibleTimeOutcome } from '../src/client/mq_client.ts';
import { AckMessageRequestHeader, ChangeInvisibleTimeRequestHeader } from '../src/remoting/headers.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const BROKER = '127.0.0.1:10911';

class RecordingClient {
  responder: ((req: RemotingCommand) => RemotingCommand) | null = null;
  codes: number[] = [];
  respond(req: RemotingCommand): RemotingCommand {
    if (this.responder != null) return this.responder(req);
    const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null)!;
    resp.opaque = req.opaque;
    return resp;
  }
  invokeSync(addr: string, request: RemotingCommand, _timeout?: number): Promise<RemotingCommand> {
    this.codes.push(request.code);
    return Promise.resolve(this.respond(request));
  }
  invokeOneway(_addr: string, _request: RemotingCommand): Promise<void> { return Promise.resolve(); }
  invokeAsync(_addr: string, request: RemotingCommand,
    cb: (r: RemotingCommand | null, e: Error | null) => void, _timeout?: number): void {
    cb(this.respond(request), null);
  }
}

let seq = 0;
function newClient(rc: RecordingClient): MQClient {
  return new MQClient(`127.0.0.1@ack_${process.pid}_${seq++}`, '127.0.0.1:9876', rc as any);
}

function ackHeader(): AckMessageRequestHeader {
  const h = new AckMessageRequestHeader();
  h.bname = 'broker-a';
  h.consumerGroup = 'GID_ack';
  h.topic = 'AckTopic';
  h.queueId = 0;
  h.extraInfo = '0 0 0 0 AckTopic broker-a 0 1';
  h.offset = 1;
  return h;
}

function ciHeader(): ChangeInvisibleTimeRequestHeader {
  const h = new ChangeInvisibleTimeRequestHeader();
  h.bname = 'broker-a';
  h.consumerGroup = 'GID_ack';
  h.topic = 'AckTopic';
  h.queueId = 0;
  h.extraInfo = '0 0 0 0 AckTopic broker-a 0 1';
  h.offset = 1;
  h.invisibleTime = 30000;
  h.suspend = false;
  return h;
}

function failure(req: RemotingCommand, code: number, remark: string): RemotingCommand {
  const resp = RemotingCommand.createResponseCommand(code, remark)!;
  resp.opaque = req.opaque;
  return resp;
}

async function main() {
  // ------------------------------------------------- 1. ack SUCCESS stays success
  {
    const rc = new RecordingClient();
    const client = newClient(rc);
    const code = await client.ackMessage(BROKER, ackHeader(), 3000);
    check('ack SUCCESS returns the code and does not throw',
      code === ResponseCode.SUCCESS && rc.codes[0] === RequestCode.ACK_MESSAGE, `code=${code}`);
  }

  // --------------------- 2. ack non-SUCCESS is AckStatus.NO_EXIST, NOT an error
  {
    const rc = new RecordingClient();
    const client = newClient(rc);
    rc.responder = (req) => failure(req, ResponseCode.SYSTEM_ERROR, 'no such checkpoint');
    let threw: Error | null = null;
    let code = -1;
    try { code = await client.ackMessage(BROKER, ackHeader(), 3000); }
    catch (e) { threw = e as Error; }
    check('ack SYSTEM_ERROR is a result carrying the broker code, not a rejection',
      threw === null && code === ResponseCode.SYSTEM_ERROR, `code=${code} threw=${threw}`);
  }

  // --------------------------------- 3. changeInvisibleTime SUCCESS decodes CK data
  {
    const rc = new RecordingClient();
    const client = newClient(rc);
    rc.responder = (req) => {
      const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null)!;
      resp.opaque = req.opaque;
      resp.extFields = { popTime: '1770000000', invisibleTime: '60000', reviveQid: '3' };
      return resp;
    };
    const outcome: ChangeInvisibleTimeOutcome = await client.changeInvisibleTime(BROKER, ciHeader(), 3000);
    check('changeInvisible SUCCESS reports OK with the decoded new window',
      outcome.code === ResponseCode.SUCCESS && outcome.header !== null &&
      outcome.header!.popTime === 1770000000 && outcome.header!.invisibleTime === 60000 &&
      outcome.header!.reviveQid === 3,
      JSON.stringify(outcome));
  }

  // ----------- 4. changeInvisibleTime non-SUCCESS is NO_EXIST, header stays null
  {
    const rc = new RecordingClient();
    const client = newClient(rc);
    rc.responder = (req) => failure(req, ResponseCode.SYSTEM_ERROR, 'no such checkpoint');
    let threw: Error | null = null;
    let outcome: ChangeInvisibleTimeOutcome | null = null;
    try { outcome = await client.changeInvisibleTime(BROKER, ciHeader(), 3000); }
    catch (e) { threw = e as Error; }
    check('changeInvisible SYSTEM_ERROR is a NO_EXIST result, not a rejection',
      threw === null && outcome !== null && outcome.code === ResponseCode.SYSTEM_ERROR &&
      outcome.header === null && outcome.remark === 'no such checkpoint',
      `threw=${threw} outcome=${JSON.stringify(outcome)}`);
  }

  console.log(fail === 0 ? `ALL GREEN (${pass} checks)` : `FAILED (${fail}/${pass + fail})`);
  process.exit(fail === 0 ? 0 : 1);
}

main();
