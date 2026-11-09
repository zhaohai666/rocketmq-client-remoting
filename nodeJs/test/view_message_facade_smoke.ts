// viewMessage facade contract — offline wire test (no sockets).
//
// Pinned contract (Java parity, two distinct layers):
//   - MQAdminImpl#viewMessage (MQAdminImpl.java:267) is a SINGLE leg: decode
//     the offset msgId, ask the broker that stamped it VIEW_MESSAGE_BY_ID(33).
//     Decode failure maps to the NO_MESSAGE client error "query message by id
//     finished, but no message." The producer / push / pull facades ride this
//     single-leg path — NO uniq-key fallback at this layer.
//   - DefaultMQAdminExtImpl#viewMessage (DefaultMQAdminExtImpl.java:578) is
//     TWO legs: any leg-1 failure ("the msgId maybe created by new client")
//     falls back to the U-index query; when neither leg matches it returns
//     null instead of raising.
//
// Run: node --experimental-strip-types test/view_message_facade_smoke.ts
import { MQClient } from '../src/client/mq_client.ts';
import { DefaultMQAdminExt } from '../src/client/admin.ts';
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { DefaultMQPullConsumer } from '../src/client/pull_consumer.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import {
  encodeMessageExt, createMessageId,
} from '../src/common/messageDecoder.ts';
import { ipAndPortToBytes } from '../src/common/utilAll.ts';
import { MessageExt } from '../src/common/message.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const BROKER_PORT = 10911;
const PHYS_OFFSET = 987654321;
const offsetMsgId = createMessageId(ipAndPortToBytes('127.0.0.1', BROKER_PORT), PHYS_OFFSET);

function storedMessage(): MessageExt {
  const m = new MessageExt();
  m.setTopic('ViewTopic');
  m.setBody(Buffer.from('view-me'));
  m.setQueueId(0);
  m.setQueueOffset(7);
  m.setCommitLogOffset(PHYS_OFFSET);
  m.setSysFlag(0);
  m.setBornTimestamp(Date.now());
  m.setStoreTimestamp(Date.now());
  m.setBornHost('127.0.0.1');
  m.setStoreHost('127.0.0.1');
  m.storeHostPort = BROKER_PORT;
  m.setReconsumeTimes(0);
  m.setFlag(0);
  m.setBodyCrc(0);
  return m;
}

class RecordingClient {
  codes: number[] = [];
  lastRequest: RemotingCommand | null = null;
  responder: ((req: RemotingCommand) => RemotingCommand) | null = null;
  invokeSync(_addr: string, request: RemotingCommand, _timeout?: number): Promise<RemotingCommand> {
    this.codes.push(request.code);
    this.lastRequest = request;
    request.headerEncode();
    const resp = this.responder != null
      ? this.responder(request)
      : RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null)!;
    resp.opaque = request.opaque;
    return Promise.resolve(resp);
  }
}

let seq = 0;
function newClient(rc: RecordingClient): MQClient {
  return new MQClient(`127.0.0.1@view_${process.pid}_${seq++}`, '127.0.0.1:9876', rc as any);
}

async function main() {
  // ------------------- 1. instance leg: SUCCESS decodes the VIEW body
  {
    const rc = new RecordingClient();
    rc.responder = (req) => {
      const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null)!;
      resp.body = encodeMessageExt(storedMessage());
      return resp;
    };
    const client = newClient(rc);
    const msg = await client.viewMessage('ViewTopic', offsetMsgId, 3000);
    check('viewMessage SUCCESS decodes the message',
      msg != null && msg.getTopic() === 'ViewTopic' && msg.getBody().toString() === 'view-me',
      JSON.stringify(msg && msg.getTopic()));
    check('exactly one request, and it is VIEW_MESSAGE_BY_ID(33)',
      rc.codes.length === 1 && rc.codes[0] === RequestCode.VIEW_MESSAGE_BY_ID,
      `codes=${rc.codes}`);
    const exts = rc.lastRequest!.extFields;
    check('request carries topic + the decoded physical offset',
      exts != null && exts['topic'] === 'ViewTopic' && exts['offset'] === String(PHYS_OFFSET),
      JSON.stringify(exts));
  }

  // ------- 2. instance leg: a uniq key dies with the NO_MESSAGE client error,
  //              and never touches the network (the fallback is admin-only)
  {
    const rc = new RecordingClient();
    const client = newClient(rc);
    let threw: Error | null = null;
    try { await client.viewMessage('ViewTopic', 'MSGID-UNIQ-STRING-1', 3000); }
    catch (e) { threw = e as Error; }
    check('a uniq key surfaces the MQAdminImpl NO_MESSAGE wording',
      threw != null && threw.message.includes('query message by id finished, but no message.'),
      `threw=${threw && threw.message}`);
    check('decode failure sends nothing', rc.codes.length === 0, `codes=${rc.codes}`);
  }

  // ------------- 3. instance leg: a non-SUCCESS answer is the broker error
  {
    const rc = new RecordingClient();
    rc.responder = (req) => {
      const resp = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, 'store disk full')!;
      return resp;
    };
    const client = newClient(rc);
    let threw: Error | null = null;
    try { await client.viewMessage('ViewTopic', offsetMsgId, 3000); }
    catch (e) { threw = e as Error; }
    check('a failing broker answer surfaces as MQBrokerException',
      threw != null && threw.name === 'MQBrokerException' && threw.message.includes('store disk full'),
      `threw=${threw}`);
  }

  // ------------------- 4. facades ride the started client; unstarted rejects
  {
    const sentinel = storedMessage();
    const p = new DefaultMQProducer('GID_view');
    let producerThrew = false;
    try { await p.viewMessage('T', offsetMsgId); }
    catch { producerThrew = true; }
    check('producer viewMessage rejects before start', producerThrew);
    let gotTopic = '';
    (p as any).client = {
      viewMessage: async (_t: string, _id: string) => {
        gotTopic = _t;
        return sentinel;
      },
    };
    const got = await p.viewMessage('ViewTopic', offsetMsgId);
    check('producer viewMessage delegates to the started client',
      got === sentinel && gotTopic === 'ViewTopic');

    const push = new DefaultMQPushConsumer('GID_view');
    let pushThrew = false;
    try { await push.viewMessage('T', offsetMsgId); }
    catch { pushThrew = true; }
    check('push consumer viewMessage rejects before start', pushThrew);
    (push as any).mqClient = { viewMessage: async () => sentinel };
    check('push consumer viewMessage delegates', (await push.viewMessage('T', offsetMsgId)) === sentinel);

    const pull = new DefaultMQPullConsumer('GID_view');
    let pullThrew = false;
    try { await pull.viewMessage('T', offsetMsgId); }
    catch { pullThrew = true; }
    check('pull consumer viewMessage rejects before start', pullThrew);
    (pull as any).mqClient = { viewMessage: async () => sentinel };
    check('pull consumer viewMessage delegates', (await pull.viewMessage('T', offsetMsgId)) === sentinel);
  }

  // ------------------- 5. admin: leg1 hit returns without the fallback query
  {
    const admin = new DefaultMQAdminExt();
    const stored = storedMessage();
    (admin as any)._started = true;
    (admin as any).client = {
      viewMessage: async (t: string, id: string) => {
        if (t === 'ViewTopic' && id === offsetMsgId) return stored;
        throw new Error('no such message');
      },
    };
    const got = await admin.viewMessage('ViewTopic', offsetMsgId);
    check('admin leg1 hit returns the message untouched', got === stored);
  }

  // ------ 6. admin: leg1 failure falls back to the uniq-key query (two legs)
  {
    const admin = new DefaultMQAdminExt();
    const stored = storedMessage();
    let uniqQueried = false;
    (admin as any)._started = true;
    (admin as any).client = {
      viewMessage: async () => { throw new Error('no such message'); },
    };
    (admin as any).queryMessageFromRoute = async (_t: string, key: string) => {
      uniqQueried = key === 'UNIQ-1';
      return { messageList: [Object.assign(stored, {
        getProperty: (k: string) => k === 'UNIQ_KEY' ? 'UNIQ-1' : null,
      })], indexLastUpdateTimestamp: 0 };
    };
    const got = await admin.viewMessage('ViewTopic', 'UNIQ-1');
    check('admin uniq fallback returns the U-index hit', uniqQueried && got != null,
      `uniqQueried=${uniqQueried}`);
  }

  // -------- 7. admin: neither leg matching returns null instead of raising
  {
    const admin = new DefaultMQAdminExt();
    (admin as any)._started = true;
    (admin as any).client = {
      viewMessage: async () => { throw new Error('no such message'); },
    };
    (admin as any).queryMessageFromRoute = async () =>
      ({ messageList: [], indexLastUpdateTimestamp: 0 });
    let got: MessageExt | null | undefined;
    let threw: Error | null = null;
    try { got = await admin.viewMessage('ViewTopic', 'UNIQ-MISSING'); }
    catch (e) { threw = e as Error; }
    check('admin no-match returns null (Java parity), not an error',
      threw === null && got === null, `threw=${threw} got=${got}`);
  }

  console.log(fail === 0 ? `ALL GREEN (${pass} checks)` : `FAILED (${fail}/${pass + fail})`);
  process.exit(fail === 0 ? 0 : 1);
}

main();
