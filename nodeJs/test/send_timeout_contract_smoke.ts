// Smoke: the send-time budget contract —
//   1. an explicit per-call timeout is the value that reaches the transport,
//      not the producer-wide sendMsgTimeout;
//   2. one call has ONE budget: when route lookup / queue selection / an earlier
//      attempt already spent it, the loop stops with "sendDefaultImpl call
//      timeout" instead of issuing a doomed request;
//   3. sendBySelector charges route lookup + the selector against the same
//      budget ("sendSelectImpl call timeout");
//   4. while retries remain, a single attempt is capped by
//      sendMsgMaxTimeoutPerRequest so a slow broker cannot swallow the retry's
//      share of the budget.
// Run: node --experimental-strip-types test/send_timeout_contract_smoke.ts
import { DefaultMQProducer } from '../src/client/producer.ts';
import { Message, MessageQueue } from '../src/common/message.ts';
import { SendResult, SendStatus } from '../src/client/send_result.ts';
import { TopicPublishInfo } from '../src/client/mq_client.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RemotingConnectException, RemotingTooMuchRequestException } from '../src/remoting/exception.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, detail = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${detail}`); }
}

const TOPIC = 'TimeoutContractTopic';
const ADDR = '127.0.0.1:10911';

type FakeOpts = {
  timeouts: number[];
  asyncTimeouts: number[];
  // attempt index (0-based) -> what the fake broker does for a sync send
  send?: (attempt: number) => SendResult;
};

function attachFakeClient(p: DefaultMQProducer, opts: FakeOpts): void {
  const info = new TopicPublishInfo();
  info.msgQueueList = [new MessageQueue(TOPIC, 'broker-a', 0)];
  (p as any).client = {
    getTopicPublishInfo: () => info,
    updateTopicRouteInfoFromNameServer: async () => true,
    publishAddrFor: () => ADDR,
    findBrokerAddressInPublish: () => ADDR,
    buildSendRequest: () => RemotingCommand.createRequestCommand(310, {}),
    sendMessage: async (_addr: string, _req: RemotingCommand, mq: MessageQueue, timeout: number) => {
      const attempt = opts.timeouts.length;
      opts.timeouts.push(timeout);
      if (opts.send) return opts.send(attempt);
      return new SendResult(SendStatus.SEND_OK, 'mid', mq, 0);
    },
    sendMessageAsync: (_addr: string, _req: RemotingCommand, mq: MessageQueue, timeout: number,
      cb: (sr: SendResult | null, err: Error | null) => void) => {
      opts.asyncTimeouts.push(timeout);
      cb(new SendResult(SendStatus.SEND_OK, 'mid', mq, 0), null);
    },
  };
}

function spin(ms: number): void {
  const until = Date.now() + ms;
  while (Date.now() < until) { /* burn the budget on purpose */ }
}

(async () => {
  console.log('== explicit timeout reaches the transport ==');
  {
    const p = new DefaultMQProducer('GID_timeout_explicit');
    p.setSendMsgTimeout(3000);
    const timeouts: number[] = [];
    attachFakeClient(p, { timeouts, asyncTimeouts: [] });
    const r = await p.send(new Message(TOPIC, Buffer.from('x')), 700);
    // The budget is charged the milliseconds spent before the attempt, so the
    // value may be a hair under 700 -- but never the producer-wide 3000.
    check('send(msg, 700) hands the caller budget to the transport, not sendMsgTimeout',
      timeouts.length === 1 && timeouts[0] > 600 && timeouts[0] <= 700,
      `timeouts=${JSON.stringify(timeouts)}`);
    check('a completed send still returns SEND_OK', r.sendStatus === SendStatus.SEND_OK);
  }

  console.log('== no explicit timeout falls back to sendMsgTimeout ==');
  {
    const p = new DefaultMQProducer('GID_timeout_default');
    p.setSendMsgTimeout(1200);
    const timeouts: number[] = [];
    attachFakeClient(p, { timeouts, asyncTimeouts: [] });
    await p.send(new Message(TOPIC, Buffer.from('x')));
    check('send(msg) uses sendMsgTimeout', timeouts.length === 1 && timeouts[0] > 1100 && timeouts[0] <= 1200,
      `timeouts=${JSON.stringify(timeouts)}`);
  }

  console.log('== one budget per call: sendDefaultImpl ==');
  {
    const p = new DefaultMQProducer('GID_timeout_budget');
    const timeouts: number[] = [];
    attachFakeClient(p, { timeouts, asyncTimeouts: [] });
    // Queue selection is part of the same budget: here it eats the whole 5 ms.
    (p as any)._selectOneMessageQueue = () => {
      spin(40);
      return new MessageQueue(TOPIC, 'broker-a', 0);
    };
    let err: any = null;
    try { await p.send(new Message(TOPIC, Buffer.from('x')), 5); } catch (e) { err = e; }
    check('a spent budget ends the call as a timeout, not a retry-exhausted failure',
      err instanceof RemotingTooMuchRequestException && err.message === 'sendDefaultImpl call timeout',
      err != null ? `${err.name}: ${err.message}` : 'no error');
    check('no request is issued once the budget is gone', timeouts.length === 0, `sends=${timeouts.length}`);
  }

  console.log('== one budget per call: sendSelectImpl ==');
  {
    const p = new DefaultMQProducer('GID_timeout_select');
    const timeouts: number[] = [];
    attachFakeClient(p, { timeouts, asyncTimeouts: [] });
    const selector = { select: () => { spin(40); return new MessageQueue(TOPIC, 'broker-a', 0); } } as any;
    let err: any = null;
    try { await p.sendBySelector(new Message(TOPIC, Buffer.from('x')), selector, null, 5); } catch (e) { err = e; }
    check('route lookup + selector are charged against the caller budget',
      err instanceof RemotingTooMuchRequestException && err.message === 'sendSelectImpl call timeout',
      err != null ? `${err.name}: ${err.message}` : 'no error');
    check('a timed-out selector issues no request', timeouts.length === 0, `sends=${timeouts.length}`);
  }

  console.log('== sendMsgMaxTimeoutPerRequest caps a single attempt ==');
  {
    const p = new DefaultMQProducer('GID_timeout_cap');
    p.setSendMsgTimeout(2000);
    p.setSendMsgMaxTimeoutPerRequest(150);
    p.retryTimesWhenSendFailed = 1;
    const timeouts: number[] = [];
    attachFakeClient(p, {
      timeouts, asyncTimeouts: [],
      send: (attempt) => {
        if (attempt === 0) throw new RemotingConnectException('boom');
        return new SendResult(SendStatus.SEND_OK, 'mid', null, 0);
      },
    });
    await p.send(new Message(TOPIC, Buffer.from('x')));
    check('a capped attempt while retries remain', timeouts.length === 2 && timeouts[0] === 150,
      `timeouts=${JSON.stringify(timeouts)}`);
    check('the retry keeps what is left of the caller budget', timeouts.length === 2 && timeouts[1] > 150,
      `timeouts=${JSON.stringify(timeouts)}`);
    check('the cap defaults to -1 (off)', new DefaultMQProducer('GID_timeout_cap_default').getSendMsgMaxTimeoutPerRequest() === -1);
  }

  console.log('== explicit async timeout ==');
  {
    const p = new DefaultMQProducer('GID_timeout_async');
    p.setSendMsgTimeout(3000);
    const asyncTimeouts: number[] = [];
    attachFakeClient(p, { timeouts: [], asyncTimeouts });
    const seen: Array<[unknown, unknown]> = [];
    await p.sendAsync(new Message(TOPIC, Buffer.from('x')), (sr, err) => { seen.push([sr, err]); }, 800);
    check('sendAsync(msg, cb, 800) hands the caller budget to the transport',
      asyncTimeouts.length === 1 && asyncTimeouts[0] > 700 && asyncTimeouts[0] <= 800,
      `timeouts=${JSON.stringify(asyncTimeouts)}`);
    check('the async callback fires once with the result', seen.length === 1 && seen[0][1] == null);
  }

  console.log(`\n${fail === 0 ? `ALL GREEN (${pass} checks)` : `${fail} failure(s)`}`);
  process.exit(fail === 0 ? 0 : 1);
})();
