// BATCH_ACK_MESSAGE(200151) request-shape smoke -- no network.
//
//   node --experimental-strip-types test/pop_batch_ack_smoke.ts
//
// The batch ack is the one POP primitive the runtime never calls: the POP consumer
// acknowledges one message at a time. That makes its request construction easy to break
// silently -- this file pins what actually leaves the client:
//
//   1. the request carries the encoded body (the grouping key, the bit index and the
//      base64 BitSet are inside it), and
//   2. it carries NO custom header: group, topic, queue, offsets, popTime and
//      invisibleTime all travel in the body, so a header field here would be a field the
//      broker never reads.
//
// Plus the reply contract: SUCCESS resolves, anything else surfaces as a broker exception
// that keeps the response code.
import assert from 'node:assert';
import { Buffer } from 'node:buffer';

import { MQClient } from '../src/client/mq_client.ts';
import { ResponseCode, RequestCode } from '../src/remoting/codes.ts';
import { MQBrokerException } from '../src/remoting/exception.ts';
import { buildBatchAckMessageRequestBody } from '../src/remoting/pop_bodies.ts';

let pass = 0;
let fail = 0;
function check(name: string, cond: boolean, detail = ''): void {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${detail}`); }
}

// Two pops of queue 0: offsets 0 and 1 share one checkpoint, offset 3 is a later pop.
const CK_A0 = '0 1727700000000 60000 3 0 broker-a 0 0';
const CK_A1 = '0 1727700000000 60000 3 0 broker-a 0 1';
const CK_B = '2 1727700000500 60000 3 0 broker-a 0 3';

function clientRecording(handler: (addr: string, req: any) => any): { client: MQClient; seen: any[] } {
  const seen: any[] = [];
  const rc = {
    invokeSync: async (addr: string, req: any, _timeout: number) => {
      seen.push(req);
      return handler(addr, req);
    },
  };
  return { client: new MQClient('cid-batch-ack-smoke', 'ns-a:9876', rc as any), seen };
}

async function main(): Promise<void> {
  console.log('== the batch ack request carries the body and no header ==');
  const { client, seen } = clientRecording(() => ({ code: ResponseCode.SUCCESS, body: null, remark: '' }));
  await client.batchAckMessage('127.0.0.1:10911',
    buildBatchAckMessageRequestBody('BatchAckTopic', 'GID_BatchAck', [CK_A0, CK_A1, CK_B]), 3000);

  assert.strictEqual(seen.length, 1, 'exactly one request goes out');
  const req = seen[0];
  check('the request is BATCH_ACK_MESSAGE(200151)', req.code === RequestCode.BATCH_ACK_MESSAGE,
    `code=${req.code}`);
  check('the request carries the encoded batch body',
    Buffer.isBuffer(req.body) && req.body != null && req.body.length > 0,
    `body=${req.body == null ? 'null' : req.body.length + ' bytes'}`);

  const wire = JSON.parse((req.body as Buffer).toString('utf8'));
  check('the body names the first checkpoint broker and merges the two pops into two entries',
    wire.brokerName === 'broker-a' && wire.acks.length === 2,
    JSON.stringify(wire).slice(0, 200));
  check('the merged entry acknowledges its offsets as bits over its own start offset',
    wire.acks[0].so === 0 && wire.acks[0].b === Buffer.from([0x03]).toString('base64')
    && wire.acks[1].so === 2 && wire.acks[1].b === Buffer.from([0x02]).toString('base64'),
    `so=${wire.acks[0].so}/${wire.acks[1].so} b=${wire.acks[0].b}/${wire.acks[1].b}`);
  check('every entry echoes group, topic, marker, queue, reviveQid, popTime and invisibleTime '
    + 'from its checkpoint',
    wire.acks.every((a: any) => a.c === 'GID_BatchAck' && a.t === 'BatchAckTopic'
      && a.r === '0' && a.q === 0 && a.rq === 3 && a.it === 60000)
    && wire.acks[0].pt === 1727700000000 && wire.acks[1].pt === 1727700000500,
    JSON.stringify(wire.acks[1]));

  check('the request has no custom header', req.customHeader == null,
    `customHeader=${JSON.stringify(req.customHeader)}`);
  const headerFields = req.encodeHeader(0).toString('utf8');
  check('nothing but the frame itself is in the header: no group, topic or offset field',
    !headerFields.includes('consumerGroup') && !headerFields.includes('topic')
    && !headerFields.includes('queueId') && !headerFields.includes('offset'),
    headerFields.slice(0, 160));

  console.log('== the reply contract ==');
  const ok = clientRecording(() => ({ code: ResponseCode.SUCCESS, body: null, remark: 'ok' }));
  let resolved = false;
  try {
    await ok.client.batchAckMessage('127.0.0.1:10911',
      buildBatchAckMessageRequestBody('BatchAckTopic', 'GID_BatchAck', [CK_A0]), 3000);
    resolved = true;
  } catch (e) {
    resolved = false;
  }
  check('SUCCESS resolves without raising', resolved);

  const bad = clientRecording(() => ({ code: ResponseCode.SYSTEM_ERROR, body: null, remark: 'busy' }));
  let code = 0;
  let remark = '';
  try {
    await bad.client.batchAckMessage('127.0.0.1:10911',
      buildBatchAckMessageRequestBody('BatchAckTopic', 'GID_BatchAck', [CK_A0]), 3000);
  } catch (e: any) {
    code = e instanceof MQBrokerException ? e.responseCode : 0;
    remark = e?.message || '';
  }
  check('a non-SUCCESS answer surfaces as a broker exception that keeps the response code',
    code === ResponseCode.SYSTEM_ERROR && remark.includes('busy'),
    `code=${code} remark=${remark}`);

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail > 0 ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
