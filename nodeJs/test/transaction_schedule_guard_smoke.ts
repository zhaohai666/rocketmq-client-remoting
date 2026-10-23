// Transactional-send schedule guard — offline test of which members a half message refuses.
//
// Baseline this suite pins:
//   * a half message is committed exactly as it stands, so a schedule asked for on
//     the transactional path can never be honoured — the send is refused;
//   * "asks for a later delivery" covers FOUR spellings: the delay level (DELAY) and
//     the three timer forms — relative seconds (TIMER_DELAY_SEC), relative
//     milliseconds (TIMER_DELAY_MS) and an absolute deliver-at (TIMER_DELIVER_MS);
//   * unlike the batch gate, this one reads the KEY, not the value: a "0" or an empty
//     value asks for nothing yet is still a stamp the transaction path cannot carry,
//     so it refuses too;
//   * only those four names refuse. A property that merely looks delay-ish (DELAY_TIME
//     is not a schedule spelling at all) must reach the send path;
//   * a refusal leaves the caller's message untouched — no half-message marker, no
//     producer group.
//
// Run: node --experimental-strip-types test/transaction_schedule_guard_smoke.ts
import { Buffer } from 'node:buffer';

import { Message } from '../src/common/message.ts';
import { MessageConst } from '../src/common/messageConst.ts';
import {
  DefaultMQProducer, TransactionListener, LocalTransactionState,
} from '../src/client/producer.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const SCHEDULE_NAMES = [
  MessageConst.PROPERTY_DELAY_TIME_LEVEL, MessageConst.PROPERTY_TIMER_DELAY_SEC,
  MessageConst.PROPERTY_TIMER_DELAY_MS, MessageConst.PROPERTY_TIMER_DELIVER_MS,
];

class CommitListener extends TransactionListener {
  executeLocalTransaction(): number { return LocalTransactionState.COMMIT_MESSAGE; }
  checkLocalTransaction(): number { return LocalTransactionState.COMMIT_MESSAGE; }
}

// No client is ever built: the guard has to answer before the send path is reached.
function producer(): DefaultMQProducer {
  const p = new DefaultMQProducer('GID_TxGuard');
  p.setInstanceName(`tx-guard-${Math.random().toString(36).slice(2, 8)}`);
  return p;
}

async function failure(msg: Message): Promise<string> {
  try {
    await producer().sendMessageInTransaction(msg, new CommitListener());
    return '<no error>';
  } catch (e) {
    return (e as Error).message;
  }
}

const REFUSED = 'Transactional messages do not support delayed delivery';

console.log('== the four schedule spellings refuse ==');
for (const name of SCHEDULE_NAMES) {
  for (const value of ['3', '0', '']) {
    const msg = new Message('TxGuardTopic', Buffer.from('x'));
    msg.putProperty(name, value);
    const message = await failure(msg);
    check(`${name}=${JSON.stringify(value)} is refused`, message === REFUSED, `got ${message}`);
  }
}

console.log('== the refusal leaves the message alone ==');
{
  const msg = new Message('TxGuardTopic', Buffer.from('x'));
  msg.putProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS, '1700000000000');
  await failure(msg);
  check('a refused message carries no half-message marker',
    msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED) == null);
  check('a refused message carries no producer group',
    msg.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP) == null);
}

console.log('== only those four names refuse ==');
for (const name of ['DELAY_TIME', 'SOME_USER_PROPERTY']) {
  const msg = new Message('TxGuardTopic', Buffer.from('x'));
  msg.putProperty(name, '3');
  const message = await failure(msg);
  check(`${name} is not a schedule spelling, so it reaches the send path`,
    message !== REFUSED && message.includes('not started'), `got ${message}`);
}

console.log('== a plain transactional message passes the guard ==');
{
  const msg = new Message('TxGuardTopic', Buffer.from('x'));
  const message = await failure(msg);
  check('the plain message is refused only for having no client',
    message !== REFUSED && message.includes('not started'), `got ${message}`);
}

console.log(`\ntransaction schedule guard: ${pass + fail} checks, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
