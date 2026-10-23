// Batch-send schedule guard — offline test of which members a batch refuses.
//
// Baseline this suite pins:
//   * a batch leaves the process as ONE message carrying ONE property set, so a
//     member that asks for a later delivery would put that request on the envelope
//     holding every other member. The whole batch is refused;
//   * "asks for a later delivery" covers FOUR spellings: the delay level (DELAY)
//     and the three timer forms — relative seconds (TIMER_DELAY_SEC), relative
//     milliseconds (TIMER_DELAY_MS) and an absolute deliver-at (TIMER_DELIVER_MS);
//   * checking only the delay level is the bug this pins: a timer-scheduled member
//     would be packed and sent instead of refused;
//   * a zero under any of the four names is "no schedule", so it must NOT reject.
//     Without that control a presence-only check passes the same tests;
//   * the refusal is a positive statement about the wording callers branch on.
//
// Run: node --experimental-strip-types test/batch_schedule_guard_smoke.ts
import { Message, MessageBatch } from '../src/common/message.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const SCHEDULE_NAMES = ['DELAY', 'TIMER_DELAY_SEC', 'TIMER_DELAY_MS', 'TIMER_DELIVER_MS'];

function msg(topic: string, body: string): Message {
  return new Message(topic, Buffer.from(body));
}

function rejection(list: Message[]): string {
  try {
    MessageBatch.generateFromList(list);
    return '';
  } catch (e) {
    return (e as Error).message;
  }
}

// A plain batch is the control the guard must never touch.
{
  const batch = MessageBatch.generateFromList([msg('BatchGuardTopic', 'a'), msg('BatchGuardTopic', 'b')]);
  check('a plain batch is accepted', batch.length === 2);
  check('the batch takes its topic from the members', batch.getTopic() === 'BatchGuardTopic');
  check('the batch body is the packed members', batch.getBody().length > 0);
}

// Every one of the four spellings rejects, with the same wording.
for (const name of SCHEDULE_NAMES) {
  const single = [msg('BatchGuardTopic', 'a')];
  single[0].putProperty(name, '3');
  check(`${name}=3 rejects the batch`,
    rejection(single).includes('Delayed messages are not supported for batching'),
    rejection(single));

  // The same value under the same name must be refused wherever it sits in the
  // batch, not only on the first member.
  const second = [msg('BatchGuardTopic', 'a'), msg('BatchGuardTopic', 'b')];
  second[1].putProperty(name, '3');
  check(`${name}=3 on a non-first member rejects the batch`,
    rejection(second).includes('Delayed messages are not supported for batching'),
    rejection(second));

  // Paired control: a zero means "no schedule" and must be packed normally.
  const zero = [msg('BatchGuardTopic', 'a')];
  zero[0].putProperty(name, '0');
  check(`${name}=0 leaves the batch alone`, rejection(zero) === '', rejection(zero));
}

// The setters the public API exposes reach the same four properties.
{
  const viaLevel = msg('BatchGuardTopic', 'a');
  viaLevel.setDelayTimeLevel(2);
  check('setDelayTimeLevel rejects', rejection([viaLevel]).includes('Delayed messages are not supported'));

  const viaSec = msg('BatchGuardTopic', 'a');
  viaSec.setDelayTimeSec(30);
  check('setDelayTimeSec rejects', rejection([viaSec]).includes('Delayed messages are not supported'));

  const viaMs = msg('BatchGuardTopic', 'a');
  viaMs.setDelayTimeMs(30000);
  check('setDelayTimeMs rejects', rejection([viaMs]).includes('Delayed messages are not supported'));

  const viaDeliver = msg('BatchGuardTopic', 'a');
  viaDeliver.setDeliverTimeMs(1700000000000);
  check('setDeliverTimeMs rejects', rejection([viaDeliver]).includes('Delayed messages are not supported'));
}

// The other refusals keep their precedence and wording.
{
  check('an empty list is refused', rejection([]).includes('messages must not be null or empty'));

  const mixed = [msg('BatchGuardTopicA', 'a'), msg('BatchGuardTopicB', 'b')];
  check('mixed topics are refused', rejection(mixed).includes('The topic of the messages in one batch should be the same'));

  const retry = msg('%RETRY%BatchGuardGroup', 'a');
  check('a retry topic is refused', rejection([retry]).includes('Retry Group is not supported for batching'));

  const mismatch = [msg('BatchGuardTopic', 'a'), msg('BatchGuardTopic', 'b')];
  mismatch[0].setWaitStoreMsgOK(true);
  mismatch[1].setWaitStoreMsgOK(false);
  check('a waitStoreMsgOK mismatch is refused',
    rejection(mismatch).includes('The waitStoreMsgOK of the messages in one batch should be the same'));
}

console.log(`\nbatch schedule guard: ${pass} checks, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
