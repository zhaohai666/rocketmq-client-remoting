// The pull-result offset sanity warning — offline, driving the real per-queue push
// loop with its waits RECORDED and its WARN lines CAPTURED.
//
//   node --experimental-strip-types test/pull_offset_sanity_smoke.ts
//
// A FOUND round remembers the cursor value it asked with (prevRequestOffset), then
// advances the cursor to nextBeginOffset. When the round is over — the cursor moved,
// the batch handed to the consume side — the client looks back at the answer and
// warns when either
//
//   * nextBeginOffset < prevRequestOffset, or
//   * firstMsgOffset  < prevRequestOffset,
//
// where firstMsgOffset is the queue offset of the FIRST row of the list the round
// actually produces for consumption, i.e. AFTER this client's own tag pass and the
// FilterMessageHook rewrite; a round whose list is empty has no first row, so the
// number standing in for it is Number.MAX_SAFE_INTEGER and the second comparison can
// never fire.
//
// The warning is diagnostic only, and that is on trial here as much as the two legs:
// a firing round leaves the same cursor, the same pacing decision and the same PULL_RT
// / PULL TPS bookings as an identical round that does not fire. Only a FOUND round is
// judged — NO_NEW_MSG, NO_MATCHED_MSG and OFFSET_ILLEGAL bring no sanity line — and
// all three numbers reach the log in the one message, so an operator can see which of
// the two legs the round failed.
import { format } from 'node:util';
import { DefaultMQPushConsumer, pullOffsetsLookWrong } from '../src/client/consumer.ts';
import { ProcessQueue } from '../src/client/process_queue.ts';
import { PullResult, PullStatus } from '../src/client/consumer_result.ts';
import { ConsumerStatsManager } from '../src/client/consumer_stats.ts';
import { FilterMessageContext, FilterMessageHook } from '../src/client/hook.ts';
import { FilterAPI } from '../src/common/subscriptionData.ts';
import { MessageExt, MessageQueue } from '../src/common/message.ts';
import { mqKey } from '../src/client/offset_store.ts';
import { getLogger } from '../src/logging.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const TOPIC = 'PullOffsetSanityTopic';
const BROKER = 'broker-a';
const ADDR = '127.0.0.1:10911';
const PULL_INTERVAL_MS = 50;
// The number an empty round stands in for its missing first row.
const NO_ROW = Number.MAX_SAFE_INTEGER;
const consumerLogger = getLogger('client.consumer');

function row(offset: number, tag: string | null): MessageExt {
  const m = new MessageExt(TOPIC, Buffer.from('body-' + offset), tag);
  m.setQueueId(0);
  m.setQueueOffset(offset);
  m.setCommitLogOffset(offset);
  m.setBornTimestamp(Date.now());
  m.setStoreHost('127.0.0.1');
  m.setBornHost('127.0.0.1');
  return m;
}

function found(rows: MessageExt[], next: number): PullResult {
  return new PullResult(PullStatus.FOUND, next, 0, 1000, rows);
}
const IDLE = () => new PullResult(PullStatus.NO_NEW_MSG, 0, 0, 0, []);

class DropFirstHook extends FilterMessageHook {
  filterMessage(ctx: FilterMessageContext): any[] | null {
    ctx.msgList = (ctx.msgList ?? []).slice(1);
    return ctx.msgList;
  }
}

// warnLines collects every WARN the consumer logger emits while a drive runs, both
// the warn() and the warning() spellings, so a check can count sanity lines and tell
// them from the round's other warnings.
const warnLines: string[] = [];
const origWarn = consumerLogger.warn;
const origWarning = consumerLogger.warning;

// drive runs the REAL per-queue pull loop over one round: the cursor starts at
// `cursor` (the value the round asks with), `first` is that round's answer, and the
// wait is recorded rather than slept out.
async function drive(opts: {
  group: string;
  cursor: number;
  first: PullResult;
  expression?: string;
  hooks?: FilterMessageHook[];
  pullInterval?: number;
}): Promise<{
  waits: number[], handed: MessageExt[][], warnLines: string[], rounds: number,
  cursorAfter: number | undefined, pullInterval: number, c: DefaultMQPushConsumer,
}> {
  const c = new DefaultMQPushConsumer(opts.group);
  const anyC = c as any;
  c.pullInterval = opts.pullInterval ?? PULL_INTERVAL_MS;
  // No cache gate within reach: the round under test is the only thing on trial.
  c.pullThresholdForQueue = 1 << 30;
  c.pullThresholdSizeForQueue = 1 << 30;
  c.consumeConcurrentlyMaxSpan = 1 << 30;
  c.subscription.set(TOPIC, FilterAPI.buildSubscriptionData(TOPIC, opts.expression ?? '*'));
  c.started = true;
  c._statsManager = new ConsumerStatsManager();
  for (const hook of opts.hooks ?? []) c.registerFilterMessageHook(hook);

  const mq = new MessageQueue(TOPIC, BROKER, 0);
  const key = mqKey(mq);
  const pq = new ProcessQueue(false);
  const handed: MessageExt[][] = [];
  const origPut = pq.putMessage.bind(pq);
  pq.putMessage = (msgs: MessageExt[]): boolean => { handed.push(msgs); return origPut(msgs); };
  c.processQueueTable.set(key, pq);
  c.offsetTable.set(key, opts.cursor);
  c.mqClient = { updateTopicRouteInfoFromNameServer: async () => {} } as never;
  anyC._findBrokerAddressInSubscribe = () => ({ addr: ADDR, isSlave: false });

  const flag = anyC._newStopFlag();
  let rounds = 0;
  anyC._pullKernel = async () => {
    rounds++;
    if (rounds === 1) return opts.first;
    // The second fetch only ends the run, with a status that schedules nothing.
    anyC._stopFlags.delete(flag);
    return IDLE();
  };
  const waits: number[] = [];
  anyC._sleepOrStop = async (ms: number) => {
    waits.push(ms);
    anyC._stopFlags.delete(flag);
    return true;
  };
  warnLines.length = 0;
  consumerLogger.warn = (...args: any[]) => { warnLines.push(format(...args)); };
  consumerLogger.warning = (...args: any[]) => { warnLines.push(format(...args)); };
  try {
    await anyC._queuePullLoop(mq, flag);
  } finally {
    consumerLogger.warn = origWarn;
    consumerLogger.warning = origWarning;
  }
  return {
    waits, handed, warnLines: warnLines.slice(), rounds,
    cursorAfter: c.offsetTable.get(key), pullInterval: c.pullInterval, c,
  };
}

// sanityLines are the WARN lines of the offset sanity check.
function sanityLines(r: { warnLines: string[] }): string[] {
  return r.warnLines.filter((l) => l.includes('does not line up'));
}

// numbersOf reads the three offsets a sanity line carries.
function numbersOf(line: string): { next: number, first: number, prev: number } {
  const pick = (name: string) => {
    const m = new RegExp(`${name}=(\\d+)`).exec(line);
    return m ? Number(m[1]) : NaN;
  };
  return { next: pick('nextBeginOffset'), first: pick('firstMsgOffset'),
    prev: pick('prevRequestOffset') };
}

async function main() {
  // ---- 1. next < prev: the helper says so, and the round says it out loud
  {
    check('the helper flags a cursor that moved backwards',
      pullOffsetsLookWrong(50, 101, 100) === true);
    check('and flags nothing else',
      pullOffsetsLookWrong(150, 101, 100) === false
      && pullOffsetsLookWrong(100, 100, 100) === false, 'equal offsets must not flag');

    // Rows all ahead of the ask, so only the next-offset leg can fire here.
    const r = await drive({
      group: 'GID_sanity_next_backwards', cursor: 100,
      first: found([row(101, 'TagA'), row(102, 'TagA')], 50),
    });
    check('a FOUND round whose nextBeginOffset is below the asked offset warns',
      sanityLines(r).length === 1, JSON.stringify(r.warnLines));
    const n = numbersOf(sanityLines(r)[0] ?? '');
    check('the one WARN carries all three numbers it judged',
      n.next === 50 && n.first === 101 && n.prev === 100, JSON.stringify(n));
  }

  // ---- 2. firstMsg < prev while next >= prev: the second leg, alone
  {
    check('the helper flags a first row from before the ask',
      pullOffsetsLookWrong(200, 90, 100) === true);
    const r = await drive({
      group: 'GID_sanity_first_backwards', cursor: 100,
      first: found([row(90, 'TagA'), row(120, 'TagA'), row(121, 'TagA')], 130),
    });
    check('a FOUND round whose first row predates the asked offset warns',
      sanityLines(r).length === 1, JSON.stringify(r.warnLines));
    const n = numbersOf(sanityLines(r)[0] ?? '');
    check('naming next >= prev so the row leg is the one that fired',
      n.next === 130 && n.first === 90 && n.prev === 100, JSON.stringify(n));
  }

  // ---- 3. consistent: nothing to say
  {
    const r = await drive({
      group: 'GID_sanity_consistent', cursor: 100,
      first: found([row(100, 'TagA'), row(101, 'TagA')], 100),
    });
    check('a round that answers at and after the asked offset warns nothing',
      sanityLines(r).length === 0 && r.warnLines.length === 0,
      JSON.stringify(r.warnLines));
  }

  // ---- 4. an empty FOUND list: no first row, so the sentinel stands in
  {
    check('the sentinel keeps an empty round off the row leg',
      pullOffsetsLookWrong(150, NO_ROW, 100) === false);
    const quiet = await drive({ group: 'GID_sanity_found_empty', cursor: 100,
      first: found([], 150) });
    check('a FOUND round with an empty answer warns nothing',
      sanityLines(quiet).length === 0, JSON.stringify(quiet.warnLines));
    const loud = await drive({ group: 'GID_sanity_found_empty_backwards', cursor: 100,
      first: found([], 40) });
    check('while the same empty round with a backwards nextBeginOffset still warns',
      sanityLines(loud).length === 1, JSON.stringify(loud.warnLines));
    const n = numbersOf(sanityLines(loud)[0] ?? '');
    check('and reports the missing first row as the sentinel, not as a real offset',
      n.first === NO_ROW && n.next === 40 && n.prev === 100, JSON.stringify(n));
  }

  // ---- 5. firstMsgOffset is the first row AFTER this client's own filtering
  {
    // TagB at offset 90 is filtered out, so the round hands over rows from 101 up:
    // pre-filter reading would flag 90 < 100, post-filter reading must stay quiet.
    const filteredOut = await drive({
      group: 'GID_sanity_tag_drops_the_low_row', cursor: 100, expression: 'TagA',
      first: found([row(90, 'TagB'), row(101, 'TagA'), row(102, 'TagA')], 130),
    });
    check('a low row the tag pass removes does not warn',
      sanityLines(filteredOut).length === 0, JSON.stringify(filteredOut.warnLines));
    // Here the tag pass keeps the low row, so the round does warn.
    const kept = await drive({
      group: 'GID_sanity_tag_keeps_the_low_row', cursor: 100, expression: 'TagA',
      first: found([row(90, 'TagA'), row(101, 'TagB'), row(102, 'TagA')], 130),
    });
    check('a low row the tag pass keeps warns, at the surviving row',
      sanityLines(kept).length === 1 && numbersOf(sanityLines(kept)[0] ?? '').first === 90,
      JSON.stringify(kept.warnLines));
    // The hook rewrite counts too: dropping the low row silences the round.
    const hooked = await drive({
      group: 'GID_sanity_hook_drops_the_low_row', cursor: 100,
      hooks: [new DropFirstHook()],
      first: found([row(90, 'TagA'), row(101, 'TagA'), row(102, 'TagA')], 130),
    });
    check('a low row the FilterMessageHook removes does not warn either',
      sanityLines(hooked).length === 0, JSON.stringify(hooked.warnLines));
  }

  // ---- 6. only a FOUND round is judged
  {
    const noNew = await drive({
      group: 'GID_sanity_no_new', cursor: 100,
      first: new PullResult(PullStatus.NO_NEW_MSG, 40, 0, 1000, []),
    });
    check('a NO_NEW_MSG round with a backwards nextBeginOffset brings no sanity line',
      sanityLines(noNew).length === 0 && noNew.warnLines.length === 0,
      JSON.stringify(noNew.warnLines));
    const noMatched = await drive({
      group: 'GID_sanity_no_matched', cursor: 100,
      first: new PullResult(PullStatus.NO_MATCHED_MSG, 40, 0, 1000, []),
    });
    check('the same for NO_MATCHED_MSG',
      sanityLines(noMatched).length === 0, JSON.stringify(noMatched.warnLines));
    const illegal = await drive({
      group: 'GID_sanity_illegal', cursor: 100,
      first: new PullResult(PullStatus.OFFSET_ILLEGAL, 40, 0, 1000, []),
    });
    check('an OFFSET_ILLEGAL round keeps its own warning and adds no sanity line',
      sanityLines(illegal).length === 0 && illegal.warnLines.length === 1
      && illegal.warnLines[0].includes('offset illegal'), JSON.stringify(illegal.warnLines));
  }

  // ---- 7. diagnostic only: the firing round leaves cursor, pacing and stats alone
  {
    const fires = await drive({
      group: 'GID_sanity_effect_fires', cursor: 100,
      first: found([row(101, 'TagA'), row(102, 'TagA'), row(103, 'TagA')], 50),
    });
    const quiet = await drive({
      group: 'GID_sanity_effect_quiet', cursor: 100,
      first: found([row(101, 'TagA'), row(102, 'TagA'), row(103, 'TagA')], 130),
    });
    check('the firing round still advanced its cursor to the answer it got',
      fires.cursorAfter === 50, String(fires.cursorAfter));
    check('and makes exactly the pacing decision the quiet round makes',
      JSON.stringify(fires.waits) === JSON.stringify(quiet.waits)
      && fires.waits.length === 1 && fires.waits[0] === fires.pullInterval,
      `${JSON.stringify(fires.waits)}/${JSON.stringify(quiet.waits)}`);
    check('with the same rows handed over, so the consumer sees the same batch',
      fires.handed.length === 1 && quiet.handed.length === 1
      && fires.handed[0].length === quiet.handed[0].length && fires.handed[0].length === 3,
      `${fires.handed.map((h) => h.length)}/${quiet.handed.map((h) => h.length)}`);
    const fStats = (fires.c as any)._statsManager;
    const qStats = (quiet.c as any)._statsManager;
    const fRt = fStats.topicAndGroupPullRT.find(`${TOPIC}@GID_sanity_effect_fires`);
    const qRt = qStats.topicAndGroupPullRT.find(`${TOPIC}@GID_sanity_effect_quiet`);
    const fTps = fStats.topicAndGroupPullTPS.find(`${TOPIC}@GID_sanity_effect_fires`);
    const qTps = qStats.topicAndGroupPullTPS.find(`${TOPIC}@GID_sanity_effect_quiet`);
    check('and books the same PULL_RT and PULL_TPS as the quiet round',
      fRt != null && qRt != null && fRt.times === qRt.times && fRt.times === 1
      && fTps != null && qTps != null && fTps.value === qTps.value && fTps.value === 3,
      `rt=${fRt ? fRt.times : 'none'}/${qRt ? qRt.times : 'none'} `
      + `tps=${fTps ? fTps.value : 'none'}/${qTps ? qTps.value : 'none'}`);
    // The row leg, same effect: the warning changes nothing about the round that drew it.
    const rowLeg = await drive({
      group: 'GID_sanity_effect_row_leg', cursor: 100,
      first: found([row(90, 'TagA'), row(101, 'TagA')], 130),
    });
    check('the row-leg round still lands its cursor on nextBeginOffset and waits once',
      sanityLines(rowLeg).length === 1 && rowLeg.cursorAfter === 130
      && rowLeg.waits.length === 1 && rowLeg.waits[0] === rowLeg.pullInterval,
      `${JSON.stringify(rowLeg.waits)}/${rowLeg.cursorAfter}`);
  }

  console.log(`\npull result offset sanity: ${pass} checks, ${fail} failed`);
  if (fail > 0) process.exitCode = 1;
}

main().catch((e) => { console.error(e); process.exitCode = 1; });
