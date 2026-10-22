// BODYCRC contract — offline test of the CRC field a message store frame carries.
//
// Baseline this suite pins:
//   * the 4-byte BODYCRC slot holds CRC-32/ISO-HDLC with the sign bit dropped, i.e.
//     the raw full-width CRC masked with 0x7fffffff — NOT the raw CRC itself;
//   * for a body whose raw CRC is below 2^31 the two numbers coincide, so a small
//     fixture proves nothing. The check must be exercised on a body whose raw CRC
//     HAS bit 31 set, where the two forms differ by exactly 2^31 and a full-width
//     comparison rejects a perfectly valid message;
//   * the comparison is opt-in through the last argument of
//     decodeMessage(raw, readBody, decompressBody, isClient, checkCrc), and a
//     rejected frame surfaces as a null return because the CRC error is caught
//     inside the decode.
//
// Run: node --experimental-strip-types test/body_crc_smoke.ts
import { Buffer } from 'node:buffer';
import { MessageExt } from '../src/common/message.ts';
import { encodeMessageExt, decodeMessage } from '../src/common/messageDecoder.ts';
import { crc32, crc32Match } from '../src/common/utilAll.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

// A body whose RAW crc32 has bit 31 set, so raw and wire forms differ. Short
// candidates are tried until one is found, then its high bit is asserted below —
// the fixture can never silently decay into a case that tests both forms alike.
const HIGH_BIT_BODY = 'crc-highbit-2';
const BODYCRC_OFFSET = 8; // after TOTALSIZE(4) + MAGICCODE(4)

function stampBodyCrc(frame: Buffer, bodyCrc: number): Buffer {
  const out = Buffer.from(frame);
  out.writeUInt32BE(bodyCrc >>> 0, BODYCRC_OFFSET);
  return out;
}

function buildFrame(topic: string, body: Buffer, bodyCrc: number): Buffer {
  const m = new MessageExt(topic, body);
  m.setQueueId(0);
  m.setQueueOffset(0);
  m.setCommitLogOffset(0);
  m.setBornTimestamp(1700000000000);
  m.setStoreTimestamp(1700000000000);
  m.setBornHost('127.0.0.1');
  m.bornHostPort = 10911;
  m.setStoreHost('127.0.0.1');
  m.storeHostPort = 10911;
  m.setReconsumeTimes(0);
  m.setFlag(0);
  m.setPreparedTransactionOffset(0);
  m.setBodyCrc(bodyCrc);
  return encodeMessageExt(m);
}

console.log('== crc32 / crc32Match: the two forms, pinned apart ==');
{
  // "a": raw CRC has bit 31 set -> raw and wire forms differ by exactly 2^31.
  const rawA = crc32(Buffer.from('a'));
  const wireA = crc32Match(Buffer.from('a'));
  check('crc32("a") is the raw full-width value 0xE8B7BE43', rawA === 0xE8B7BE43, `got 0x${rawA.toString(16)}`);
  check('crc32("a") carries bit 31: the raw form is above 2^31', rawA > 0x7fffffff);
  check('crc32Match("a") is the wire BODYCRC form 0x68B7BE43', wireA === 0x68B7BE43, `got 0x${wireA.toString(16)}`);
  check('the two forms are not interchangeable for that body',
    wireA !== rawA && rawA - wireA === 0x80000000, `raw=${rawA} wire=${wireA}`);

  // "abc": raw CRC is below 2^31 -> masking is a no-op, both forms coincide.
  const rawAbc = crc32(Buffer.from('abc'));
  check('crc32Match("abc") is 0x352441C2', crc32Match(Buffer.from('abc')) === 0x352441C2,
    `got 0x${crc32Match(Buffer.from('abc')).toString(16)}`);
  check('a CRC below 2^31 is unaffected by the mask (raw == wire form here)',
    rawAbc === 0x352441C2 && crc32Match(Buffer.from('abc')) === rawAbc);

  // The fixture itself must stay discriminating.
  const rawFixture = crc32(Buffer.from(HIGH_BIT_BODY));
  check(`fixture "${HIGH_BIT_BODY}" has a raw CRC with bit 31 set`,
    (rawFixture & 0x80000000) !== 0, `raw=0x${rawFixture.toString(16)}`);
  check('crc32Match accepts a string as well as a buffer',
    crc32Match(HIGH_BIT_BODY) === crc32Match(Buffer.from(HIGH_BIT_BODY)));
}

console.log('\n== decodeMessage(checkCrc) against the wire BODYCRC ==');
{
  const body = Buffer.from(HIGH_BIT_BODY);
  const wireCrc = crc32Match(body);
  const rawCrc = crc32(body);
  const frame = buildFrame('BodyCrcTopic', body, wireCrc);

  // A store frame carrying the masked CRC of a high-bit body is a VALID message.
  const dec = decodeMessage(frame, true, true, true, true);
  check('a frame stamped with the wire BODYCRC decodes with checkCrc enabled', dec != null,
    'decode returned null: the CRC check rejected a valid message');
  if (dec != null) {
    check('the body survives the check intact', dec.getBody().equals(body));
    check('the BODYCRC field round-trips through the frame unchanged', dec.getBodyCrc() === wireCrc,
      `expected ${wireCrc} got ${dec.getBodyCrc()}`);
  }

  // The same body stamped with the FULL-WIDTH CRC is not a frame any store writes:
  // for a high-bit body the stored field always has bit 31 cleared.
  check('a frame stamped with the full-width CRC is refused — the field is the masked form',
    decodeMessage(stampBodyCrc(frame, rawCrc), true, true, true, true) === null,
    `raw=0x${rawCrc.toString(16)} wire=0x${wireCrc.toString(16)}`);

  // Negative control: neither form matches, so the guard still fires.
  check('a deliberately wrong BODYCRC still throws the frame away',
    decodeMessage(stampBodyCrc(frame, (wireCrc ^ 0x5a5a5a5a) >>> 0), true, true, true, true) === null);
  check('a BODYCRC of 0 is refused for a non-empty body',
    decodeMessage(stampBodyCrc(frame, 0), true, true, true, true) === null);

  // The check stays opt-in: without it the same frames are readable either way.
  const uncheckedRaw = decodeMessage(stampBodyCrc(frame, rawCrc), true, true, true, false);
  const uncheckedWrong = decodeMessage(stampBodyCrc(frame, 12345), true, true, true, false);
  check('checkCrc off leaves the CRC comparison out of the decode',
    uncheckedRaw != null && uncheckedWrong != null && uncheckedWrong.getBodyCrc() === 12345);

  // A body whose CRC is below 2^31 must keep passing: the mask must not have
  // broken the case where the two forms already agreed.
  const lowBody = Buffer.from('abc');
  const lowFrame = buildFrame('BodyCrcLowTopic', lowBody, crc32Match(lowBody));
  check('a low-CRC body also decodes with checkCrc enabled',
    decodeMessage(lowFrame, true, true, true, true) != null);

  // Header-only decode never reads the body, so it cannot be the check's business.
  check('a header-only decode ignores the CRC entirely',
    decodeMessage(stampBodyCrc(frame, 999), false, true, true, true) != null);

  // The field the encoder writes is what the decoder reports — no recompute on read.
  const zeroCrcFrame = buildFrame('BodyCrcZeroTopic', Buffer.alloc(0), 0);
  const zeroDec = decodeMessage(zeroCrcFrame, true, true, true, true);
  check('an empty body with a 0 BODYCRC is a valid frame', zeroDec != null);
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
