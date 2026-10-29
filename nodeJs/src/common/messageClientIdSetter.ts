import { getHostIp, getPid, bytes2string } from './utilAll.ts';

function ipToBytes(ip) {
  const parts = ip.split('.').map(Number);
  const b = Buffer.alloc(4);
  b.writeUInt8(parts[0] | 0, 0);
  b.writeUInt8(parts[1] | 0, 1);
  b.writeUInt8(parts[2] | 0, 2);
  b.writeUInt8(parts[3] | 0, 3);
  return b;
}

// The id is 16 raw bytes rendered as 32 uppercase hex chars:
//   IP (4) + pid (2) + class hash (4) + millis into the month (4) + counter (2).
const IP = ipToBytes(getHostIp());
const PID = Buffer.alloc(2);
PID.writeUInt16BE(getPid() & 0xffff, 0);
const CLASS_HASH = Buffer.alloc(4); // reserved slot, always 0

// Start instant of the elapsed-millis field: local start of the month this
// process was launched in.
function monthStartMillis(now: number): number {
  const d = new Date(now);
  return new Date(d.getFullYear(), d.getMonth(), 1, 0, 0, 0, 0).getTime();
}
const START_TIME = monthStartMillis(Date.now());

const COUNTER = { value: 0 };
const COUNTER_MAX = 0xffff;

export function createUniqID() {
  COUNTER.value = (COUNTER.value + 1) & COUNTER_MAX;
  const elapsed = Buffer.alloc(4);
  elapsed.writeUInt32BE((Date.now() - START_TIME) >>> 0, 0);
  const counter = Buffer.alloc(2);
  counter.writeUInt16BE(COUNTER.value, 0);
  return bytes2string(Buffer.concat([IP, PID, CLASS_HASH, elapsed, counter]));
}

export default { createUniqID };
