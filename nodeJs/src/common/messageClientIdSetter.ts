// -*- coding: utf-8 -*-
// Client-side unique message id generator (org.apache.rocketmq.common.message.MessageClientIDSetter).
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

const FIX_STRING = bytes2string(ipToBytes(getHostIp()));
const COUNTER = { value: 0 };
const COUNTER_MAX = 0x7fffffff;

export function createUniqID() {
  COUNTER.value = (COUNTER.value + 1) & COUNTER_MAX;
  const pid = (getPid() & 0xffff).toString(16).padStart(4, '0');
  const ts = Math.floor(Date.now() / 1000).toString(16).padStart(8, '0');
  const counter = COUNTER.value.toString(16).padStart(8, '0');
  return `${FIX_STRING}${pid}${ts}${counter}`;
}

export default { createUniqID };
