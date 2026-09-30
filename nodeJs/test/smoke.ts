// Smoke test for the protocol spine. Run: node --experimental-strip-types test/smoke.ts
import assert from 'node:assert';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { SerializeType } from '../src/remoting/codes.ts';
import { RocketMQSerializable, fastjsonLoads } from '../src/remoting/serialize.ts';
import { Message, MessageExt } from '../src/common/message.ts';
import { encodeMessageExt, decodeMessage, messageProperties2String, string2MessageProperties } from '../src/common/messageDecoder.ts';
import { signAcl } from '../src/remoting/acl.ts';

// 1) RemotingCommand JSON round-trip
{
  const cmd = RemotingCommand.createRequestCommand(10);
  cmd.addExtField('a', '1');
  cmd.addExtField('b', 'hello');
  cmd.remark = 'rm';
  cmd.body = Buffer.from('payload');
  const enc = cmd.encode();
  const dec = RemotingCommand.decode(enc);
  assert.strictEqual(dec.code, 10);
  assert.strictEqual(dec.getExtField('a'), '1');
  assert.strictEqual(dec.getExtField('b'), 'hello');
  assert.strictEqual(dec.remark, 'rm');
  assert.strictEqual(dec.body.toString(), 'payload');
  console.log('OK: RemotingCommand JSON round-trip');
}

// 2) RemotingCommand ROCKETMQ binary round-trip
{
  const cmd = new RemotingCommand(34, null, 'hb', null, 0, Buffer.from('x'));
  cmd.addExtField('topic', 'T');
  cmd.addExtField('n', '42');
  cmd.serializeTypeCurrentRpc = SerializeType.ROCKETMQ;
  const enc = cmd.encode();
  const dec = RemotingCommand.decode(enc);
  assert.strictEqual(dec.code, 34);
  assert.strictEqual(dec.remark, 'hb');
  assert.strictEqual(dec.getExtField('topic'), 'T');
  assert.strictEqual(dec.getExtField('n'), '42');
  assert.strictEqual(dec.body.toString(), 'x');
  console.log('OK: RemotingCommand ROCKETMQ binary round-trip');
}

// 3) fastjson2 tolerant parser (unquoted numeric map keys + inline object key)
{
  const s = '{"brokerAddrs":{0:"127.0.0.1:10911"}}';
  const obj = fastjsonLoads(s);
  assert.strictEqual(obj.brokerAddrs['0'], '127.0.0.1:10911');
  const s2 = '{"offsetTable":{{"brokerName":"b","queueId":3,"topic":"t"}:{}}}';
  const obj2 = fastjsonLoads(s2);
  assert.ok(obj2.offsetTable != null);
  console.log('OK: fastjson2 tolerant parser');
}

// 4) 17-segment message encode/decode round-trip
{
  const m = new MessageExt('TopicTest', Buffer.from('hello world'));
  m.setQueueId(2);
  m.setQueueOffset(100);
  m.setCommitLogOffset(200);
  m.setBornTimestamp(Date.now());
  m.setStoreTimestamp(Date.now());
  m.setBornHost('10.0.0.5');
  m.bornHostPort = 8888;
  m.setStoreHost('10.0.0.6');
  m.storeHostPort = 10911;
  m.setBodyCrc(12345);
  m.putProperty('KEYS', 'k1');
  m.putProperty('TAGS', 'tagA');
  const enc = encodeMessageExt(m);
  const dec = decodeMessage(enc);
  assert.ok(dec, 'decode returned null');
  assert.strictEqual(dec.getTopic(), 'TopicTest');
  assert.strictEqual(dec.getBody().toString(), 'hello world');
  assert.strictEqual(dec.getQueueId(), 2);
  assert.strictEqual(dec.getQueueOffset(), 100);
  assert.strictEqual(dec.getBornHostString(), '10.0.0.5:8888');
  assert.strictEqual(dec.getProperty('KEYS'), 'k1');
  console.log('OK: 17-segment message round-trip');
}

// 5) properties string round-trip
{
  const props = { KEYS: 'k', TAGS: 't', WAIT: 'true' };
  const s = messageProperties2String(props);
  const back = string2MessageProperties(s);
  assert.strictEqual(back.KEYS, 'k');
  assert.strictEqual(back.TAGS, 't');
  console.log('OK: properties string round-trip');
}

// 6) ACL sign produces a stable signature
{
  const ext: Record<string, string> = { a: '1', b: '2' };
  const sig1 = signAcl('ak', 'sk', ext, Buffer.from('body'));
  assert.ok(ext['Signature']);
  assert.ok(ext['AccessKey'] === 'ak');
  // replay determinism
  const ext2: Record<string, string> = { a: '1', b: '2' };
  const sig2 = signAcl('ak', 'sk', ext2, Buffer.from('body'));
  assert.strictEqual(sig1, sig2);
  console.log('OK: ACL sign deterministic');
}

console.log('\nAll smoke checks passed.');
