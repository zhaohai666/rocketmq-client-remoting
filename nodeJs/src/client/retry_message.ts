// The normal message that carries a failed delivery to %RETRY%<group>.
//
// Both send-back forms share this shape: the body and properties are copied,
// the origin id falls back to the message id, the retry bookkeeping is written,
// and the half-message marker is cleared so the broker does not treat the copy
// as a transaction check-back message all over again.
import { Message, MessageExt } from '../common/message.ts';
import { MessageConst } from '../common/messageConst.ts';
import { MixAll } from '../common/mixAll.ts';

export function buildRetryMessage(consumerGroup: string, msg: MessageExt, maxReconsumeTimes: number): Message {
  const newMsg = new Message(MixAll.getRetryTopic(consumerGroup), Buffer.from(msg.getBody() || Buffer.alloc(0)));
  newMsg.setProperties({ ...(msg.getProperties() || {}) });
  newMsg.setFlag(msg.getFlag());
  let originMsgID = msg.getMsgId();
  const orig = msg.getProperty(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID);
  if (orig) originMsgID = orig;
  if (originMsgID) newMsg.putProperty(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID, originMsgID);
  newMsg.putProperty(MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
  newMsg.putProperty(MessageConst.PROPERTY_RECONSUME_TIME, String(msg.getReconsumeTimes() + 1));
  newMsg.putProperty(MessageConst.PROPERTY_MAX_RECONSUME_TIMES, String(maxReconsumeTimes));
  newMsg.removeProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
  newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());
  return newMsg;
}
