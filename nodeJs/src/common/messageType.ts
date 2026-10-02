// -*- coding: utf-8 -*-
// MessageType enum (org.apache.rocketmq.common.message.MessageType / MessageConst.MSG_TYPE).
export const MessageType = {
  NORMAL: 'Normal_Msg',
  ORDERLY: 'Order_Msg',
  TRANSACTION: 'Trans_Msg_Half',
  DELAY: 'Delay_Msg',
  BATCH: 'Batch_Msg',
  REQUEST_REPLY: 'Request_Reply_Msg',
  REPLY: 'reply', // MixAll.REPLY_MESSAGE_FLAG — what a REPLY send carries
  TIMER: 'Timer_Msg',
};

export default MessageType;
