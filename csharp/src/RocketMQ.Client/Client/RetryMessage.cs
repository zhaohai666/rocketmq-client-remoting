using System.Globalization;

using RocketMQ.Common;

namespace RocketMQ.Client;

/// <summary>
/// Builds the normal message that carries a failed delivery to <c>%RETRY%&lt;group&gt;</c>:
/// body and properties copied, the origin id falling back to the message id, the retry
/// bookkeeping written, and the half-message marker cleared so the broker does not treat
/// the copy as a transaction check-back message all over again.
/// </summary>
internal static class RetryMessageFactory
{
    public static Message Build(string consumerGroup, MessageExt msg, int maxReconsumeTimes)
    {
        var newMsg = new Message(MixAll.GetRetryTopic(consumerGroup), msg.Body);
        newMsg.Properties = new PropertyMap(msg.Properties);
        newMsg.Flag = msg.Flag;
        string originMsgId = msg.GetProperty(MessageConst.PropertyOriginMessageId);
        if (string.IsNullOrEmpty(originMsgId))
        {
            originMsgId = msg.MsgId;
        }

        if (!string.IsNullOrEmpty(originMsgId))
        {
            newMsg.PutProperty(MessageConst.PropertyOriginMessageId, originMsgId);
        }

        newMsg.PutProperty(MessageConst.PropertyRetryTopic, msg.Topic);
        newMsg.PutProperty(MessageConst.PropertyReconsumeTime,
            (msg.ReconsumeTimes + 1).ToString(CultureInfo.InvariantCulture));
        newMsg.PutProperty(MessageConst.PropertyMaxReconsumeTimes,
            maxReconsumeTimes.ToString(CultureInfo.InvariantCulture));
        newMsg.RemoveProperty(MessageConst.PropertyTransactionPrepared);
        newMsg.DelayTimeLevel = 3 + msg.ReconsumeTimes;
        if (string.IsNullOrEmpty(newMsg.GetProperty(MessageConst.PropertyUniqClientMessageIdKeyidx)))
        {
            MessageClientIDSetter.SetUniqId(newMsg);
        }

        return newMsg;
    }
}
