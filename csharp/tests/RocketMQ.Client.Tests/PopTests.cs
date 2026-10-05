// POP 模式（5.x 轻量消费）单测 —— 纯逻辑，不联网。
//
// 覆盖：
// 1. ExtraInfoUtil 全函数：7/8 段 build+split 往返、**末尾空串被丢弃**（Java String.split 语义）、
//    长度下限校验、retry 判定三分支、getRealTopic 三分支；
// 2. startOffsetInfo / msgOffsetInfo / orderCountInfo 的解析与错误分支（段数≠3、重复 key）；
// 3. 5 个 POP 请求/响应头的 extFields **逐键断言**（broker 用 fastjson2 按 Java 属性名反序列化，
//    错一个字母就静默丢字段 —— 这是守卫）；
// 4. MQClientInstance.StampPopCk：POP_CK 反构（8 段、msgQueueOffset 取自体下标）、
//    已带 POP_CK 不覆盖、1ST_POP_TIME 只在缺失时补、startOffsetInfo 缺失时的降级分支。
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;
// 与 src 侧的 global using 保持一致（测试项目没有该全局别名）
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

public class PopTests
{
    private const string Topic = "PopTestTopic";
    private const string Group = "GID_PopTest";
    private const string Broker = "broker-a";
    private const long PopTime = 1789613086027L;

    private static MessageExt MakeMsg(string topic, int queueId, long queueOffset)
    {
        var m = new MessageExt(new Message(topic, new byte[] { 1, 2, 3 }))
        {
            QueueId = queueId,
            QueueOffset = queueOffset,
        };
        return m;
    }

    private static PopMessageResponseHeader MakeHeader(string startOffsetInfo, string msgOffsetInfo) =>
        new()
        {
            PopTime = PopTime,
            InvisibleTime = 60000,
            ReviveQid = 0,
            StartOffsetInfo = startOffsetInfo,
            MsgOffsetInfo = msgOffsetInfo,
        };

    // ------------------------------------------------ 1. extraInfo 拼装/切分

    [Fact]
    public void BuildExtraInfoWithMsgQueueOffsetHasEightSegments()
    {
        string ck = ExtraInfoUtil.BuildExtraInfo(0, PopTime, 60000, 0, Topic, Broker, 3, 7);
        Assert.Equal("0 1789613086027 60000 0 0 broker-a 3 7", ck);
        Assert.Equal(8, ExtraInfoUtil.Split(ck).Length);
    }

    [Fact]
    public void BuildExtraInfoWithoutMsgQueueOffsetHasSevenSegments()
    {
        string ck = ExtraInfoUtil.BuildExtraInfo(0, 1, 2, 3, Topic, Broker, 4);
        Assert.Equal("0 1 2 3 0 broker-a 4", ck);
        Assert.Equal(7, ExtraInfoUtil.Split(ck).Length);
    }

    [Theory]
    [InlineData("1 2 3 ")]
    [InlineData("1 2 3  ")]
    [InlineData("1 2 3")]
    public void SplitDropsTrailingEmptySegments(string input)
    {
        // Java String.split(" ") 的 limit=0 语义：末尾空串被丢弃。C# 的 Split(' ') 会保留，
        // 所以这里必须显式丢弃 —— 否则长度下限校验会与 Java 分歧。
        Assert.Equal(3, ExtraInfoUtil.Split(input).Length);
    }

    [Fact]
    public void SplitNullThrows() => Assert.Throws<ArgumentException>(() => ExtraInfoUtil.Split(null!));

    [Fact]
    public void GettersReadTheRightSegment()
    {
        // 11 22 33 44 1 broker-b 5 123456789
        string ck = "11 22 33 44 1 broker-b 5 123456789";
        string[] seg = ExtraInfoUtil.Split(ck);
        Assert.Equal(11, ExtraInfoUtil.GetCkQueueOffset(seg));
        Assert.Equal(22, ExtraInfoUtil.GetPopTime(seg));
        Assert.Equal(33, ExtraInfoUtil.GetInvisibleTime(seg));
        Assert.Equal(44, ExtraInfoUtil.GetReviveQid(seg));
        Assert.Equal("1", ExtraInfoUtil.GetRetry(seg));
        Assert.Equal("broker-b", ExtraInfoUtil.GetBrokerName(seg));
        Assert.Equal(5, ExtraInfoUtil.GetQueueId(seg));
        Assert.Equal(123456789, ExtraInfoUtil.GetQueueOffset(seg));
    }

    [Fact]
    public void GettersEnforceJavaLengthFloors()
    {
        string[] seg7 = ExtraInfoUtil.Split(ExtraInfoUtil.BuildExtraInfo(0, 1, 2, 3, Topic, Broker, 4));
        string[] seg6 = ExtraInfoUtil.Split("0 1 2 3 0 broker-a");

        Assert.Throws<ArgumentException>(() => ExtraInfoUtil.GetQueueOffset(seg7));
        Assert.Throws<ArgumentException>(() => ExtraInfoUtil.GetQueueId(seg6));
        Assert.Throws<ArgumentException>(() => ExtraInfoUtil.GetCkQueueOffset(Array.Empty<string>()));
    }

    [Fact]
    public void IsOrderOnlyForTheOrderReviveQid()
    {
        Assert.True(ExtraInfoUtil.IsOrder(ExtraInfoUtil.Split(
            ExtraInfoUtil.BuildExtraInfo(0, 1, 2, ExtraInfoUtil.PopOrderReviveQueue, Topic, Broker, 0))));
        Assert.False(ExtraInfoUtil.IsOrder(ExtraInfoUtil.Split(
            ExtraInfoUtil.BuildExtraInfo(0, 1, 2, 0, Topic, Broker, 0))));
    }

    [Fact]
    public void RetryOfTopicHasThreeBranches()
    {
        Assert.Equal(ExtraInfoUtil.NormalTopic, ExtraInfoUtil.RetryOfTopic(Topic));
        Assert.Equal(ExtraInfoUtil.RetryTopic, ExtraInfoUtil.RetryOfTopic(
            ExtraInfoUtil.BuildPopRetryTopicV1(Topic, Group)));
        Assert.Equal(ExtraInfoUtil.RetryTopicV2, ExtraInfoUtil.RetryOfTopic(
            ExtraInfoUtil.BuildPopRetryTopicV2(Topic, Group)));
    }

    [Fact]
    public void PopRetryTopicsAreSeparateFromPushRetryTopic()
    {
        // POP 的重投 topic 带 topic 后缀；push 的 %RETRY%<group> 不带 —— 两套互不混。
        Assert.Equal("%RETRY%" + Group + "_" + Topic,
            ExtraInfoUtil.BuildPopRetryTopicV1(Topic, Group));
        Assert.Equal("%RETRY%" + Group + "+" + Topic,
            ExtraInfoUtil.BuildPopRetryTopicV2(Topic, Group));
        Assert.NotEqual(MixAll.RetryGroupTopicPrefix + Group,
            ExtraInfoUtil.BuildPopRetryTopicV1(Topic, Group));
    }

    [Fact]
    public void GetRealTopicResolvesAllThreeRetryFlags()
    {
        Assert.Equal(Topic, ExtraInfoUtil.GetRealTopic(Topic, Group, "0"));
        Assert.Equal(ExtraInfoUtil.BuildPopRetryTopicV1(Topic, Group),
            ExtraInfoUtil.GetRealTopic(Topic, Group, "1"));
        Assert.Equal(ExtraInfoUtil.BuildPopRetryTopicV2(Topic, Group),
            ExtraInfoUtil.GetRealTopic(Topic, Group, "2"));
        Assert.Throws<ArgumentException>(() => ExtraInfoUtil.GetRealTopic(Topic, Group, "9"));
    }

    // ------------------------------------------------ 解析响应头编码

    [Fact]
    public void ParseStartOffsetInfoSplitsQueues()
    {
        Dictionary<string, long>? start = ExtraInfoUtil.ParseStartOffsetInfo("0 0 0;0 3 0;0 2 0");
        Assert.NotNull(start);
        Assert.Equal(3, start!.Count);
        Assert.Equal(0, start["0@0"]);
        Assert.Equal(0, start["0@3"]);
        Assert.Equal(0, start["0@2"]);

        // 只有一段时不该被 ";" 切坏
        Dictionary<string, long>? single = ExtraInfoUtil.ParseStartOffsetInfo("0 3 5");
        Assert.NotNull(single);
        Assert.Equal(5, single!["0@3"]);
    }

    [Fact]
    public void ParseMsgOffsetInfoAndOrderCountInfo()
    {
        Dictionary<string, List<long>>? msg = ExtraInfoUtil.ParseMsgOffsetInfo("0 3 0,1,2");
        Assert.NotNull(msg);
        Assert.Equal(3, msg!["0@3"].Count);
        Assert.Equal(2, msg["0@3"][2]);

        Dictionary<string, int>? order = ExtraInfoUtil.ParseOrderCountInfo("0 3 5");
        Assert.NotNull(order);
        Assert.Equal(5, order!["0@3"]);
    }

    [Fact]
    public void ParseInfoReturnsNullForEmptyInput()
    {
        Assert.Null(ExtraInfoUtil.ParseStartOffsetInfo(""));
        Assert.Null(ExtraInfoUtil.ParseMsgOffsetInfo(""));
        Assert.Null(ExtraInfoUtil.ParseOrderCountInfo(""));
        Assert.Null(ExtraInfoUtil.ParseStartOffsetInfo(null));
    }

    [Fact]
    public void ParseInfoRejectsWrongColumnCountAndDuplicates()
    {
        Assert.Throws<ArgumentException>(() => ExtraInfoUtil.ParseStartOffsetInfo("0 3"));
        Assert.Throws<ArgumentException>(() => ExtraInfoUtil.ParseStartOffsetInfo("0 3 0 9"));
        Assert.Throws<ArgumentException>(() => ExtraInfoUtil.ParseStartOffsetInfo("0 3 0;0 3 1"));
    }

    [Fact]
    public void MapKeysMatchJavaFormat()
    {
        Assert.Equal("0@3", ExtraInfoUtil.GetStartOffsetInfoMapKey(Topic, 3));
        Assert.Equal("qo3%7", ExtraInfoUtil.GetQueueOffsetKeyValueKey(3, 7));
        Assert.Equal("0@qo3%7", ExtraInfoUtil.GetQueueOffsetMapKey(Topic, 3, 7));

        // 带 popCk 的重载用 CK 第 5 段当 retryFlag（broker 可能改写 topic，不能只看 topic）
        string retryCk = ExtraInfoUtil.BuildExtraInfo(0, 1, 2, 0,
            ExtraInfoUtil.BuildPopRetryTopicV1(Topic, Group), Broker, 3);
        Assert.Equal("1@3", ExtraInfoUtil.GetStartOffsetInfoMapKey(Topic, retryCk, 3));
    }

    // ------------------------------------------------ 2. 请求/响应头 extFields 键名

    [Fact]
    public void PopMessageRequestHeaderUsesJavaFieldNamesVerbatim()
    {
        var h = new PopMessageRequestHeader
        {
            ConsumerGroup = Group,
            Topic = Topic,
            QueueId = -1,
            MaxMsgNums = 32,
            InvisibleTime = 60000,
            PollTime = 0,
            BornTime = PopTime,
            InitMode = 0,
            ExpType = "TAG",
            Exp = "*",
            AttemptId = "attempt-1",
        };
        PropertyMap ext = h.ToExtFields();

        // 12 个字段全在（order 是 primitive boolean，总出现）
        string[] expected =
        {
            "consumerGroup", "topic", "queueId", "maxMsgNums", "invisibleTime", "pollTime",
            "bornTime", "initMode", "expType", "exp", "order", "attemptId",
        };
        Assert.Equal(12, ext.Count);
        foreach (string k in expected)
        {
            Assert.True(ext.ContainsKey(k), "PopMessageRequestHeader missing key: " + k);
        }

        Assert.Equal("-1", ext["queueId"]);
        Assert.Equal("0", ext["pollTime"]);
        Assert.Equal(PopTime.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ext["bornTime"]);
        Assert.Equal("false", ext["order"]);
    }

    [Fact]
    public void PopMessageRequestHeaderOmitsUnsetOptionalFields()
    {
        var h = new PopMessageRequestHeader { ConsumerGroup = Group, Topic = Topic };
        PropertyMap ext = h.ToExtFields();
        Assert.False(ext.ContainsKey("queueId"));
        Assert.False(ext.ContainsKey("attemptId"));
        Assert.False(ext.ContainsKey("bornTime"));

        // order 是 primitive，未被赋值也要出现
        Assert.True(ext.ContainsKey("order"));
        Assert.Equal("false", ext["order"]);
    }

    [Fact]
    public void PopMessageResponseHeaderParsesAllFields()
    {
        var h = new PopMessageResponseHeader();
        h.FromExtFields(new PropertyMap
        {
            ["popTime"] = PopTime.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["invisibleTime"] = "60000",
            ["reviveQid"] = "3",
            ["restNum"] = "9",
            ["startOffsetInfo"] = "0 0 0;0 3 0;0 2 0",
            ["msgOffsetInfo"] = "0 0 5;0 3 6;0 2 7",
            ["orderCountInfo"] = "0 3 2",
        });

        Assert.Equal(PopTime, h.PopTime);
        Assert.Equal(60000, h.InvisibleTime);
        Assert.Equal(3, h.ReviveQid);
        Assert.Equal(9, h.RestNum);
        Assert.Equal("0 0 0;0 3 0;0 2 0", h.StartOffsetInfo);
        Assert.Equal("0 0 5;0 3 6;0 2 7", h.MsgOffsetInfo);
        Assert.Equal("0 3 2", h.OrderCountInfo);
    }

    [Fact]
    public void PopMessageResponseHeaderLeavesMissingFieldsNull()
    {
        var h = new PopMessageResponseHeader();
        h.FromExtFields(new PropertyMap());
        Assert.Null(h.PopTime);
        Assert.Null(h.InvisibleTime);
        Assert.Null(h.ReviveQid);
        Assert.Null(h.StartOffsetInfo);
    }

    [Fact]
    public void AckMessageRequestHeaderUsesJavaFieldNames()
    {
        var h = new AckMessageRequestHeader
        {
            ConsumerGroup = Group,
            Topic = Topic,
            QueueId = 3,
            ExtraInfo = "ck",
            Offset = 7,
        };
        PropertyMap ext = h.ToExtFields();

        Assert.Equal(5, ext.Count);
        Assert.Equal("ck", ext["extraInfo"]);
        Assert.Equal("7", ext["offset"]);
        Assert.Equal("3", ext["queueId"]);
        Assert.Equal(Group, ext["consumerGroup"]);
        Assert.Equal(Topic, ext["topic"]);
    }

    [Fact]
    public void ChangeInvisibleTimeHeadersUseJavaFieldNames()
    {
        var req = new ChangeInvisibleTimeRequestHeader
        {
            ConsumerGroup = Group,
            Topic = Topic,
            QueueId = 3,
            ExtraInfo = "ck",
            Offset = 7,
            InvisibleTime = 20000,
        };
        PropertyMap ext = req.ToExtFields();
        string[] expected =
        {
            "consumerGroup", "topic", "queueId", "extraInfo", "offset", "invisibleTime",
            "suspend",
        };
        Assert.Equal(7, ext.Count);
        foreach (string k in expected)
        {
            Assert.True(ext.ContainsKey(k), "ChangeInvisibleTimeRequestHeader missing key: " + k);
        }

        // Java 是 primitive boolean，总是出现且小写
        Assert.Equal("false", ext["suspend"]);
        Assert.Equal("20000", ext["invisibleTime"]);

        var resp = new ChangeInvisibleTimeResponseHeader();
        resp.FromExtFields(new PropertyMap
        {
            ["popTime"] = "111",
            ["invisibleTime"] = "222",
            ["reviveQid"] = "3",
        });
        Assert.Equal(111, resp.PopTime);
        Assert.Equal(222, resp.InvisibleTime);
        Assert.Equal(3, resp.ReviveQid);
    }

    // ------------------------------------------------ 3. POP_CK 反构

    [Fact]
    public void StampPopCkBuildsEightSegmentsMatchingTheMessage()
    {
        var msgs = new List<MessageExt>
        {
            MakeMsg(Topic, 0, 0),
            MakeMsg(Topic, 3, 0),
            MakeMsg(Topic, 2, 0),
        };
        MQClientInstance.StampPopCk(msgs, Broker, MakeHeader("0 0 0;0 3 0;0 2 0", "0 0 5;0 3 6;0 2 7"));

        foreach (MessageExt m in msgs)
        {
            Assert.True(m.Properties.ContainsKey(MessageConst.PropertyPopCk),
                "POP_CK missing on queue " + m.QueueId);

            string[] seg = ExtraInfoUtil.Split(m.Properties[MessageConst.PropertyPopCk]);
            Assert.Equal(8, seg.Length);
            Assert.Equal(Broker, ExtraInfoUtil.GetBrokerName(seg));
            Assert.Equal(m.QueueId, ExtraInfoUtil.GetQueueId(seg));

            // msgQueueOffset 来自 msgOffsetInfo，不是消息自身的 queueOffset
            long want = m.QueueId switch { 0 => 5, 3 => 6, _ => 7 };
            Assert.Equal(want, ExtraInfoUtil.GetQueueOffset(seg));
        }
    }

    [Fact]
    public void StampPopCkTakesCkQueueOffsetFromStartOffsetInfo()
    {
        var msgs = new List<MessageExt> { MakeMsg(Topic, 3, 0) };
        MQClientInstance.StampPopCk(msgs, Broker, MakeHeader("0 3 99", "0 3 0"));

        string[] seg = ExtraInfoUtil.Split(msgs[0].Properties[MessageConst.PropertyPopCk]);
        Assert.Equal(99, ExtraInfoUtil.GetCkQueueOffset(seg));
    }

    [Fact]
    public void StampPopCkDoesNotOverrideExistingCk()
    {
        // retry topic 弹回来的消息 broker 已经写好 POP_CK（retryFlag=1），绝不能覆盖。
        const string existing = "1 1 2 0 1 broker-a 0 0";
        var msgs = new List<MessageExt> { MakeMsg(Topic, 3, 0) };
        msgs[0].Properties[MessageConst.PropertyPopCk] = existing;

        MQClientInstance.StampPopCk(msgs, Broker, MakeHeader("0 3 0", "0 3 6"));

        Assert.Equal(existing, msgs[0].Properties[MessageConst.PropertyPopCk]);
    }

    [Fact]
    public void StampPopCkIndexSelectsRightOffsetWithinQueue()
    {
        // 同队列 3 条：下标是在**本批该队列的 queueOffset 排序表**里求，不是直接在
        // msgOffsetInfo 里按值找 —— 所以这里故意让 msgOffsetInfo 的值与消息自身的
        // queueOffset 不同（100/101/102 vs 10/11/12）。若按值找会一条都盖不上 POP_CK。
        var msgs = new List<MessageExt>
        {
            MakeMsg(Topic, 3, 10),
            MakeMsg(Topic, 3, 11),
            MakeMsg(Topic, 3, 12),
        };
        MQClientInstance.StampPopCk(msgs, Broker, MakeHeader("0 3 0", "0 3 100,101,102"));

        long[] want = { 100, 101, 102 };
        for (int i = 0; i < msgs.Count; i++)
        {
            Assert.True(msgs[i].Properties.ContainsKey(MessageConst.PropertyPopCk),
                "POP_CK missing on index " + i);
            string[] seg = ExtraInfoUtil.Split(msgs[i].Properties[MessageConst.PropertyPopCk]);
            Assert.Equal(want[i], ExtraInfoUtil.GetQueueOffset(seg));
        }
    }

    [Fact]
    public void StampPopCkFallsBackWhenStartOffsetInfoMissing()
    {
        var msgs = new List<MessageExt> { MakeMsg(Topic, 3, 42) };
        MQClientInstance.StampPopCk(msgs, Broker, MakeHeader(string.Empty, string.Empty));

        string[] seg = ExtraInfoUtil.Split(msgs[0].Properties[MessageConst.PropertyPopCk]);
        Assert.Equal(8, seg.Length);
        // 降级分支里 ckQueueOffset 与自己那条的 msgQueueOffset 都用消息自身 queueOffset
        Assert.Equal(42, ExtraInfoUtil.GetCkQueueOffset(seg));
        Assert.Equal(42, ExtraInfoUtil.GetQueueOffset(seg));
    }

    [Fact]
    public void StampPopCkFillsFirstPopTimeOnlyWhenMissing()
    {
        var msgs = new List<MessageExt> { MakeMsg(Topic, 0, 0), MakeMsg(Topic, 3, 0) };
        msgs[1].Properties[MessageConst.PropertyFirstPopTime] = "111";

        MQClientInstance.StampPopCk(msgs, Broker, MakeHeader("0 0 0;0 3 0", "0 0 0;0 3 0"));

        Assert.Equal(PopTime.ToString(System.Globalization.CultureInfo.InvariantCulture),
            msgs[0].Properties[MessageConst.PropertyFirstPopTime]);
        Assert.Equal("111", msgs[1].Properties[MessageConst.PropertyFirstPopTime]);
    }

    // ------------------------------------------------ 4. 状态名

    [Fact]
    public void PopStatusNamesMatchJava()
    {
        Assert.Equal("FOUND", PopStatusNames.Name(PopStatus.Found));
        Assert.Equal("NO_NEW_MSG", PopStatusNames.Name(PopStatus.NoNewMsg));
        Assert.Equal("POLLING_FULL", PopStatusNames.Name(PopStatus.PollingFull));
        Assert.Equal("POLLING_NOT_FOUND", PopStatusNames.Name(PopStatus.PollingNotFound));
    }

    [Fact]
    public void ChangeInvisibleTimeResultSuccessFollowsResponseCode()
    {
        Assert.True(new ChangeInvisibleTimeResult { Code = ResponseCode.Success }.Success);
        Assert.False(new ChangeInvisibleTimeResult { Code = ResponseCode.SystemError }.Success);
    }
}
