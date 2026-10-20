// Wire-level proof of the POP batch acknowledge primitive.
//
// A batch ack is worth exactly one round trip, and everything that decides that round trip is
// *inside the request body*: which checkpoints collapse into one entry, which one of them lends
// the entry its fields, and which bit of the packed payload stands for which message. None of it
// is visible in the client's own bookkeeping, and a broker that simply revives the message after
// invisibleTime expires turns a wrong bit index into "the consumer looks fine but the queue keeps
// redelivering" -- invisible on a machine whose observation window is shorter than invisibleTime.
// So every claim here is read back off the frame the loopback broker recorded.
//
// Each positive assertion is paired with a control that pins the other side of the same rule:
// a component that does NOT belong to the grouping key must not split an entry, a checkpoint that
// is a duplicate must not overwrite the entry that already exists, and the single-message ack must
// stay a separate request code with a custom header of its own.
using System.Globalization;
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class PopBatchAckWireTests
{
    private const string Group = "GID_BatchAckWire";
    private const string Topic = "BatchAckWireTopic";

    /// <summary>The broker the shared fake cluster advertises for its single queue.</summary>
    private const string Broker = "broker-0";

    private const string ClientId = "127.0.0.1@BatchAckWire#1";

    // ---------------------------------------------------------------- Fixture

    /// <summary>Hand-assembles the 8-segment checkpoint string: every component the grouping
    /// rule reads has to be settable on its own, which the topic-derived builder cannot do.</summary>
    private static string Ck(long ckQueueOffset, long popTime, long invisibleTime, int reviveQid,
        string retry, string brokerName, int queueId, long queueOffset) =>
        string.Format(CultureInfo.InvariantCulture, "{0} {1} {2} {3} {4} {5} {6} {7}",
            ckQueueOffset, popTime, invisibleTime, reviveQid, retry, brokerName, queueId, queueOffset);

    private static string Ck(long ckQueueOffset, long popTime, int queueId, long queueOffset) =>
        Ck(ckQueueOffset, popTime, 60000, 0, "0", Broker, queueId, queueOffset);

    /// <summary>An instance whose route really comes from the fake name server, so the address
    /// lookup the primitive performs on its own is exercised too.</summary>
    private static MQClientInstance Seeded(MockCluster cluster)
    {
        var instance = new MQClientInstance(ClientId, new List<string> { cluster.NamesrvAddr });
        Assert.True(instance.UpdateTopicRouteInfoFromNameServer(Topic), "the route must enter the cache");
        return instance;
    }

    /// <summary>Sends one batch ack and returns the broker response code. The primitive is
    /// synchronous, so the frame is already recorded when it returns -- nothing waits on a sleep.
    /// The address is left to the route lookup unless the case pins one.</summary>
    private static int RunBatchAck(MockCluster cluster, IReadOnlyList<string> checkpoints)
    {
        MQClientInstance instance = Seeded(cluster);
        try
        {
            return instance.BatchAckMessage(Group, Topic, checkpoints, 3000);
        }
        finally
        {
            instance.Shutdown();
        }
    }

    private static int RunAck(MockCluster cluster, string extraInfo, long offset)
    {
        MQClientInstance instance = Seeded(cluster);
        try
        {
            return instance.AckMessage(Group, Topic, 0, extraInfo, offset, 3000);
        }
        finally
        {
            instance.Shutdown();
        }
    }

    /// <summary>The one batch request the fake recorded. Asserting the count here is what makes
    /// "a batch costs one round trip" a measured fact rather than a claim.</summary>
    private static WireRecord SingleBatchRequest(MockCluster cluster)
    {
        List<WireRecord> hits = cluster.Records().FindAll(r => r.Code == RequestCode.BatchAckMessage);
        return Assert.Single(hits);
    }

    private static JsonValue RecordedBody(WireRecord hit)
    {
        Assert.True(hit.HasBody, "a batch ack carries its entries in the request body");
        Assert.NotEmpty(hit.Body);
        Assert.True(Json.TryParse(Encoding.UTF8.GetString(hit.Body), out JsonValue body, out string? error),
            "the recorded body must parse: " + error);
        return body;
    }

    private static JsonValue RecordedAcks(MockCluster cluster, out JsonValue body)
    {
        body = RecordedBody(SingleBatchRequest(cluster));
        Assert.Equal(new[] { "brokerName", "acks" }, Keys(body));
        JsonValue acks = body.Get("acks");
        Assert.True(acks.IsArray, "acks must be a JSON array");
        return acks;
    }

    private static string[] Keys(JsonValue v) =>
        v.ObjectItems().Select(kv => kv.Key).ToArray();

    /// <summary>The packed payload of one entry, decoded back into bit indexes.</summary>
    private static int[] BitsOf(JsonValue entry)
    {
        string encoded = entry.Get("b").StringValue();
        Assert.True(encoded.Length > 0, "an entry that acks nothing must never be sent");
        byte[] packed = Convert.FromBase64String(encoded);
        var indexes = new List<int>();
        for (int i = 0; i < packed.Length; ++i)
        {
            for (int bit = 0; bit < 8; ++bit)
            {
                if ((packed[i] & (1 << bit)) != 0)
                {
                    indexes.Add(i * 8 + bit);
                }
            }
        }

        return indexes.ToArray();
    }

    // ---------------------------------------------------------------- (1) one round trip, one code

    [Fact]
    public void BatchAck_SendsTheBatchRequestCodeOnceForEveryCheckpoints()
    {
        using var cluster = MockCluster.Start(1);
        int code = RunBatchAck(cluster, new List<string>
        {
            Ck(100, 1700, 3, 100),
            Ck(100, 1700, 3, 101),
            Ck(100, 1700, 3, 102),
        });

        Assert.Equal(ResponseCode.Success, code);
        Assert.Equal(1, cluster.CountRequests(RequestCode.BatchAckMessage));
        // The batch path must not fall back to one single-message ack per checkpoint
        Assert.Equal(0, cluster.CountRequests(RequestCode.AckMessage));
    }

    /// <summary>Control for the case above: the single-message primitive really does cost one
    /// request per message and never touches the batch code, so "one request" above is a saving
    /// and not two primitives pointing at the same code.</summary>
    [Fact]
    public void SingleAck_SendsTheAckRequestCodeOncePerCheckpoint()
    {
        using var cluster = MockCluster.Start(1);
        List<string> checkpoints = new()
        {
            Ck(100, 1700, 3, 100),
            Ck(100, 1700, 3, 101),
            Ck(100, 1700, 3, 102),
        };

        Assert.Equal(3, checkpoints.Count);
        foreach (string ck in checkpoints)
        {
            long offset = ExtraInfoUtil.GetQueueOffset(ExtraInfoUtil.Split(ck));
            Assert.Equal(ResponseCode.Success, RunAck(cluster, ck, offset));
        }

        Assert.Equal(3, cluster.CountRequests(RequestCode.AckMessage));
        Assert.Equal(0, cluster.CountRequests(RequestCode.BatchAckMessage));
    }

    // ---------------------------------------------------------------- (2) no custom header

    /// <summary>The batch request is built with a null header: every field the broker needs lives
    /// in the body, so no extField may leak onto the wire.</summary>
    [Fact]
    public void BatchAck_RequestsHasNoCustomHeader()
    {
        using var cluster = MockCluster.Start(1);
        RunBatchAck(cluster, new List<string> { Ck(100, 1700, 3, 100) });

        WireRecord hit = SingleBatchRequest(cluster);
        Assert.Empty(hit.Ext);

        JsonValue body = RecordedBody(hit);
        // ... while the group, the topic and the checkpoint all appear in the body instead
        JsonValue entry = body.Get("acks").At(0);
        Assert.Equal(Group, entry.Get("c").StringValue());
        Assert.Equal(Topic, entry.Get("t").StringValue());
        Assert.Equal(100, entry.Get("so").IntValue());
    }

    /// <summary>Control for the case above: the single ack does have a custom header, and its
    /// five fields are exactly what the batch request moved into the body.</summary>
    [Fact]
    public void SingleAck_RequestCarriesItsHeaderAsExtFields()
    {
        using var cluster = MockCluster.Start(1);
        string ck = Ck(100, 1700, 3, 104);
        RunAck(cluster, ck, 104);

        WireRecord hit = Assert.Single(
            cluster.Records().FindAll(r => r.Code == RequestCode.AckMessage));
        Assert.Equal(Group, hit.Ext["consumerGroup"]);
        Assert.Equal(Topic, hit.Ext["topic"]);
        Assert.Equal("0", hit.Ext["queueId"]);
        Assert.Equal(ck, hit.Ext["extraInfo"]);
        Assert.Equal("104", hit.Ext["offset"]);
    }

    // ---------------------------------------------------------------- (3) the grouping key

    /// <summary>Two checkpoints whose grouping key matches collapse into one entry that carries
    /// both bits.</summary>
    [Fact]
    public void BatchAck_CheckpointsSharingTheGroupingKeyCollapseIntoOneEntry()
    {
        using var cluster = MockCluster.Start(1);
        RunBatchAck(cluster, new List<string>
        {
            Ck(100, 1700, 3, 100),
            Ck(100, 1700, 3, 101),
            Ck(100, 1700, 3, 103),
        });

        JsonValue acks = RecordedAcks(cluster, out _);
        Assert.Equal(1, acks.Size());
        JsonValue entry = acks.At(0);
        Assert.Equal(new[] { 0, 1, 3 }, BitsOf(entry));
        Assert.Equal("Cw==", entry.Get("b").StringValue());
    }

    /// <summary>The grouping key is exactly retry / queueId / start offset / pop time: a
    /// checkpoint that differs in one of those must become its own entry, and one that differs
    /// only in the two fields outside the key (revive queue, invisible time) must not.</summary>
    [Theory]
    [InlineData("retry", 2)]
    [InlineData("queueId", 2)]
    [InlineData("startOffset", 2)]
    [InlineData("popTime", 2)]
    [InlineData("reviveQid", 1)]
    [InlineData("invisibleTime", 1)]
    public void BatchAck_GroupingKeyHasFourComponentsAndOnlyThose(string component, int expectedEntries)
    {
        using var cluster = MockCluster.Start(1);
        string first = Ck(100, 1700, 60000, 7, "0", Broker, 3, 100);
        string second = component switch
        {
            "retry" => Ck(100, 1700, 60000, 7, "1", Broker, 3, 101),
            "queueId" => Ck(100, 1700, 60000, 7, "0", Broker, 4, 101),
            "startOffset" => Ck(200, 1700, 60000, 7, "0", Broker, 3, 201),
            "popTime" => Ck(100, 1800, 60000, 7, "0", Broker, 3, 101),
            "reviveQid" => Ck(100, 1700, 60000, 9, "0", Broker, 3, 101),
            _ => Ck(100, 1700, 120000, 7, "0", Broker, 3, 101),
        };

        RunBatchAck(cluster, new List<string> { first, second });

        JsonValue acks = RecordedAcks(cluster, out _);
        Assert.Equal(expectedEntries, acks.Size());

        JsonValue entry = acks.At(0);
        Assert.Equal(100, entry.Get("so").IntValue());
        if (expectedEntries == 1)
        {
            // Collapsed: the entry keeps the components of the checkpoint that created it
            Assert.Equal(new[] { 0, 1 }, BitsOf(entry));
            Assert.Equal(7, entry.Get("rq").IntValue());
            Assert.Equal(60000, entry.Get("it").IntValue());
        }
        else
        {
            // Split: one entry per checkpoint, and the two differ in the whole key -- no
            // checkpoint may be silently merged away, and no bit may be counted from the
            // other entry's start offset.
            JsonValue other = acks.At(1);
            Assert.Equal(new[] { 0 }, BitsOf(entry));
            Assert.Equal(new[] { 1 }, BitsOf(other));
            Assert.Equal(Group, other.Get("c").StringValue());
            Assert.Equal(Topic, other.Get("t").StringValue());
            Assert.NotEqual((entry.Get("r").StringValue(), entry.Get("q").IntValue(),
                             entry.Get("so").IntValue(), entry.Get("pt").IntValue()),
                            (other.Get("r").StringValue(), other.Get("q").IntValue(),
                             other.Get("so").IntValue(), other.Get("pt").IntValue()));
        }
    }

    /// <summary>A repeated grouping key only contributes its bit; the entry keeps the fields of
    /// the checkpoint that created it, whichever one that was.</summary>
    [Fact]
    public void BatchAck_EntryFieldsComeFromTheCheckpointThatCreatedIt()
    {
        using var cluster = MockCluster.Start(1);
        RunBatchAck(cluster, new List<string>
        {
            Ck(100, 1700, 90000, 7, "0", Broker, 3, 100),
            Ck(100, 1700, 60000, 9, "0", Broker, 3, 101),
        });

        JsonValue acks = RecordedAcks(cluster, out _);
        Assert.Equal(1, acks.Size());
        Assert.Equal(90000, acks.At(0).Get("it").IntValue());
        Assert.Equal(7, acks.At(0).Get("rq").IntValue());

        // The mirror image: swapping the two checkpoints swaps which one lends the fields, so the
        // assertion above cannot be satisfied by a constant.
        using var swapped = MockCluster.Start(1);
        RunBatchAck(swapped, new List<string>
        {
            Ck(100, 1700, 60000, 9, "0", Broker, 3, 101),
            Ck(100, 1700, 90000, 7, "0", Broker, 3, 100),
        });

        JsonValue swappedAcks = RecordedAcks(swapped, out _);
        Assert.Equal(1, swappedAcks.Size());
        Assert.Equal(60000, swappedAcks.At(0).Get("it").IntValue());
        Assert.Equal(9, swappedAcks.At(0).Get("rq").IntValue());
        Assert.Equal(new[] { 0, 1 }, BitsOf(swappedAcks.At(0)));
    }

    /// <summary>Entries are emitted in the order the checkpoints were first seen, so the same
    /// list always produces the same frame.</summary>
    [Fact]
    public void BatchAck_EntriesFollowFirstSeenOrder()
    {
        using var cluster = MockCluster.Start(1);
        RunBatchAck(cluster, new List<string>
        {
            Ck(100, 1700, 3, 100),
            Ck(100, 1700, 4, 200),
            Ck(100, 1700, 3, 101),
        });

        JsonValue acks = RecordedAcks(cluster, out _);
        Assert.Equal(2, acks.Size());
        Assert.Equal(3, acks.At(0).Get("q").IntValue());
        Assert.Equal(4, acks.At(1).Get("q").IntValue());

        using var reversed = MockCluster.Start(1);
        RunBatchAck(reversed, new List<string>
        {
            Ck(100, 1700, 4, 200),
            Ck(100, 1700, 3, 100),
            Ck(100, 1700, 3, 101),
        });

        JsonValue reversedAcks = RecordedAcks(reversed, out _);
        Assert.Equal(4, reversedAcks.At(0).Get("q").IntValue());
        Assert.Equal(3, reversedAcks.At(1).Get("q").IntValue());
    }

    // ---------------------------------------------------------------- (4) the bit index

    /// <summary>The bit index is measured from the entry's start offset, never from the raw
    /// consume-queue offset: index 7 and 8 need two bytes, the second one holding bit 0.</summary>
    [Fact]
    public void BatchAck_BitIndexCountsFromTheEntryStartOffset()
    {
        using var cluster = MockCluster.Start(1);
        RunBatchAck(cluster, new List<string>
        {
            Ck(100, 1700, 3, 107),
            Ck(100, 1700, 3, 108),
        });

        JsonValue entry = RecordedAcks(cluster, out _).At(0);
        Assert.Equal(new[] { 7, 8 }, BitsOf(entry));
        Assert.Equal("gAE=", entry.Get("b").StringValue());
        Assert.Equal(2, Convert.FromBase64String(entry.Get("b").StringValue()).Length);
    }

    /// <summary>A payload holds exactly the bytes up to the highest set bit: no trailing zero byte
    /// and no fixed-width padding, so one acked message at index 0 is one byte.</summary>
    [Fact]
    public void BatchAck_PayloadStopsAtTheByteHoldingTheHighestBit()
    {
        using var cluster = MockCluster.Start(1);
        RunBatchAck(cluster, new List<string> { Ck(100, 1700, 3, 100) });

        JsonValue entry = RecordedAcks(cluster, out _).At(0);
        Assert.Equal("AQ==", entry.Get("b").StringValue());
        Assert.Single(Convert.FromBase64String(entry.Get("b").StringValue()));

        // A gap inside the window keeps its zero byte: index 0 and index 9 span two bytes.
        using var gapped = MockCluster.Start(1);
        RunBatchAck(gapped, new List<string>
        {
            Ck(100, 1700, 3, 100),
            Ck(100, 1700, 3, 109),
        });

        JsonValue gappedEntry = RecordedAcks(gapped, out _).At(0);
        Assert.Equal(new[] { 0, 9 }, BitsOf(gappedEntry));
        Assert.Equal(2, Convert.FromBase64String(gappedEntry.Get("b").StringValue()).Length);
    }

    /// <summary>The same checkpoint listed twice is one bit, not two: the payload and the entry
    /// count must both stay as small as a single message.</summary>
    [Fact]
    public void BatchAck_DuplicateCheckpointDoesNotGrowThePayload()
    {
        using var cluster = MockCluster.Start(1);
        RunBatchAck(cluster, new List<string>
        {
            Ck(100, 1700, 3, 100),
            Ck(100, 1700, 3, 100),
            Ck(100, 1700, 3, 100),
        });

        JsonValue acks = RecordedAcks(cluster, out _);
        Assert.Equal(1, acks.Size());
        Assert.Equal(new[] { 0 }, BitsOf(acks.At(0)));
        Assert.Equal("AQ==", acks.At(0).Get("b").StringValue());
    }

    // ---------------------------------------------------------------- (5) the key spellings

    /// <summary>The compact spellings, in the order the entries declare them.</summary>
    [Fact]
    public void BatchAck_WireKeysUseTheCompactSpellings()
    {
        using var cluster = MockCluster.Start(1);
        RunBatchAck(cluster, new List<string> { Ck(100, 1700, 3, 100) });

        WireRecord hit = SingleBatchRequest(cluster);
        JsonValue entry = RecordedBody(hit).Get("acks").At(0);
        Assert.Equal(new[] { "c", "t", "r", "so", "q", "rq", "pt", "it", "b" }, Keys(entry));
        Assert.Equal("0", entry.Get("r").StringValue());
        Assert.Equal(3, entry.Get("q").IntValue());
        Assert.Equal(1700, entry.Get("pt").IntValue());

        // The same frame read back through the body model: every field of the entry is the one
        // the checkpoint carried, and nothing is stored twice.
        Assert.True(BatchAckMessageRequestBody.Decode(hit.Body, out BatchAckMessageRequestBody body));
        Assert.Equal(Broker, body.BrokerName);
        Assert.Single(body.Acks);
        Assert.Equal(Group, body.Acks[0].ConsumerGroup);
        Assert.Equal(Topic, body.Acks[0].Topic);
        Assert.Equal("0", body.Acks[0].Retry);
        Assert.Equal(100, body.Acks[0].StartOffset);
        Assert.Equal(3, body.Acks[0].QueueId);
        Assert.Equal(0, body.Acks[0].ReviveQueueId);
        Assert.Equal(1700, body.Acks[0].PopTime);
        Assert.Equal(60000, body.Acks[0].InvisibleTime);
        Assert.Equal(new[] { 0 }, body.Acks[0].AckOffsets.ToArray());
    }

    /// <summary>The reader accepts the long names too, one key at a time, so a peer that spells
    /// them out still round-trips -- and an entry using them is not what this client emits.</summary>
    [Fact]
    public void BatchAck_ReadsBackTheLongKeySpellings()
    {
        var compact = BatchAck.FromJson(Json.Parse(
            "{\"c\":\"G\",\"t\":\"T\",\"r\":\"1\",\"so\":9,\"q\":2,\"rq\":3,\"pt\":4,\"it\":5,\"b\":\"Ag==\"}"));
        var longHanded = BatchAck.FromJson(Json.Parse(
            "{\"consumerGroup\":\"G\",\"topic\":\"T\",\"retry\":\"1\",\"startOffset\":9,"
            + "\"queueId\":2,\"reviveQueueId\":3,\"popTime\":4,\"invisibleTime\":5,\"bitSet\":\"Ag==\"}"));

        Assert.Equal(longHanded.ConsumerGroup, compact.ConsumerGroup);
        Assert.Equal(longHanded.Topic, compact.Topic);
        Assert.Equal(longHanded.Retry, compact.Retry);
        Assert.Equal(longHanded.StartOffset, compact.StartOffset);
        Assert.Equal(longHanded.QueueId, compact.QueueId);
        Assert.Equal(longHanded.ReviveQueueId, compact.ReviveQueueId);
        Assert.Equal(longHanded.PopTime, compact.PopTime);
        Assert.Equal(longHanded.InvisibleTime, compact.InvisibleTime);
        Assert.Equal(longHanded.AckOffsets.ToArray(), compact.AckOffsets.ToArray());

        // ... and the object it writes back is compact, so a round trip cannot smuggle a long
        // name onto the wire.
        Assert.Equal(new[] { "c", "t", "r", "so", "q", "rq", "pt", "it", "b" }, Keys(compact.ToJson()));
        Assert.Equal("Ag==", compact.ToJson().Get("b").StringValue());
    }

    // ---------------------------------------------------------------- (6) the body's broker name

    /// <summary>The broker of the whole request is the first checkpoint's, no matter what the
    /// later ones claim: a batch never spans two brokers.</summary>
    [Fact]
    public void BatchAck_BrokerNameIsTheFirstCheckpoints()
    {
        using var cluster = MockCluster.Start(1);
        RunBatchAck(cluster, new List<string>
        {
            Ck(100, 1700, 60000, 0, "0", Broker, 3, 100),
            Ck(100, 1700, 60000, 0, "0", "broker-9", 3, 101),
        });

        RecordedAcks(cluster, out JsonValue body);
        Assert.Equal(Broker, body.Get("brokerName").StringValue());
        Assert.NotEqual("broker-9", body.Get("brokerName").StringValue());
    }

    /// <summary>An empty first broker name still wins: "already taken" is decided by position,
    /// not by whether the name has any content.</summary>
    [Fact]
    public void BatchAck_EmptyFirstBrokerNameStillWins()
    {
        using var cluster = MockCluster.Start(1);
        MQClientInstance instance = Seeded(cluster);
        try
        {
            instance.BatchAckMessage(Group, Topic, new List<string>
            {
                Ck(100, 1700, 60000, 0, "0", string.Empty, 3, 100),
                Ck(100, 1700, 60000, 0, "0", Broker, 3, 101),
            }, 3000, cluster.BrokerAddrs[0]);
        }
        finally
        {
            instance.Shutdown();
        }

        RecordedAcks(cluster, out JsonValue body);
        Assert.Equal(string.Empty, body.Get("brokerName").StringValue());
    }

    /// <summary>The address is resolved from that same first checkpoint: name it at a broker the
    /// route does not carry and the request never goes out.</summary>
    [Fact]
    public void BatchAck_AddressComesFromTheFirstCheckpoint()
    {
        using var cluster = MockCluster.Start(1);
        MQClientException e = Assert.Throws<MQClientException>(() => RunBatchAck(cluster, new List<string>
        {
            Ck(100, 1700, 60000, 0, "0", "broker-missing", 3, 100),
            Ck(100, 1700, 60000, 0, "0", Broker, 3, 101),
        }));
        Assert.Contains("broker-missing", e.Message, StringComparison.Ordinal);
        Assert.Equal(0, cluster.CountRequests(RequestCode.BatchAckMessage));

        // Same list, first checkpoint naming a broker the route does know: exactly one frame.
        using var ok = MockCluster.Start(1);
        Assert.Equal(ResponseCode.Success, RunBatchAck(ok, new List<string>
        {
            Ck(100, 1700, 60000, 0, "0", Broker, 3, 100),
            Ck(100, 1700, 60000, 0, "0", Broker, 3, 101),
        }));
        Assert.Equal(1, ok.CountRequests(RequestCode.BatchAckMessage));
    }

    // ---------------------------------------------------------------- (7) the response code

    /// <summary>The primitive hands the broker code back unchanged: a failure is reported, not
    /// thrown and not swallowed into success.</summary>
    [Fact]
    public void BatchAck_ReturnsTheBrokerResponseCodeUnchanged()
    {
        using var cluster = MockCluster.Start(1);
        cluster.SetReplyCode(RequestCode.BatchAckMessage, ResponseCode.SystemError);
        Assert.Equal(ResponseCode.SystemError, RunBatchAck(cluster, new List<string>
        {
            Ck(100, 1700, 3, 100),
        }));

        cluster.SetReplyCode(RequestCode.BatchAckMessage, ResponseCode.SystemBusy);
        Assert.Equal(ResponseCode.SystemBusy, RunBatchAck(cluster, new List<string>
        {
            Ck(100, 1700, 3, 100),
        }));

        cluster.SetReplyCode(RequestCode.BatchAckMessage, ResponseCode.Success);
        Assert.Equal(ResponseCode.Success, RunBatchAck(cluster, new List<string>
        {
            Ck(100, 1700, 3, 100),
        }));
        Assert.Equal(3, cluster.CountRequests(RequestCode.BatchAckMessage));
    }

    /// <summary>Control for the case above: the single ack publishes the same convention, so the
    /// batch primitive returning a code instead of throwing is the house rule, not a slip.</summary>
    [Fact]
    public void SingleAck_ReturnsTheBrokerResponseCodeUnchanged()
    {
        using var cluster = MockCluster.Start(1);
        cluster.SetReplyCode(RequestCode.AckMessage, ResponseCode.SystemError);
        Assert.Equal(ResponseCode.SystemError, RunAck(cluster, Ck(100, 1700, 3, 100), 100));

        cluster.SetReplyCode(RequestCode.AckMessage, ResponseCode.Success);
        Assert.Equal(ResponseCode.Success, RunAck(cluster, Ck(100, 1700, 3, 100), 100));
    }

    // ---------------------------------------------------------------- (8) unusable input never reaches the wire

    [Fact]
    public void BatchAck_RejectsACheckpointBehindItsStartOffsetWithoutSending()
    {
        using var cluster = MockCluster.Start(1);
        Assert.Throws<ArgumentException>(() => RunBatchAck(cluster, new List<string>
        {
            Ck(100, 1700, 3, 99),
        }));
        Assert.Equal(0, cluster.CountRequests(RequestCode.BatchAckMessage));

        // The boundary itself is fine: an offset equal to the start offset is bit 0.
        Assert.Equal(ResponseCode.Success, RunBatchAck(cluster, new List<string>
        {
            Ck(100, 1700, 3, 100),
        }));
        Assert.Equal(1, cluster.CountRequests(RequestCode.BatchAckMessage));
    }

    [Fact]
    public void BatchAck_RejectsCheckpointsItCannotReadWithoutSending()
    {
        using var cluster = MockCluster.Start(1);

        // A checkpoint without the trailing consume-queue offset cannot name a bit at all.
        Assert.Throws<ArgumentException>(() => RunBatchAck(cluster, new List<string>
        {
            ExtraInfoUtil.BuildExtraInfo(100, 1700, 60000, 0, Topic, Broker, 3),
        }));
        Assert.Equal(0, cluster.CountRequests(RequestCode.BatchAckMessage));

        Assert.Throws<ArgumentException>(() => RunBatchAck(cluster, new List<string>()));
        Assert.Equal(0, cluster.CountRequests(RequestCode.BatchAckMessage));

        // A bit index outside one consume-queue window is refused instead of allocating a payload
        // nobody can read: the largest index a batch may still name is accepted.
        Assert.Throws<ArgumentException>(() => RunBatchAck(cluster, new List<string>
        {
            Ck(0, 1700, 3, BatchAck.MaxBitIndex + 1L),
        }));
        Assert.Equal(0, cluster.CountRequests(RequestCode.BatchAckMessage));

        Assert.Equal(ResponseCode.Success, RunBatchAck(cluster, new List<string>
        {
            Ck(0, 1700, 3, 4095),
        }));
        WireRecord hit = cluster.Records().FindAll(
            r => r.Code == RequestCode.BatchAckMessage).Single();
        Assert.Equal(512, Convert.FromBase64String(
            RecordedBody(hit).Get("acks").At(0).Get("b").StringValue()).Length);
    }
}
