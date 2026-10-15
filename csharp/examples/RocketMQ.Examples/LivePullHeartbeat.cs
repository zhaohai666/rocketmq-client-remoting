// Real-machine verification of the pull-mode consumer heartbeat (#98), covering scenario
// sections A0-A6.
// Usage: rmq pull-heartbeat [namesrv] [masterAddr] [slaveAddr]
//
// Why it must be a real machine: the offline unit tests only prove "the wire shape is
// right", they cannot prove that the broker's ConsumerManager really registered this group
// in consumerTable (the broker builds the table per instance), nor that the 35 unregister
// removes it immediately.
//   * 203 examineConsumerConnectionInfo: whether the group is there, and what
//     consumeType / consumeFromWhere / messageModel / subscriptionTable are (these are
//     exactly what AdminBrokerProcessor:1971 reads);
//   * 38 GET_CONSUMER_LIST_BY_GROUP: the clientId list in consumerTable (a bare RPC; when
//     the group is not there the broker replies "no consumer for this group", not an empty
//     list);
//   * after shutdown -> 35, whether a 203 stops finding it at once (without waiting out the
//     ~120s channel scan).
// The negative control: a ghost group that never heartbeated must not be findable --
// otherwise the criterion itself is spinning on nothing.
using System.Globalization;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LivePullHeartbeat
{
    private static readonly string Stamp =
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);

    private static readonly string Topic = "PullHbNet_" + Stamp;
    private static readonly string Group = "PG_PullHbNet_" + Stamp;
    private static readonly string GhostGroup = "PG_PullHbNetGhost_" + Stamp;

    private static int _pass;
    private static int _fail;

    private static void Check(string name, bool ok, string detail = "")
    {
        if (ok)
        {
            Interlocked.Increment(ref _pass);
            Console.WriteLine("  [PASS] " + name + (detail.Length > 0 ? "  " + detail : ""));
        }
        else
        {
            Interlocked.Increment(ref _fail);
            Console.WriteLine("  [FAIL] " + name + (detail.Length > 0 ? "  " + detail : ""));
        }
    }

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    // 203's raw answer: ConsumerConnection when online, null when not (an exception always
    // counts as not online).
    private static ConsumerConnection? GroupIsOnline(DefaultMQAdminExt admin, string group,
        string addr)
    {
        try
        {
            return admin.ExamineConsumerConnectionInfo(group, addr);
        }
        catch (Exception)
        {
            return null;
        }
    }

    // 38's raw answer: when the group is not there the broker throws "no consumer for this
    // group", which is treated as an empty list.
    private static List<string> ConsumerIds(DefaultMQAdminExt admin, string group, string addr)
    {
        try
        {
            return admin.GetConsumerListByGroup(group, addr).ConsumerIdList;
        }
        catch (Exception)
        {
            return new List<string>();
        }
    }

    private static string? SubStringOf(ConsumerConnection conn, string topic)
    {
        JsonValue entry = conn.SubscriptionTable.Find(topic) ?? JsonValue.Null;
        if (!entry.IsObject)
        {
            return null;
        }

        JsonValue? s = entry.Find("subString") ?? entry.Find("sub_string");
        return s is null ? null : s.StringValue();
    }

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        string master = args.Length > 1 ? args[1] : "127.0.0.1:10911";
        string slave = args.Length > 2 ? args[2] : string.Empty;

        Console.WriteLine(new string('=', 70));
        Console.WriteLine("PullConsumer heartbeat live (C#): namesrv=" + namesrv + " master=" + master
            + (slave.Length > 0 ? " slave=" + slave : string.Empty));
        Console.WriteLine("topic=" + Topic + " group=" + Group);
        Console.WriteLine(new string('=', 70));

        var admin = new DefaultMQAdminExt("PullHbNetAdmin");
        admin.SetNamesrvAddr(namesrv);
        admin.Start();
        DefaultMQPullConsumer? consumer = null;
        try
        {
            // ---------------- A0 create the topic ----------------
            admin.CreateTopicInBroker(master, Topic, 4, 4);

            // ---------------- A1 start the pull-mode consumer and pull one round ----------------
            consumer = new DefaultMQPullConsumer(Group);
            consumer.SetNamesrvAddr(namesrv);
            consumer.RegisterMessageQueueListener(Topic, new NoopListener());
            consumer.Start();
            List<MessageQueue> mqs = new();
            string pullStatus = "-";
            try
            {
                mqs = consumer.FetchSubscribeMessageQueues(Topic);
                if (mqs.Count > 0)
                {
                    PullResult r = consumer.Pull(mqs[0], "*", 0, 32, 5000);
                    pullStatus = r.Status.ToString();
                }
            }
            catch (Exception e)
            {
                pullStatus = e.GetType().Name + ": " + e.Message;
            }

            Check("A1 the pull-mode consumer started and pulled one round successfully",
                consumer.HeartbeatCount >= 1 && mqs.Count > 0
                    && (pullStatus == "Found" || pullStatus == "NoNewMsg"),
                "queues=" + mqs.Count.ToString(CultureInfo.InvariantCulture) + " status=" + pullStatus
                    + " heartbeats=" + consumer.HeartbeatCount.ToString(CultureInfo.InvariantCulture));

            // ---------------- A2 203 on the master ----------------
            ConsumerConnection? conn = GroupIsOnline(admin, Group, master);
            Check("A2 the 203 on the master finds this group (the heartbeat registered it)", conn is not null,
                "connections="
                    + (conn is null ? "-" : conn.ConnectionSet.Count.ToString(CultureInfo.InvariantCulture)));
            Check("A2 the consume type is CONSUME_ACTIVELY",
                conn is not null && conn.ConsumeType == ConsumeType.ConsumeActively,
                "consumeType=" + (conn?.ConsumeType ?? "-"));
            Check("A2 the consume-from-where is CONSUME_FROM_LAST_OFFSET (:353)",
                conn is not null && conn.ConsumeFromWhere == ConsumeFromWhere.ConsumeFromLastOffset,
                "consumeFromWhere=" + (conn?.ConsumeFromWhere ?? "-"));
            Check("A2 the broadcasting/cluster rule is CLUSTERING",
                conn is not null && conn.MessageModel == MessageModel.Clustering,
                "messageModel=" + (conn?.MessageModel ?? "-"));

            string? subString = conn is null ? null : SubStringOf(conn, Topic);
            Check("A2b the 203 subscription table carries the topic from registerTopics and subString=*",
                subString == "*",
                "subscriptionTable=" + (conn?.SubscriptionTable.Dump() ?? "-"));

            // ---------------- A3 38 on the master ----------------
            string clientId = consumer.ClientId;
            List<string> ids = ConsumerIds(admin, Group, master);
            Check("A3 the 38 on the master finds this clientId", ids.Contains(clientId),
                "ids=" + ids.Count.ToString(CultureInfo.InvariantCulture) + " clientId=" + clientId);

            // ---------------- A4 the slave ----------------
            if (slave.Length > 0)
            {
                ConsumerConnection? slaveConn = GroupIsOnline(admin, Group, slave);
                Check("A4 the 203 on the slave finds this group too (the heartbeat fans out to the slave)", slaveConn is not null,
                    "slave=" + slave + " connections="
                        + (slaveConn is null
                            ? "-"
                            : slaveConn.ConnectionSet.Count.ToString(CultureInfo.InvariantCulture)));
                List<string> slaveIds = ConsumerIds(admin, Group, slave);
                Check("A4 the 38 on the slave finds this clientId too", slaveIds.Contains(clientId),
                    "ids=" + slaveIds.Count.ToString(CultureInfo.InvariantCulture));
            }

            // ---------------- A5 the control: a ghost group (never heartbeated) must not be findable ----------------
            ConsumerConnection? ghostConn = GroupIsOnline(admin, GhostGroup, master);
            List<string> ghostIds = ConsumerIds(admin, GhostGroup, master);
            Check("A5 control: a 203 for the never-heartbeated ghost group finds nothing", ghostConn is null,
                "connections="
                    + (ghostConn is null
                        ? "-"
                        : ghostConn.ConnectionSet.Count.ToString(CultureInfo.InvariantCulture)));
            Check("A5 control: the 38 for the never-heartbeated ghost group is an empty list", ghostIds.Count == 0,
                "ids=" + ghostIds.Count.ToString(CultureInfo.InvariantCulture));

            // ---------------- A6 shutdown unregisters immediately (35)----------------
            consumer.Shutdown();
            consumer = null;
            bool gone = false;
            long deadline = NowMs() + 10000;
            while (NowMs() < deadline)
            {
                if (GroupIsOnline(admin, Group, master) is null)
                {
                    gone = true;
                    break;
                }

                Thread.Sleep(500);
            }

            Check("A6 after shutdown a 203 stops finding this group at once (the 35 unregister was sent)", gone, "clientId=" + clientId);
        }
        catch (Exception e)
        {
            Check("the integration threw", false, e.ToString());
        }
        finally
        {
            try
            {
                consumer?.Shutdown();
            }
            catch (Exception)
            {
                // the cleanup path, ignored
            }

            try
            {
                admin.DeleteTopicInBroker(master, Topic);
            }
            catch (Exception)
            {
                // the cleanup path, ignored
            }

            admin.Shutdown();
        }

        Console.WriteLine();
        Console.WriteLine("PullConsumerHeartbeat: PASS=" + _pass + " FAIL=" + _fail);
        return _fail == 0 ? 0 : 1;
    }

    private sealed class NoopListener : IMessageQueueListener
    {
        public void MessageQueueChanged(string topic, IReadOnlyList<MessageQueue> mqAll,
            IReadOnlyList<MessageQueue> mqDivided)
        {
        }
    }
}
