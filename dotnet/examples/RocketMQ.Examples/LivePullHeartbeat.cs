// 拉模式消费者心跳真机验证（#98）：对应 python/verify_pull_consumer_heartbeat_live.py 的
// A0–A6 与 cpp/examples/live_pull_heartbeat.cpp（三语言同一套断言）。
// 用法：rmq pull-heartbeat [namesrv] [masterAddr] [slaveAddr]
//
// 为什么必须真机：离线单测只证明「报文形状对」，证明不了 broker 的 ConsumerManager 真的把
// 本组登记进 consumerTable（broker 是按台建表的），也证明不了 35 注销之后立刻摘除。
//   * 203 examineConsumerConnectionInfo：组在不在、consumeType / consumeFromWhere /
//     messageModel / subscriptionTable 是什么（AdminBrokerProcessor:1971 读的就是这些）；
//   * 38 GET_CONSUMER_LIST_BY_GROUP：consumerTable 里的 clientId 列表（裸 RPC，组不在时
//     broker 回 "no consumer for this group"，不是空列表）；
//   * shutdown → 35 之后 203 是否立刻查不到（不必等 ~120s 通道扫描）。
// 反面对照：从未心跳过的幽灵组必须查不到 —— 否则说明判据本身是空转的。
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

    // 203 的原始答案：在线返回 ConsumerConnection，不在线返回 null（异常一律当不在线）。
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

    // 38 的原始答案：组不在时 broker 抛 "no consumer for this group"，当空列表处理。
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
        Console.WriteLine("PullConsumer heartbeat live (.NET): namesrv=" + namesrv + " master=" + master
            + (slave.Length > 0 ? " slave=" + slave : string.Empty));
        Console.WriteLine("topic=" + Topic + " group=" + Group);
        Console.WriteLine(new string('=', 70));

        var admin = new DefaultMQAdminExt("PullHbNetAdmin");
        admin.SetNamesrvAddr(namesrv);
        admin.Start();
        DefaultMQPullConsumer? consumer = null;
        try
        {
            // ---------------- A0 建 topic ----------------
            admin.CreateTopicInBroker(master, Topic, 4, 4);

            // ---------------- A1 起拉模式消费者并拉一轮 ----------------
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

            Check("A1 拉模式消费者启动并成功拉取一轮",
                consumer.HeartbeatCount >= 1 && mqs.Count > 0
                    && (pullStatus == "Found" || pullStatus == "NoNewMsg"),
                "queues=" + mqs.Count.ToString(CultureInfo.InvariantCulture) + " status=" + pullStatus
                    + " heartbeats=" + consumer.HeartbeatCount.ToString(CultureInfo.InvariantCulture));

            // ---------------- A2 主节点 203 ----------------
            ConsumerConnection? conn = GroupIsOnline(admin, Group, master);
            Check("A2 主节点 203 查到本组（心跳已注册）", conn is not null,
                "connections="
                    + (conn is null ? "-" : conn.ConnectionSet.Count.ToString(CultureInfo.InvariantCulture)));
            Check("A2 消费类型是 CONSUME_ACTIVELY（Java DefaultMQPullConsumerImpl:348）",
                conn is not null && conn.ConsumeType == ConsumeType.ConsumeActively,
                "consumeType=" + (conn?.ConsumeType ?? "-"));
            Check("A2 消费位点是 CONSUME_FROM_LAST_OFFSET（:353）",
                conn is not null && conn.ConsumeFromWhere == ConsumeFromWhere.ConsumeFromLastOffset,
                "consumeFromWhere=" + (conn?.ConsumeFromWhere ?? "-"));
            Check("A2 广播/集群口径是 CLUSTERING",
                conn is not null && conn.MessageModel == MessageModel.Clustering,
                "messageModel=" + (conn?.MessageModel ?? "-"));

            // ---------------- A2b 订阅集来自 registerTopics（Java subscriptions():357-385）----
            string? subString = conn is null ? null : SubStringOf(conn, Topic);
            Check("A2b 203 的订阅表带 registerTopics 的 topic 且 subString=*",
                subString == "*",
                "subscriptionTable=" + (conn?.SubscriptionTable.Dump() ?? "-"));

            // ---------------- A3 38 主节点 ----------------
            string clientId = consumer.ClientId;
            List<string> ids = ConsumerIds(admin, Group, master);
            Check("A3 主节点 38 查到本 clientId", ids.Contains(clientId),
                "ids=" + ids.Count.ToString(CultureInfo.InvariantCulture) + " clientId=" + clientId);

            // ---------------- A4 从节点 ----------------
            if (slave.Length > 0)
            {
                ConsumerConnection? slaveConn = GroupIsOnline(admin, Group, slave);
                Check("A4 从节点 203 也查到本组（心跳扇出到从节点）", slaveConn is not null,
                    "slave=" + slave + " connections="
                        + (slaveConn is null
                            ? "-"
                            : slaveConn.ConnectionSet.Count.ToString(CultureInfo.InvariantCulture)));
                List<string> slaveIds = ConsumerIds(admin, Group, slave);
                Check("A4 从节点 38 也查到本 clientId", slaveIds.Contains(clientId),
                    "ids=" + slaveIds.Count.ToString(CultureInfo.InvariantCulture));
            }

            // ---------------- A5 对照：幽灵组（从未心跳）必须查不到 ----------------
            ConsumerConnection? ghostConn = GroupIsOnline(admin, GhostGroup, master);
            List<string> ghostIds = ConsumerIds(admin, GhostGroup, master);
            Check("A5 对照：未心跳的幽灵组 203 查不到", ghostConn is null,
                "connections="
                    + (ghostConn is null
                        ? "-"
                        : ghostConn.ConnectionSet.Count.ToString(CultureInfo.InvariantCulture)));
            Check("A5 对照：未心跳的幽灵组 38 空列表", ghostIds.Count == 0,
                "ids=" + ghostIds.Count.ToString(CultureInfo.InvariantCulture));

            // ---------------- A6 shutdown 立刻注销（35）----------------
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

            Check("A6 shutdown 后 203 立刻查不到本组（发过 35 注销）", gone, "clientId=" + clientId);
        }
        catch (Exception e)
        {
            Check("联调异常", false, e.ToString());
        }
        finally
        {
            try
            {
                consumer?.Shutdown();
            }
            catch (Exception)
            {
                // 清理路径，忽略
            }

            try
            {
                admin.DeleteTopicInBroker(master, Topic);
            }
            catch (Exception)
            {
                // 清理路径，忽略
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
