// broker 真的死了：在途请求必须**立刻**有终态（Java failFast → requestFail）真机验证。
//（对应 python/verify_fail_fast_live.py、cpp/examples/live_fail_fast.cpp、
//  rust/examples/live_fail_fast.rs 的 L1–L5）
//
// 离线用例（tests/FailFastTests.cs）锁的是传输层契约：本机假对端读完就关，断言
// "毫秒级判死 + 报 RemotingSendRequestException 而不是超时 + 回调只投一次"。但这条路径
// 存在的意义正是**真机上的 broker 重启 / 主备切换 / 网络抖动**，只有真集群能回答：
//   L1 基线：真 broker 上发送与长轮询都正常（先确认后面的失败不是环境造成的）。
//   L2 挂起：把三条**真的挂在 broker 上**的长轮询（suspend 20s、客户端超时 30s）钉在在途表里。
//   L3 收口：杀掉 broker（读线程见到 EOF）→ 长轮询必须立刻拿到
//       RemotingSendRequestException，而不是等满 30s 报一个 RemotingTimeoutException。
//       类型不能错：异步发送的重试分类按异常**类型**分流，报成超时等于换了一整套重试决策。
//   L4 范围：判死只牵连死掉那条连接；同一个传输实例上的 namesrv 连接照常服务。
//   L5 恢复：broker 拉起后同一个 producer 实例重新建连照常发送；已拿到 SEND_OK 的消息
//       一条都不能少。
//
// L3 的阈值（8s）远小于客户端超时（30s），也小于 broker 的 suspend 上限（20s）：缺了
// failFast 这条断言必然失败，不是碰运气。
//
// 用法：rmq fail-fast [namesrv]（会停一次本地 broker 再拉起，不删 store）
using System.Diagnostics;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveFailFast
{
    /// <summary>客户端侧超时故意放到 30s：判死若走的是超时路径，至少要等这么久。</summary>
    private const int ClientTimeoutMillis = 30000;

    /// <summary>broker 侧挂起上限，故意小于客户端超时。</summary>
    private const int BrokerSuspendMillis = 20000;

    /// <summary>failFast 应当是毫秒级；留 8s 给真机调度（EOF 到达 + 回调投递）。</summary>
    private const int FailFastLimitMs = 8000;

    private const int BaselineMsgs = 5;
    private const int Parked = 3;

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

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string ScriptPath()
    {
        string root = Environment.GetEnvironmentVariable("RMQ_REPO_ROOT") ?? "..";
        return Path.Combine(root, "scripts", "rmq_test_broker.sh");
    }

    /// <summary>
    /// 跑 scripts/rmq_test_broker.sh。输出走**文件**而不是管道：start 会把 broker 拉成
    /// 常驻进程，谁继承它的写端谁就等不到 EOF。
    /// </summary>
    private static (bool Ok, string Out) BrokerCtl(string script, string action)
    {
        string logPath = "/tmp/rmq_fail_fast_cs_broker_ctl." + action + ".log";
        try
        {
            var psi = new ProcessStartInfo("/bin/sh",
                "-c \"sh '" + script + "' " + action + " > '" + logPath + "' 2>&1 < /dev/null\"")
            {
                UseShellExecute = false,
            };
            using Process? proc = Process.Start(psi);
            proc?.WaitForExit();
            string text = File.Exists(logPath) ? File.ReadAllText(logPath).Trim() : string.Empty;
            bool ok = proc is not null && proc.ExitCode == 0;
            return (ok, text.Length > 0 ? text : "exit=" + (proc?.ExitCode ?? -1));
        }
        catch (Exception ex)
        {
            return (false, "run " + action + " failed: " + ex.Message);
        }
    }

    /// <summary>
    /// 手工构造一条 suspend=true 的长轮询：不经 pull consumer 的钳制，30s 客户端超时与
    /// 20s broker suspend 都由本用例说了算。
    /// </summary>
    private static RemotingCommand ParkedPullRequest(string group, MessageQueue mq, long queueOffset)
    {
        var header = new PullMessageRequestHeader
        {
            ConsumerGroup = group,
            Topic = mq.Topic,
            QueueId = mq.QueueId,
            QueueOffset = queueOffset,
            MaxMsgNums = 32,
            SysFlag = PullSysFlag.BuildSysFlag(
                commitOffset: false, suspend: true, subscription: true, classFilter: false),
            CommitOffset = 0,
            SuspendTimeoutMillis = BrokerSuspendMillis,
            Subscription = "*",
            SubVersion = 0,
            ExpressionType = "TAG",
            MaxMsgBytes = -1,
            RequestSource = 0,
        };
        return RemotingCommand.CreateRequestCommand(RequestCode.PullMessage, header);
    }

    /// <summary>一条挂起的长轮询的结局。</summary>
    private sealed class ParkedResult
    {
        public string Kind = string.Empty;   // response / send_request / timeout / connect / other
        public string Message = string.Empty;
        public int ElapsedMs;
    }

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        string script = ScriptPath();
        long stamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        string topic = "FailFastCs_" + stamp;
        string group = "fail_fast_cs_" + stamp;

        Console.WriteLine("broker_ctl=" + script + " namesrv=" + namesrv + " topic=" + topic);
        (bool up, string upOut) = BrokerCtl(script, "status");
        if (!up)
        {
            Console.WriteLine("broker 没在跑：先按本地集群 runbook 起 namesrv + broker（" + upOut + "）");
            return 2;
        }

        var producer = new DefaultMQProducer(group + "_p")
        {
            NamesrvAddr = namesrv,
            InstanceName = "ff_cs_" + stamp,
        };
        var consumer = new DefaultMQPullConsumer(group);
        consumer.SetNamesrvAddr(namesrv);
        consumer.SetInstanceName("ff_cs_" + stamp);

        bool brokerStopped = false;
        try
        {
            // ------------------------------------------------------------ L1
            producer.Start();
            int landed = 0;
            for (int i = 0; i < BaselineMsgs; i++)
            {
                var msg = new Message(topic, Bytes("fail-fast-cs-base-" + i));
                msg.Keys = "ff-base-" + i;
                if (producer.Send(msg, 5000).SendStatus == SendStatus.SendOk)
                {
                    landed++;
                }
            }
            Check("L1 基线：" + BaselineMsgs + " 条同步发送 SEND_OK", landed == BaselineMsgs,
                "landed=" + landed);

            consumer.Start();
            List<MessageQueue> queues = consumer.FetchSubscribeMessageQueues(topic);
            Check("L1 取到队列", queues.Count > 0, "queues=" + queues.Count);
            if (queues.Count == 0)
            {
                return Finish(producer, consumer, script, brokerStopped);
            }

            MQClientInstance client = producer.Client();
            TopicRouteData? route = client.GetTopicRouteData(topic);
            string addr = route is null
                ? string.Empty
                : MQClientInstance.FindBrokerAddrInRoute(route, queues[0].BrokerName);
            Check("L1 拿到 broker 地址", addr.Length > 0, "addr=" + addr);
            if (addr.Length == 0)
            {
                return Finish(producer, consumer, script, brokerStopped);
            }

            long maxOffset = consumer.MaxOffset(queues[0]);
            Check("L1 待挂长轮询的队列有位点可用", maxOffset >= 0, "max_offset=" + maxOffset);
            // 发送是跨队列轮转的，单条队列的队尾只覆盖落在它上面的那部分，
            // 所以"真的落盘"要看全部队列的合计。
            long totalMax = 0;
            foreach (MessageQueue q in queues)
            {
                totalMax += consumer.MaxOffset(q);
            }
            Check("L1 各队列队尾位点合计覆盖刚发的 " + BaselineMsgs + " 条（真的落盘）",
                totalMax >= BaselineMsgs, "max_offset 合计=" + totalMax);

            // ------------------------------------------------------------ L2
            RemotingClient remoting = client.RemotingClient;
            int baselineInFlight = remoting.PendingRequestCount;
            var parked = new List<ParkedResult>();
            var parkedLock = new object();
            int returned = 0;
            Task[] tasks = new Task[Parked];
            for (int i = 0; i < Parked; i++)
            {
                tasks[i] = Task.Run(() =>
                {
                    var one = new ParkedResult();
                    Stopwatch sw = Stopwatch.StartNew();
                    try
                    {
                        RemotingCommand resp = remoting.InvokeSync(
                            addr, ParkedPullRequest(group, queues[0], maxOffset), ClientTimeoutMillis);
                        one.Kind = "response:" + resp.Code;
                    }
                    catch (RemotingSendRequestException ex)
                    {
                        one.Kind = "send_request";
                        one.Message = ex.Message;
                    }
                    catch (RemotingTimeoutException ex)
                    {
                        one.Kind = "timeout";
                        one.Message = ex.Message;
                    }
                    catch (RemotingConnectException ex)
                    {
                        one.Kind = "connect";
                        one.Message = ex.Message;
                    }
                    catch (Exception ex)
                    {
                        one.Kind = "other";
                        one.Message = ex.GetType().Name + ": " + ex.Message;
                    }
                    one.ElapsedMs = (int)sw.ElapsedMilliseconds;
                    lock (parkedLock)
                    {
                        parked.Add(one);
                        returned++;
                    }
                });
            }
            Thread.Sleep(2000);
            int returnedNow;
            lock (parkedLock)
            {
                returnedNow = returned;
            }
            Check("L2 三条长轮询真的挂在 broker 上（2s 后仍未返回）", returnedNow == 0,
                "returned=" + returnedNow);
            Check("L2 在途表里有它们",
                remoting.PendingRequestCount >= baselineInFlight + Parked,
                "in_flight=" + remoting.PendingRequestCount + " baseline=" + baselineInFlight);

            // ------------------------------------------------------------ L3
            brokerStopped = true;
            (bool stopped, string stopOut) = BrokerCtl(script, "stop");
            Check("L3 停掉 broker", stopped, stopOut);
            Task.WaitAll(tasks, TimeSpan.FromSeconds(60));
            List<ParkedResult> snapshot;
            lock (parkedLock)
            {
                snapshot = new List<ParkedResult>(parked);
            }
            Check("L3 挂起的长轮询全部返回（没有卡死）", snapshot.Count == Parked,
                "got=" + snapshot.Count);

            int sendRequest = snapshot.Count(r => r.Kind == "send_request");
            int timeouts = snapshot.Count(r => r.Kind == "timeout");
            int worstMs = snapshot.Count == 0 ? 0 : snapshot.Max(r => r.ElapsedMs);
            string kinds = string.Join(",", snapshot.Select(r => r.Kind));
            string firstMsg = snapshot.FirstOrDefault(r => r.Kind == "send_request")?.Message ?? string.Empty;
            // 报的是 RemotingSendRequestException（Java failFast 的口径）
            Check("L3 报的是 RemotingSendRequestException（Java failFast 的口径）",
                sendRequest == Parked, "kinds=" + kinds);
            Check("L3 一条都没被报成超时（类型错 = 重试决策错）", timeouts == 0,
                "timeout=" + timeouts);
            Check("L3 判死耗时远小于 30s 客户端超时", worstMs < FailFastLimitMs,
                "worst=" + worstMs + "ms limit=" + FailFastLimitMs + "ms");
            Check("L3 异常文案带着断连原因", firstMsg.Contains("connection closed", StringComparison.Ordinal),
                firstMsg);
            bool drained = false;
            for (int i = 0; i < 40 && !drained; i++)
            {
                drained = remoting.PendingRequestCount == 0;
                if (!drained)
                {
                    Thread.Sleep(250);
                }
            }
            Check("L3 判死之后在途表排空", drained, "in_flight=" + remoting.PendingRequestCount);

            // ------------------------------------------------------------ L4
            // 判死必须只牵连死掉那条连接：namesrv 走的是同一个传输实例的另一条连接。
            RemotingCommand probe = RemotingCommand.CreateRequestCommand(
                RequestCode.GetAllTopicListFromNameserver, null);
            string nsKind;
            int nsCode = -1;
            try
            {
                nsKind = "response";
                nsCode = remoting.InvokeSync(namesrv, probe, 5000).Code;
            }
            catch (Exception ex)
            {
                nsKind = ex.GetType().Name + ": " + ex.Message;
            }
            Check("L4 namesrv 连接没被牵连（broker 死了它还在服务）",
                nsKind == "response" && nsCode == ResponseCode.Success,
                nsKind + " code=" + nsCode);

            // ------------------------------------------------------------ L5
            (bool started, string startOut) = BrokerCtl(script, "start");
            Check("L5 broker 重新拉起", started, startOut);
            brokerStopped = false;

            bool recovered = false;
            int attempts = 0;
            string recErr = "never attempted";
            for (int i = 0; i < 20 && !recovered; i++)
            {
                attempts = i + 1;
                try
                {
                    var msg = new Message(topic, Bytes("fail-fast-cs-recover-" + i));
                    msg.Keys = "ff-recover";
                    recovered = producer.Send(msg, 5000).SendStatus == SendStatus.SendOk;
                    if (!recovered)
                    {
                        recErr = "status not OK";
                    }
                }
                catch (Exception ex)
                {
                    recErr = ex.Message;
                    Thread.Sleep(1000);
                }
            }
            Check("L5 同一个 producer 实例重新建连后照常发送", recovered,
                "attempts=" + attempts + " " + recErr);

            long after = 0;
            for (int i = 0; i < 20; i++)
            {
                after = 0;
                bool allRead = true;
                foreach (MessageQueue q in queues)
                {
                    try
                    {
                        after += consumer.MaxOffset(q);
                    }
                    catch
                    {
                        allRead = false;
                    }
                }
                if (allRead && after >= BaselineMsgs)
                {
                    break;
                }
                Thread.Sleep(500);
            }
            Check("L5 重启后 broker 上仍有那 " + BaselineMsgs + " 条 SEND_OK 的消息",
                after >= BaselineMsgs, "max_offset 合计=" + after);
        }
        catch (Exception ex)
        {
            Check("用例整体", false, ex.GetType().Name + ": " + ex.Message);
        }

        return Finish(producer, consumer, script, brokerStopped);
    }

    /// <summary>收尾：关客户端，并保证不把测试集群留在停机状态交给下一个用例。</summary>
    private static int Finish(DefaultMQProducer producer, DefaultMQPullConsumer consumer,
        string script, bool brokerStopped)
    {
        try
        {
            consumer.Shutdown();
        }
        catch
        {
            // 收尾失败不该掩盖用例结论
        }
        try
        {
            producer.Shutdown();
        }
        catch
        {
            // 同上
        }
        if (brokerStopped)
        {
            (bool ok, string outText) = BrokerCtl(script, "start");
            if (!ok)
            {
                Console.WriteLine("  [cleanup] broker 仍没起来：" + outText);
            }
        }
        Console.WriteLine("############ PASS=" + _pass + " FAIL=" + _fail + " ############");
        return _fail == 0 ? 0 : 1;
    }
}
