// The offline case (tests/FailFastTests.cs) pins the transport layer contract: a local fake
// peer closes right after reading, asserting "millisecond-scale death detection + it
// reports RemotingSendRequestException rather than a timeout + the callback is delivered
// only once". But the whole point of this path is the **real machine** case of a broker
// restart / master-slave switch / network jitter, which only a real cluster can answer:
//   L1 baseline: sending and long polling both work against the real broker (confirm first
//      that the later failure is not caused by the environment).
//   L2 park: pin three long polls that really **hang on the broker** (suspend 20s, client
//      timeout 30s) in the in-flight table.
//   L3 converge: kill the broker (the reader thread sees EOF) -> the long polls must get
//       RemotingSendRequestException immediately instead of waiting out the 30s for a
//       RemotingTimeoutException. The type must not be wrong: the retry classification of an
//       async send branches on the exception **type**, so reporting a timeout means a whole
//       different set of retry decisions.
//   L4 scope: death detection must only involve the connection that died; the namesrv
//      connection on the same transport instance keeps serving as usual.
//   L5 recovery: once the broker is back up, the same producer instance reconnects and
//       sends as usual; not one of the messages that already got SEND_OK may be missing.
//
// L3's threshold (8s) is far below the client timeout (30s) and also below the broker's
// suspend cap (20s): without the failFast assertion this necessarily fails, it is not luck.
//
// Usage: rmq fail-fast [namesrv] (stops the local broker once and starts it back up, does not delete the store)
using System.Diagnostics;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveFailFast
{
    /// <summary>The client-side timeout is deliberately set to 30s: if death detection went
    /// down the timeout path it would have to wait at least that long.</summary>
    private const int ClientTimeoutMillis = 30000;

   /// <summary>The broker-side suspend cap, deliberately below the client timeout.</summary>
    private const int BrokerSuspendMillis = 20000;

    /// <summary>failFast should be millisecond-scale; 8s is left for real-machine scheduling
   /// (EOF arrival + callback delivery).</summary>
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
   /// Runs scripts/rmq_test_broker.sh. The output goes to a **file** rather than a pipe:
   /// start turns the broker into a resident process, and whoever inherits its write end
   /// never sees EOF.
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
   /// Hand-builds a long poll with suspend=true: it bypasses the pull consumer's clamping, so
   /// the 30s client timeout and the 20s broker suspend are both this case's own choice.
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

   /// <summary>The outcome of one hung long poll.</summary>
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
            Console.WriteLine("the broker is not running: start namesrv + broker first following the local cluster runbook (" + upOut + ")");
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
            Check("L1 baseline: " + BaselineMsgs + " synchronous sends got SEND_OK", landed == BaselineMsgs,
                "landed=" + landed);

            consumer.Start();
            List<MessageQueue> queues = consumer.FetchSubscribeMessageQueues(topic);
           Check("L1 got the queues", queues.Count > 0, "queues=" + queues.Count);
            if (queues.Count == 0)
            {
                return Finish(producer, consumer, script, brokerStopped);
            }

            MQClientInstance client = producer.Client();
            TopicRouteData? route = client.GetTopicRouteData(topic);
            string addr = route is null
                ? string.Empty
                : MQClientInstance.FindBrokerAddrInRoute(route, queues[0].BrokerName);
           Check("L1 got the broker address", addr.Length > 0, "addr=" + addr);
            if (addr.Length == 0)
            {
                return Finish(producer, consumer, script, brokerStopped);
            }

            long maxOffset = consumer.MaxOffset(queues[0]);
           Check("L1 the queue the long polls will park on has a usable offset", maxOffset >= 0, "max_offset=" + maxOffset);
           // Sending round-robins across the queues, so a single queue's tail only covers
     // the part that landed on it, which is why "really persisted" has to look at the
   // total across all queues.
            long totalMax = 0;
            foreach (MessageQueue q in queues)
            {
                totalMax += consumer.MaxOffset(q);
            }
           Check("L1 the tails of all queues together cover the " + BaselineMsgs + " just sent (really persisted)",
               totalMax >= BaselineMsgs, "max_offset total=" + totalMax);

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
           Check("L2 the three long polls really hang on the broker (still not returned after 2s)", returnedNow == 0,
                "returned=" + returnedNow);
           Check("L2 they are in the in-flight table",
                remoting.PendingRequestCount >= baselineInFlight + Parked,
                "in_flight=" + remoting.PendingRequestCount + " baseline=" + baselineInFlight);

            // ------------------------------------------------------------ L3
            brokerStopped = true;
            (bool stopped, string stopOut) = BrokerCtl(script, "stop");
           Check("L3 the broker was stopped", stopped, stopOut);
            Task.WaitAll(tasks, TimeSpan.FromSeconds(60));
            List<ParkedResult> snapshot;
            lock (parkedLock)
            {
                snapshot = new List<ParkedResult>(parked);
            }
  Check("L3 every hung long poll returned (nothing wedged)", snapshot.Count == Parked,
                "got=" + snapshot.Count);

            int sendRequest = snapshot.Count(r => r.Kind == "send_request");
            int timeouts = snapshot.Count(r => r.Kind == "timeout");
            int worstMs = snapshot.Count == 0 ? 0 : snapshot.Max(r => r.ElapsedMs);
            string kinds = string.Join(",", snapshot.Select(r => r.Kind));
            string firstMsg = snapshot.FirstOrDefault(r => r.Kind == "send_request")?.Message ?? string.Empty;
          Check("L3 what is reported is RemotingSendRequestException (the failFast rule)",
                sendRequest == Parked, "kinds=" + kinds);
         Check("L3 not one was reported as a timeout (a wrong type means wrong retry decisions)", timeouts == 0,
                "timeout=" + timeouts);
            Check("L3 the death detection took far less than the 30s client timeout", worstMs < FailFastLimitMs,
                "worst=" + worstMs + "ms limit=" + FailFastLimitMs + "ms");
       Check("L3 the exception text carries the disconnect reason", firstMsg.Contains("connection closed", StringComparison.Ordinal),
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
        Check("L3 the in-flight table is drained after death detection", drained, "in_flight=" + remoting.PendingRequestCount);

            // ------------------------------------------------------------ L4
            // Death detection must only involve the connection that died: the namesrv uses
            // another connection of the same transport instance.
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
  Check("L4 the namesrv connection was not involved (it keeps serving though the broker died)",
                nsKind == "response" && nsCode == ResponseCode.Success,
                nsKind + " code=" + nsCode);

            // ------------------------------------------------------------ L5
            (bool started, string startOut) = BrokerCtl(script, "start");
          Check("L5 the broker was started back up", started, startOut);
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
            Check("L5 the same producer instance sends as usual after reconnecting", recovered,
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
        Check("L5 after the restart the broker still holds those " + BaselineMsgs + " SEND_OK messages",
      after >= BaselineMsgs, "max_offset total=" + after);
        }
        catch (Exception ex)
        {
        Check("the case as a whole", false, ex.GetType().Name + ": " + ex.Message);
        }

        return Finish(producer, consumer, script, brokerStopped);
    }

  /// <summary>Cleanup: close the client, and make sure the test cluster is not left in a
  /// stopped state for the next case.</summary>
    private static int Finish(DefaultMQProducer producer, DefaultMQPullConsumer consumer,
        string script, bool brokerStopped)
    {
        try
        {
            consumer.Shutdown();
        }
        catch
        {
        // a cleanup failure must not mask the conclusion of the case
        }
        try
        {
            producer.Shutdown();
        }
        catch
        {
          // same
        }
        if (brokerStopped)
        {
            (bool ok, string outText) = BrokerCtl(script, "start");
            if (!ok)
            {
        Console.WriteLine("  [cleanup] the broker still did not come up: " + outText);
            }
        }
        Console.WriteLine("############ PASS=" + _pass + " FAIL=" + _fail + " ############");
        return _fail == 0 ? 0 : 1;
    }
}
