// Prerequisite: the broker has `authenticationEnabled=true` and a SUPER user matching the
//       accessKey/secretKey pair has been created with `initAuthenticationUser` (see
//       broker_acl.conf in /tmp/run_acl_live.sh). At that point **every** broker RPC requires
//       a valid signature:
//       AuthConfig.isAuthenticationRequired() = authenticationEnabled && !whitelist.contains(rpc),
//       and the whitelist is empty by default.
//
// Scenarios:
//   S1 positive: an admin with credentials creates a topic -> success (the signature on the
// admin path is accepted)
//   S2 negative: an admin without credentials creates a topic -> rejected (the broker replies
// NO_PERMISSION=16)
//   S3 negative: a producer with a wrong secretKey sends -> rejected (NO_PERMISSION=16)
//   S4 positive: a producer with credentials sends 3 -> SendOk (the msgId is assigned by the
// broker)
//   S5 positive: a consumer with credentials receives all 3 (the heartbeat / long poll /
// offset commit all carry signatures)
//            Note: S4/S5 must **start the consumer before sending**, otherwise the
//              CONSUME_FROM_LAST_OFFSET initial-offset semantics plus the broker's
//              asynchronous dispatch make the same timing give different results across
//              client implementations (see the comments at S4/S5)
//   S6 negative: a bare RPC straight to the broker without credentials -> rejected
// (NO_PERMISSION=16)
//   S7 positive: a route query through the NameServer without credentials still succeeds
// (the hook only applies to targets that require authentication)
//
// Key facts (do not change them from memory): every broker-side authentication failure
// throws AbortProcessException(NO_PERMISSION=16);
// the reason cannot be told apart by ResponseCode, only by the remark:
//   - a wrong signature -> "check signature failed."
//   - missing credentials -> "User:null is not found."
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;

namespace RocketMQ.Examples;

public static class LiveAcl
{
    // The broker's uniform response code for an authentication failure.
    private const int NoPermission = 16;

    // CreateTopicInRoute splices the underlying MQBrokerException's Message into the
    // exception, of the form "create new topic failed: CODE: 16 DESC: ...", so this marker
    // identifies NO_PERMISSION. Derived from NoPermission so the number is not written
    // twice and then drifts.
    private static readonly string NoPermissionMarker =
        "CODE: " + NoPermission.ToString(CultureInfo.InvariantCulture);

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

    private static byte[] Str2Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private sealed class CountingListener : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<string> _bodies = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            lock (_lk)
            {
                foreach (MessageExt m in msgs)
                {
                    _bodies.Add(Encoding.UTF8.GetString(m.Body));
                }
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public int Count
        {
            get { lock (_lk) return _bodies.Count; }
        }

        public List<string> Snapshot()
        {
            lock (_lk) return new List<string>(_bodies);
        }
    }

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        string ak = args.Length > 1 ? args[1] : "AK_TEST";
        string sk = args.Length > 2 ? args[2] : "SK_TEST_SECRET_12345678";

        string stamp = (NowMs() % 1000000000L).ToString(CultureInfo.InvariantCulture);
        string topic = "AclLiveNet_" + stamp;
        string group = "GID_AclLiveNet_" + stamp;

        Console.WriteLine("======================================================================");
        Console.WriteLine("ACL live (C#): namesrv=" + namesrv + " topic=" + topic
            + " group=" + group + " ak=" + ak);
        Console.WriteLine("======================================================================");

        // ---------------- S1 positive: an admin with credentials creates the topic ----------------
        Console.WriteLine();
        Console.WriteLine("S1 an admin with credentials creates the topic");
        {
            var admin = new DefaultMQAdminExt("ACL_ADMIN_OK");
            admin.SetNamesrvAddr(namesrv);
            admin.SetCredentials(ak, sk);
            admin.Start();
            try
            {
                admin.CreateTopic("TBW102", topic, 4);
    Check("S1 the admin with credentials created the topic successfully", true);
            }
            catch (Exception e)
            {
      Check("S1 the admin with credentials created the topic successfully", false, e.Message);
            }
            finally
            {
                admin.Shutdown();
            }
        }

        // ---------------- S2 negative: an admin without credentials must be rejected ----------------
        Console.WriteLine();
        Console.WriteLine("S2 an admin without credentials creating a topic must be rejected");
        {
            var admin = new DefaultMQAdminExt("ACL_ADMIN_NO");
            admin.SetNamesrvAddr(namesrv);
            admin.Start();
            bool rejected = false;
            string detail;
            try
            {
                admin.CreateTopic("TBW102", topic + "_DENIED", 4);
                detail = "unexpectedly succeeded";
            }
            catch (Exception e)
            {
                detail = e.Message;
                rejected = detail.Contains(NoPermissionMarker, StringComparison.Ordinal);
            }
            finally
            {
                admin.Shutdown();
            }

        Check("S2 the admin without credentials was rejected by the broker (NO_PERMISSION=16)", rejected, detail);
        }

     // ---------------- S3 negative: a wrong secretKey must be rejected ----------------
        Console.WriteLine();
        Console.WriteLine("S3 a producer with a wrong secretKey must be rejected");
        {
            var bad = new DefaultMQProducer("PG_AclNetBad_" + stamp);
            bad.NamesrvAddr = namesrv;
            bad.SetCredentials(ak, "WRONG_SECRET_KEY");
            bad.Start();
            bool rejected = false;
            string detail;
            try
            {
                bad.Send(new Message(topic, Str2Bytes("should-not-send")));
                detail = "unexpectedly succeeded";
            }
            catch (MQBrokerException e)
            {
                detail = "code=" + e.ResponseCode.ToString(CultureInfo.InvariantCulture)
                    + " " + e.ResponseMessage;
                rejected = e.ResponseCode == NoPermission;
            }
            catch (Exception e)
            {
                detail = e.Message;
                rejected = detail.Contains(NoPermissionMarker, StringComparison.Ordinal);
            }
            finally
            {
                bad.Shutdown();
            }

        Check("S3 the wrong secretKey was rejected by the broker (NO_PERMISSION=16)", rejected, detail);
        }

        int sent = 0;
        Console.WriteLine();
        Console.WriteLine("S4/S5 the producer/consumer with the correct credentials (start the consumer before sending)");
        {
            var listener = new CountingListener();
            var consumer = new DefaultMQPushConsumer(group);
            consumer.SetNamesrvAddr(namesrv);
            consumer.SetCredentials(ak, sk);
            consumer.Subscribe(topic, "*");
            consumer.SetMessageListener(listener);
            consumer.Start();
            try
            {
        // Wait for the rebalance to hand the queues over: the initial offset has to be
  // resolved while the topic is still empty
                Thread.Sleep(5000);

                var evidence = new StringBuilder();
                var prod = new DefaultMQProducer("PG_AclNetOk_" + stamp);
                prod.NamesrvAddr = namesrv;
                prod.SetCredentials(ak, sk);
                prod.Start();
                for (int i = 0; i < 3; ++i)
                {
                    try
                    {
                        SendResult r = prod.Send(new Message(topic, Str2Bytes("acl-ok-" + i)));
                        if (r.SendStatus == SendStatus.SendOk)
                        {
                            ++sent;
                        }

                        evidence.Append('[').Append(SendStatusNames.Name(r.SendStatus))
                            .Append('/').Append(r.MsgId).Append(']');
                    }
                    catch (Exception e)
                    {
                        evidence.Append("[EXC:").Append(e.Message).Append(']');
                    }
                }

                prod.Shutdown();
            // the msgId is generated by the broker: a non-empty one proves the broker really
     // accepted this signed request
     Check("S4 the producer with credentials sent 3 (SendOk)", sent == 3,
                    "sent=" + sent.ToString(CultureInfo.InvariantCulture) + " " + evidence);

     // S5: the heartbeat / long poll / offset commit all carry signatures the whole way; only
     // receiving everything means the consume path passed authentication
                long deadline = NowMs() + 30000;
                while (NowMs() < deadline && listener.Count < sent)
                {
                    Thread.Sleep(200);
                }

                List<string> bodies = listener.Snapshot();
       Check("S5 the consumer with credentials received all " + sent.ToString(CultureInfo.InvariantCulture),
                    listener.Count == sent,
                    "got=" + listener.Count.ToString(CultureInfo.InvariantCulture)
                        + " bodies=" + string.Join(" ", bodies));
            }
            finally
            {
                consumer.Shutdown();
            }
        }

        // ---------------- S6 negative: a bare broker RPC without credentials must be rejected ----------------
        Console.WriteLine();
        Console.WriteLine("S6 a direct broker RPC without credentials must be rejected");
        {
            var client = new MQClientInstance("ACL_PROBE_" + stamp, new List<string> { namesrv });
            client.Start();
            try
            {
                string addr = client.BrokerAddrForTopic(topic);
                bool rejected = false;
                string detail;
                if (addr.Length == 0)
                {
                    detail = "no broker addr in route for topic " + topic;
                }
                else
                {
                    try
                    {
   // GetConsumerListByGroup is a **bare** RPC: a non-SUCCESS reply throws MQBrokerException
          // straight away. GetConsumerIdListByGroup must not be used -- it swallows the
     // exception internally and returns null, which makes "rejected" and "not found" look
  // identical to the caller (a FAIL was once read as a PASS because of this).
                        client.GetConsumerListByGroup(group, addr, 5000);
                        detail = "unexpectedly succeeded, addr=" + addr;
                    }
                    catch (MQBrokerException e)
                    {
                        detail = "addr=" + addr + " code="
                            + e.ResponseCode.ToString(CultureInfo.InvariantCulture)
                            + " " + e.ResponseMessage;
                        rejected = e.ResponseCode == NoPermission;
                    }
                    catch (Exception e)
                    {
                        detail = "addr=" + addr + " " + e.Message;
                    }
                }

  Check("S6 the broker RPC without credentials was rejected (NO_PERMISSION=16)", rejected, detail);
            }
            finally
            {
                client.Shutdown();
            }
        }

    // ---------------- S7 positive: no credentials going through the namesrv must still succeed ----------------
        Console.WriteLine();
        Console.WriteLine("S7 a route query through the NameServer without credentials must still succeed");
        {
            var client = new MQClientInstance("ACL_NS_" + stamp, new List<string> { namesrv });
            client.Start();
            bool ok = false;
            string detail = string.Empty;
            try
            {
                ok = client.UpdateTopicRouteInfoFromNameServer(topic, false, 5000);
                if (!ok)
                {
                    detail = "returned false";
                }
            }
            catch (Exception e)
            {
                detail = e.Message;
            }
            finally
            {
                client.Shutdown();
            }

  Check("S7 the route query through the namesrv without credentials succeeded", ok, detail);
        }

        Console.WriteLine();
        Console.WriteLine("===== ACL live (C#) summary =====");
        Console.WriteLine("  PASS=" + _pass.ToString(CultureInfo.InvariantCulture)
            + " FAIL=" + _fail.ToString(CultureInfo.InvariantCulture));
        if (_fail == 0)
        {
 Console.WriteLine("  result: ACL authentication (signature / rejection / namesrv compatibility) passed on a real machine");
            return 0;
        }

 Console.WriteLine("  result: " + _fail.ToString(CultureInfo.InvariantCulture) + " failures");
        return 1;
    }
}
