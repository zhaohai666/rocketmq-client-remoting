// ACL 鉴权真机验证（对齐 Java AclClientRPCHook；与 cpp/examples/live_acl.cpp、
// python/verify_acl_live.py 同场景）。
// 用法：rmq acl [namesrv] [accessKey] [secretKey]
//
// 前置：broker 开了 `authenticationEnabled=true`，并用 `initAuthenticationUser`
//       建了 accessKey/secretKey 对应的 SUPER 用户（见 /tmp/run_acl_live.sh 的
//       broker_acl.conf）。此时**所有** broker RPC 都要求合法签名：
//       AuthConfig.isAuthenticationRequired() = authenticationEnabled && !whitelist.contains(rpc)，
//       而白名单默认为空。
//
// 场景：
//   S1 正向：带凭据的 admin 建 topic → 成功（管理路径签名被接受）
//   S2 反向：不带凭据的 admin 建 topic → 被拒（broker NO_PERMISSION=16）
//   S3 反向：secretKey 错误的生产者发送 → 被拒（NO_PERMISSION=16）
//   S4 正向：带凭据的生产者发 3 条 → SendOk（msgId 由 broker 赋值）
//   S5 正向：带凭据的消费者收满 3 条（心跳 / 长轮询拉取 / 位点提交都带签名）
//            ⚠ S4/S5 必须**先起消费者再发送**，否则 CONSUME_FROM_LAST_OFFSET 的初始位点
//              语义 + broker 异步分发会让同一时序在三语言间结果不一致（见 S4/S5 处注释）
//   S6 反向：不带凭据直连 broker 的裸 RPC → 被拒（NO_PERMISSION=16）
//   S7 正向：不带凭据走 NameServer 路由查询仍成功（钩子只作用于需要鉴权的目标）
//
// 关键事实（不要凭记忆改）：broker 侧所有鉴权失败都抛
// AbortProcessException(NO_PERMISSION=16)（broker/auth/pipeline/AuthenticationPipeline.java:53）；
// 失败原因用 ResponseCode 区分不了，只能看 remark：
//   - 签名不对 → "check signature failed."（DefaultAuthenticationHandler:68）
//   - 凭据缺失 → "User:null is not found."（DefaultAuthenticationHandler:60）
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;

namespace RocketMQ.Examples;

public static class LiveAcl
{
    // broker 侧鉴权失败的统一响应码。
    private const int NoPermission = 16;

    // CreateTopicInRoute 会把底层 MQBrokerException 的 Message 拼进异常，形如
    // "create new topic failed: CODE: 16 DESC: ..."，故用这个标记识别 NO_PERMISSION。
    // 由 NoPermission 派生，避免两处数字各写一遍后漂移。
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
        Console.WriteLine("ACL live (.NET): namesrv=" + namesrv + " topic=" + topic
            + " group=" + group + " ak=" + ak);
        Console.WriteLine("======================================================================");

        // ---------------- S1 正向：带凭据 admin 建 topic ----------------
        Console.WriteLine();
        Console.WriteLine("S1 带凭据 admin 建 topic");
        {
            var admin = new DefaultMQAdminExt("ACL_ADMIN_OK");
            admin.SetNamesrvAddr(namesrv);
            admin.SetCredentials(ak, sk);
            admin.Start();
            try
            {
                admin.CreateTopic("TBW102", topic, 4);
                Check("S1 带凭据 admin 建 topic 成功", true);
            }
            catch (Exception e)
            {
                Check("S1 带凭据 admin 建 topic 成功", false, e.Message);
            }
            finally
            {
                admin.Shutdown();
            }
        }

        // ---------------- S2 反向：无凭据 admin 必须被拒 ----------------
        Console.WriteLine();
        Console.WriteLine("S2 不带凭据的 admin 建 topic 必须被拒");
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

            Check("S2 无凭据 admin 被 broker 拒绝(NO_PERMISSION=16)", rejected, detail);
        }

        // ---------------- S3 反向：错误 secretKey 必须被拒 ----------------
        Console.WriteLine();
        Console.WriteLine("S3 错误 secretKey 的生产者必须被拒");
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

            Check("S3 错误 secretKey 被 broker 拒绝(NO_PERMISSION=16)", rejected, detail);
        }

        // ---------------- S4 正向：带凭据生产者发送 ----------------
        // ---------------- S4/S5 正向：**先起消费者再发送** ----------------
        // 顺序不能反：Java 默认 CONSUME_FROM_LAST_OFFSET 把新消费组的初始位点解析成该队列
        // **当时的** maxOffset（RebalancePushImpl.java:174-190），所以"先发、后起消费者"
        // 会（正确地）一条都收不到；而 broker 的 consumequeue 是异步分发/刷盘的，
        // "刚发完立刻查 maxOffset"还可能读到 0 —— 同一时序在三种语言间结果不一致
        // （实测 Python 收 0 条、C++/.NET 收 3 条）。先起消费者才是确定性的、只测 ACL 的顺序。
        int sent = 0;
        Console.WriteLine();
        Console.WriteLine("S4/S5 带正确凭据的生产者/消费者（先起消费者再发送）");
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
                // 等 rebalance 把队列分下来：初始位点必须在 topic 还空着的时候解析
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
                // msgId 由 broker 生成：非空即证明 broker 真的接受了这条签名请求
                Check("S4 带凭据生产者发送 3 条(SendOk)", sent == 3,
                    "sent=" + sent.ToString(CultureInfo.InvariantCulture) + " " + evidence);

                // S5：心跳 / 长轮询拉取 / 位点提交全程带签名，收满才算消费链路鉴权通过
                long deadline = NowMs() + 30000;
                while (NowMs() < deadline && listener.Count < sent)
                {
                    Thread.Sleep(200);
                }

                List<string> bodies = listener.Snapshot();
                Check("S5 带凭据消费者收满 " + sent.ToString(CultureInfo.InvariantCulture) + " 条",
                    listener.Count == sent,
                    "got=" + listener.Count.ToString(CultureInfo.InvariantCulture)
                        + " bodies=" + string.Join(" ", bodies));
            }
            finally
            {
                consumer.Shutdown();
            }
        }

        // ---------------- S6 反向：无凭据裸 broker RPC 必须被拒 ----------------
        Console.WriteLine();
        Console.WriteLine("S6 不带凭据的直接 broker RPC 必须被拒");
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
                        // GetConsumerListByGroup 是**裸** RPC：非 SUCCESS 直接抛 MQBrokerException。
                        // 不能用 GetConsumerIdListByGroup —— 它内部吞掉异常返回 null，
                        // 「被拒绝」与「查不到」在调用方看来一模一样（曾因此把 FAIL 看成 PASS）。
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

                Check("S6 无凭据 broker RPC 被拒绝(NO_PERMISSION=16)", rejected, detail);
            }
            finally
            {
                client.Shutdown();
            }
        }

        // ---------------- S7 正向：无凭据走 namesrv 仍应成功 ----------------
        Console.WriteLine();
        Console.WriteLine("S7 不带凭据走 NameServer 路由查询仍应成功");
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

            Check("S7 无凭据 namesrv 路由查询成功", ok, detail);
        }

        Console.WriteLine();
        Console.WriteLine("===== ACL live (.NET) summary =====");
        Console.WriteLine("  PASS=" + _pass.ToString(CultureInfo.InvariantCulture)
            + " FAIL=" + _fail.ToString(CultureInfo.InvariantCulture));
        if (_fail == 0)
        {
            Console.WriteLine("  result: ACL 鉴权（签名 / 拒绝 / namesrv 兼容）真机通过");
            return 0;
        }

        Console.WriteLine("  result: " + _fail.ToString(CultureInfo.InvariantCulture) + " 项失败");
        return 1;
    }
}
