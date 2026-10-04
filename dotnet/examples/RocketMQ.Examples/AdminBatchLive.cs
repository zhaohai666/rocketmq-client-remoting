// C# 批量 admin 方法（Java 有实现、此前 dotnet 缺失的 15 个）的真实集群联调
// （对应 cpp/examples/admin_batch_live.cpp，断言口径同 python/verify_admin_batch_live.py）。
//
// 全部打真实 nameServer + broker，无 mock。覆盖：
//   1. 批量建 topic(18)：header 为空、body={"topicConfigList":[...]}，生效校验
//   2. 批量建订阅组(225)：body key 是 groupConfigList，生效校验
//   3. 读禁配(353) 全链：禁读 → 仅查询不改动 → 恢复可读 → 回包字段
//   4. 恢复半消息(323)：非半消息返回 false 而非报错；缺 topic 本地拒绝
//   5. 顺序 topic 配置（nameserver KV 读改写）：合并/同 key 覆盖
//   6. 清理类：306/329 + ByAddr + 死地址报告；316 单请求
//   7. 消费时间跨度(303)：路由扇出聚合
//   8. nameserver 配置(318/319) 写读回环（orderMessageEnable，测完还原）
//   9. 静态 topic(513)：SKIP（参考集未覆盖）
//
// 本程序自身不启动集群；调用方需先启动 nameServer(9876) + broker(10911)。
// 用法（由 Program 以 "admin-batch-live [namesrv]" 形式调用）。
using System;
using System.Collections.Generic;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

/// <summary>批量 admin 真实集群联调（与 cpp/examples/admin_batch_live.cpp 对齐）。</summary>
internal static class AdminBatchLive
{
    private static int _gPass;
    private static int _gFail;
    private static int _gSkip;

    private static void Check(string name, bool ok, string detail = "")
    {
        if (ok) ++_gPass; else ++_gFail;
        Console.WriteLine("[" + (ok ? "PASS" : "FAIL") + "] " + name
            + (detail.Length > 0 ? "  " + detail : string.Empty));
    }

    private static void Skip(string name, string detail)
    {
        ++_gSkip;
        Console.WriteLine("[SKIP] " + name + "  " + detail);
    }

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        long stampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        string stamp = stampMs.ToString(System.Globalization.CultureInfo.InvariantCulture);
        string topicA = "NetBatchA_" + stamp;
        string topicB = "NetBatchB_" + stamp;
        string groupA = "GID_NetBatchA_" + stamp;
        string groupB = "GID_NetBatchB_" + stamp;
        string orderKey = "NetOrder_" + stamp;

        var admin = new DefaultMQAdminExt();
        admin.SetNamesrvAddr(namesrv);
        admin.SetTimeoutMillis(10000);
        try
        {
            admin.Start();
        }
        catch (Exception e)
        {
            Console.WriteLine("[FATAL] admin start failed: " + e.Message);
            return 1;
        }

        // ---------- 0. 集群探活（broker 注册竞态：端口开 != 已注册）----------
        string brokerAddr = string.Empty;
        string lastErr = string.Empty;
        for (int i = 0; i < 40; ++i)
        {
            try
            {
                ClusterInfo ci = admin.FetchBrokerClusterInfo();
                List<string> addrs = ci.GetBrokerAddrs();
                if (addrs.Count > 0)
                {
                    brokerAddr = addrs[0];
                    break;
                }
            }
            catch (Exception e)
            {
                lastErr = e.Message;
            }

            Thread.Sleep(1000);
        }

        if (brokerAddr.Length == 0)
        {
            Check("集群探活", false, "nameServer 无 broker 注册 last_err=" + lastErr);
            admin.Shutdown();
            Console.WriteLine();
            Console.WriteLine($"PASS={_gPass} FAIL={_gFail} SKIP={_gSkip}");
            return _gFail == 0 ? 0 : 1;
        }

        Console.WriteLine("cluster: master=" + brokerAddr);
        Console.WriteLine();

        // ---- 1. 批量建 topic(18)：body={"topicConfigList":[...]}，header 为空 ----
        var cfgA = new TopicConfig(topicA) { ReadQueueNums = 4, WriteQueueNums = 4 };
        var cfgB = new TopicConfig(topicB) { ReadQueueNums = 6, WriteQueueNums = 6 };
        try
        {
            admin.CreateAndUpdateTopicConfigList(brokerAddr, new List<TopicConfig> { cfgA, cfgB });
            Check("批量建 topic(18) 请求", true);
        }
        catch (Exception e)
        {
            Check("批量建 topic(18) 请求", false, e.Message);
        }

        // 生效校验：两个 topic 都存在，且各自保住批量请求的队列数 —— 这是 body 真被
        // broker 认下的证据（键名写错时 broker 收到空列表，请求"成功"但什么都不建）。
        {
            bool ok = true;
            string detail = string.Empty;
            foreach (TopicConfig cfg in new[] { cfgA, cfgB })
            {
                try
                {
                    TopicConfigSerializeWrapper wrapper = admin.GetAllTopicConfig(brokerAddr);
                    if (!wrapper.TopicConfigTable.TryGetValue(cfg.TopicName, out TopicConfig? got)
                        || got is null)
                    {
                        ok = false;
                        detail = cfg.TopicName + ": not found";
                        break;
                    }

                    if (got.ReadQueueNums != cfg.ReadQueueNums)
                    {
                        ok = false;
                        detail = cfg.TopicName + " readQueueNums=" + got.ReadQueueNums
                            + " want=" + cfg.ReadQueueNums;
                        break;
                    }
                }
                catch (Exception e)
                {
                    ok = false;
                    detail = cfg.TopicName + ": " + e.Message;
                    break;
                }
            }

            Check("批量建 topic(18) 生效且队列数正确", ok, detail);
        }

        // ---- 2. 批量建订阅组(225)：同形，但 body key 是 groupConfigList ----
        var grpA = new SubscriptionGroupConfig(groupA) { RetryQueueNums = 3 };
        var grpB = new SubscriptionGroupConfig(groupB) { RetryMaxTimes = 5 };
        try
        {
            admin.CreateAndUpdateSubscriptionGroupConfigList(
                brokerAddr, new List<SubscriptionGroupConfig> { grpA, grpB });
            Check("批量建订阅组(225) 请求", true);
        }
        catch (Exception e)
        {
            Check("批量建订阅组(225) 请求", false, e.Message);
        }

        {
            bool ok = true;
            string detail = string.Empty;
            foreach ((SubscriptionGroupConfig cfg, int want) in
                new[] { (grpA, 3), (grpB, 5) })
            {
                if (!admin.GetSubscriptionGroupConfig(brokerAddr, cfg.GroupName,
                        out SubscriptionGroupConfig got) || got is null)
                {
                    ok = false;
                    detail = cfg.GroupName + ": not found";
                    break;
                }

                int actual = ReferenceEquals(cfg, grpA) ? got.RetryQueueNums : got.RetryMaxTimes;
                if (actual != want)
                {
                    ok = false;
                    string field = ReferenceEquals(cfg, grpA) ? "retryQueueNums" : "retryMaxTimes";
                    detail = cfg.GroupName + " " + field + "=" + actual + " want=" + want;
                    break;
                }
            }

            Check("批量建订阅组(225) 生效且字段正确", ok, detail);
        }

        // ---- 3. 读禁配(353) 全链：禁读 → 仅查询不改动 → 恢复可读 → 回包字段 ----
        try
        {
            GroupForbidden fb = admin.UpdateAndGetGroupReadForbidden(brokerAddr, groupA, topicA, false);
            Check("消费组读禁配(353) 设置禁读", !fb.Readable, "readable=" + fb.Readable);
        }
        catch (Exception e)
        {
            Check("消费组读禁配(353) 设置禁读", false, e.Message);
        }

        try
        {
            // 仅查询（readable 不带）：不能把禁读状态翻回去
            GroupForbidden fb = admin.UpdateAndGetGroupReadForbidden(brokerAddr, groupA, topicA);
            Check("消费组读禁配(353) 仅查询不改动", !fb.Readable,
                "禁读状态被查询调用改掉: readable=" + fb.Readable);
        }
        catch (Exception e)
        {
            Check("消费组读禁配(353) 仅查询不改动", false, e.Message);
        }

        try
        {
            GroupForbidden fb = admin.UpdateAndGetGroupReadForbidden(brokerAddr, groupA, topicA, true);
            Check("消费组读禁配(353) 恢复可读", fb.Readable, "readable=" + fb.Readable);
        }
        catch (Exception e)
        {
            Check("消费组读禁配(353) 恢复可读", false, e.Message);
        }

        try
        {
            GroupForbidden fb = admin.UpdateAndGetGroupReadForbidden(brokerAddr, groupA, topicA);
            Check("消费组读禁配(353) 回包含 group/topic",
                fb.Group == groupA && fb.Topic == topicA,
                "group=" + fb.Group + " topic=" + fb.Topic);
        }
        catch (Exception e)
        {
            Check("消费组读禁配(353) 回包含 group/topic", false, e.Message);
        }

        // ---- 4. 恢复半消息(323) ---------------------------------------------
        // Java（MQClientAPIImpl:3279）对非 SUCCESS 返回 false 而非抛错：对非半消息
        // （broker 表现为 SYSTEM_ERROR）的良构调用必须得到 false，而不是异常。
        try
        {
            bool resumed = admin.ResumeCheckHalfMessage(
                brokerAddr, topicA, "0A0F0000000000000000000000000000000000");
            Check("恢复半消息(323) 非半消息返回 false 而非报错", !resumed,
                resumed ? "resumed=true，对非半消息竟返回 true" : "resumed=false");
        }
        catch (Exception e)
        {
            Check("恢复半消息(323) 非半消息返回 false 而非报错", false, e.Message);
        }

        try
        {
            admin.ResumeCheckHalfMessage(brokerAddr, string.Empty, "x");
            Check("恢复半消息(323) 缺 topic 被本地拒绝", false, "空 topic 竟被放行发出");
        }
        catch (MQClientException)
        {
            Check("恢复半消息(323) 缺 topic 被本地拒绝", true);
        }
        catch (Exception e)
        {
            Check("恢复半消息(323) 缺 topic 被本地拒绝", false,
                "应抛 MQClientException，实际 " + e.GetType().Name + ": " + e.Message);
        }

        // ---- 5. 顺序 topic 配置（nameserver KV，非集群模式读改写）------------
        // 非集群模式合并进 ";" 连接的条目表：同 key 写两个 topic 必须两条都在。
        try
        {
            admin.CreateOrUpdateOrderConf(orderKey, topicA + ":5", false);
            admin.CreateOrUpdateOrderConf(orderKey, topicB + ":8", false);
            string stored = admin.GetKvConfig(MixAll.NamespaceOrderTopicConfig, orderKey, out string v)
                ? v
                : string.Empty;
            Check("顺序 topic 配置 合并两条而非覆盖",
                stored.Contains(topicA + ":5") && stored.Contains(topicB + ":8"),
                "stored=" + stored);
        }
        catch (Exception e)
        {
            Check("顺序 topic 配置 合并两条而非覆盖", false, e.Message);
        }

        try
        {
            admin.CreateOrUpdateOrderConf(orderKey, topicA + ":6", false);
            string stored = admin.GetKvConfig(MixAll.NamespaceOrderTopicConfig, orderKey, out string v)
                ? v
                : string.Empty;
            Check("顺序 topic 配置 同 key 覆盖旧值",
                stored.Contains(topicA + ":6") && !stored.Contains(topicA + ":5"),
                "stored=" + stored);
        }
        catch (Exception e)
        {
            Check("顺序 topic 配置 同 key 覆盖旧值", false, e.Message);
        }

        // ---- 6. 清理类 ------------------------------------------------------
        // 大窗口（1 年）的安全探针：请求必须被接受且什么都不删。
        try
        {
            admin.CleanExpiredConsumerQueue(brokerAddr, 24 * 365);
            Check("清理过期消费队列(306)", true);
        }
        catch (Exception e)
        {
            Check("清理过期消费队列(306)", false, e.Message);
        }

        try
        {
            List<string> failed = admin.CleanExpiredConsumerQueueByAddr(
                new List<string> { brokerAddr }, 24 * 365);
            Check("清理过期消费队列(306) ByAddr", failed.Count == 0,
                "failed=" + (failed.Count == 0 ? "[]" : failed[0]));
        }
        catch (Exception e)
        {
            Check("清理过期消费队列(306) ByAddr", false, e.Message);
        }

        try
        {
            admin.DeleteExpiredCommitLog(brokerAddr, 24 * 365);
            Check("删除过期 commitlog(329)", true);
        }
        catch (Exception e)
        {
            Check("删除过期 commitlog(329)", false, e.Message);
        }

        try
        {
            List<string> failed = admin.DeleteExpiredCommitLogByAddr(
                new List<string> { brokerAddr }, 24 * 365);
            Check("删除过期 commitlog(329) ByAddr", failed.Count == 0,
                "failed=" + (failed.Count == 0 ? "[]" : failed[0]));
        }
        catch (Exception e)
        {
            Check("删除过期 commitlog(329) ByAddr", false, e.Message);
        }

        // 死地址必须被报告而不是被静默吞掉
        try
        {
            List<string> failed = admin.DeleteExpiredCommitLogByAddr(
                new List<string> { "127.0.0.1:1" }, 1);
            Check("清理类 ByAddr 报告失败地址",
                failed.Count == 1 && failed[0] == "127.0.0.1:1",
                "dead addr should be reported");
        }
        catch (Exception e)
        {
            Check("清理类 ByAddr 报告失败地址", false, e.Message);
        }

        // ---- 7. 清理未使用 topic(316)：单请求，broker 接受即 PASS ----
        try
        {
            admin.CleanUnusedTopicByAddr(brokerAddr);
            Check("清理未使用 topic(316)", true);
        }
        catch (Exception e)
        {
            Check("清理未使用 topic(316)", false, e.Message);
        }

        // ---- 8. 消费时间跨度(303)：路由扇出聚合，返回 consumeTimeSpanSet 数组 ----
        try
        {
            JsonValue spans = admin.QueryConsumeTimeSpan(topicA, groupA);
            // 空组（从未消费）返回空数组属正常：只断言形态正确、请求全链可达
            Check("消费时间跨度(303) 路由扇出聚合", spans.IsArray,
                spans.IsArray ? "spans=" + spans.Size() : "非数组");
        }
        catch (Exception e)
        {
            Check("消费时间跨度(303) 路由扇出聚合", false, e.Message);
        }

        // ---- 9. nameserver 配置(318/319) 写读回环 ---------------------------
        // namesrv 的 Configuration.update 只认真实字段（未知 key 被静默丢弃），
        // 所以用 orderMessageEnable 这个真实字段做回环，测完还原。
        try
        {
            string oldVal = "false";
            foreach (KeyValuePair<string, PropertyMap> kv in admin.GetNameServerConfig())
            {
                if (kv.Value.TryGetValue("orderMessageEnable", out string? ov) && ov is not null)
                {
                    oldVal = ov;
                }
            }

            admin.UpdateNameServerConfig(new PropertyMap { ["orderMessageEnable"] = "true" });
            string val = string.Empty;
            foreach (KeyValuePair<string, PropertyMap> kv in admin.GetNameServerConfig())
            {
                if (kv.Value.TryGetValue("orderMessageEnable", out string? ov) && ov is not null)
                {
                    val = ov;
                }
            }

            // 无论断言结果如何先还原，避免污染集群状态
            admin.UpdateNameServerConfig(new PropertyMap { ["orderMessageEnable"] = oldVal });
            Check("nameserver 配置(318/319) 写读回环", val == "true",
                "got=" + val + " want='true'");
        }
        catch (Exception e)
        {
            Check("nameserver 配置(318/319) 写读回环", false, e.Message);
        }

        // ---- 10. 静态 topic(513)：端能力差异，SKIP ----
        Skip("静态 topic(513)",
            "mapping 构造需完整 TopicQueueMappingDetail 文档（hostedBrokerIds/scope/"
            + "每队列 mappingInfo），Python/Go 参考断言集均未覆盖；header 布线由单测锁定");

        // ---- 清理 -----------------------------------------------------------
        foreach (string t in new[] { topicA, topicB })
        {
            try
            {
                admin.DeleteTopicInBroker(brokerAddr, t);
            }
            catch
            {
                // 清理失败不影响结论
            }
        }

        try
        {
            admin.DeleteKvConfig(MixAll.NamespaceOrderTopicConfig, orderKey);
        }
        catch
        {
        }

        admin.Shutdown();
        Console.WriteLine();
        Console.WriteLine($"PASS={_gPass} FAIL={_gFail} SKIP={_gSkip}");
        return _gFail == 0 ? 0 : 1;
    }
}
