// Everything hits a real nameServer + broker, no mocks. Covers:
//   1. Batch create topics(18): empty header, body={"topicConfigList":[...]}, effect verified
//   2. Batch create subscription groups(225): body key is groupConfigList, effect verified
//   3. Read-forbidden config(353) full chain: forbid -> query-only leaves it untouched -> restore -> response fields
//   4. Resume half message(323): a non-half-message returns false instead of erroring; missing topic rejected locally
//   5. Orderly topic config (nameserver KV read-modify-write): merge / same-key overwrite
//   6. Cleanup family: 306/329 + ByAddr + dead-address reporting; 316 single request
//   7. Consume time span(303): route fan-out aggregation
//   8. nameserver config(318/319) write-read round trip (orderMessageEnable, restored afterwards)
//   9. Static topic(513): SKIP (this suite's assertion set does not cover it)
//
// This program does not start a cluster itself; the caller must start nameServer(9876) + broker(10911).
// Usage (invoked by Program as "admin-batch-live [namesrv]").
using System;
using System.Collections.Generic;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

/// <summary>Batch-admin live-cluster integration.</summary>
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

        // ---------- 0. Cluster liveness (broker registration race: port open != registered) ----------
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
            Check("cluster liveness", false, "nameServer has no registered brokers last_err=" + lastErr);
            admin.Shutdown();
            Console.WriteLine();
            Console.WriteLine($"PASS={_gPass} FAIL={_gFail} SKIP={_gSkip}");
            return _gFail == 0 ? 0 : 1;
        }

        Console.WriteLine("cluster: master=" + brokerAddr);
        Console.WriteLine();

        // ---- 1. Batch create topics(18): body={"topicConfigList":[...]}, empty header ----
        var cfgA = new TopicConfig(topicA) { ReadQueueNums = 4, WriteQueueNums = 4 };
        var cfgB = new TopicConfig(topicB) { ReadQueueNums = 6, WriteQueueNums = 6 };
        try
        {
            admin.CreateAndUpdateTopicConfigList(brokerAddr, new List<TopicConfig> { cfgA, cfgB });
            Check("batch create topics(18) request", true);
        }
        catch (Exception e)
        {
            Check("batch create topics(18) request", false, e.Message);
        }

        // Effect verification: both topics exist and each keeps the queue counts from the batch request — the evidence
        // that the body was really accepted by the broker (a wrong key hands the broker an empty list: the request
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

            Check("batch create topics(18) took effect with correct queue counts", ok, detail);
        }

        // ---- 2. Batch create subscription groups(225): same shape, but the body key is groupConfigList ----
        var grpA = new SubscriptionGroupConfig(groupA) { RetryQueueNums = 3 };
        var grpB = new SubscriptionGroupConfig(groupB) { RetryMaxTimes = 5 };
        try
        {
            admin.CreateAndUpdateSubscriptionGroupConfigList(
                brokerAddr, new List<SubscriptionGroupConfig> { grpA, grpB });
            Check("batch create subscription groups(225) request", true);
        }
        catch (Exception e)
        {
            Check("batch create subscription groups(225) request", false, e.Message);
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

            Check("batch create subscription groups(225) took effect with correct fields", ok, detail);
        }

        // ---- 3. Read-forbidden config(353) full chain: forbid -> query-only leaves it -> restore -> response fields ----
        try
        {
            GroupForbidden fb = admin.UpdateAndGetGroupReadForbidden(brokerAddr, groupA, topicA, false);
            Check("consumer group read-forbidden(353) set to unreadable", !fb.Readable, "readable=" + fb.Readable);
        }
        catch (Exception e)
        {
            Check("consumer group read-forbidden(353) set to unreadable", false, e.Message);
        }

        try
        {
            // Query-only (readable not sent): must not flip the unreadable state back
            GroupForbidden fb = admin.UpdateAndGetGroupReadForbidden(brokerAddr, groupA, topicA);
            Check("consumer group read-forbidden(353) query-only leaves it unchanged", !fb.Readable,
                "unreadable state was changed by the query call: readable=" + fb.Readable);
        }
        catch (Exception e)
        {
            Check("consumer group read-forbidden(353) query-only leaves it unchanged", false, e.Message);
        }

        try
        {
            GroupForbidden fb = admin.UpdateAndGetGroupReadForbidden(brokerAddr, groupA, topicA, true);
            Check("consumer group read-forbidden(353) restored to readable", fb.Readable, "readable=" + fb.Readable);
        }
        catch (Exception e)
        {
            Check("consumer group read-forbidden(353) restored to readable", false, e.Message);
        }

        try
        {
            GroupForbidden fb = admin.UpdateAndGetGroupReadForbidden(brokerAddr, groupA, topicA);
            Check("consumer group read-forbidden(353) response carries group/topic",
                fb.Group == groupA && fb.Topic == topicA,
                "group=" + fb.Group + " topic=" + fb.Topic);
        }
        catch (Exception e)
        {
            Check("consumer group read-forbidden(353) response carries group/topic", false, e.Message);
        }

        try
        {
            bool resumed = admin.ResumeCheckHalfMessage(
                brokerAddr, topicA, "0A0F0000000000000000000000000000000000");
            Check("resume half message(323) non-half-message returns false instead of erroring", !resumed,
                resumed ? "resumed=true, unexpectedly true for a non-half-message" : "resumed=false");
        }
        catch (Exception e)
        {
            Check("resume half message(323) non-half-message returns false instead of erroring", false, e.Message);
        }

        try
        {
            admin.ResumeCheckHalfMessage(brokerAddr, string.Empty, "x");
            Check("resume half message(323) missing topic rejected locally", false, "an empty topic was let through");
        }
        catch (MQClientException)
        {
            Check("resume half message(323) missing topic rejected locally", true);
        }
        catch (Exception e)
        {
            Check("resume half message(323) missing topic rejected locally", false,
                "expected MQClientException, got " + e.GetType().Name + ": " + e.Message);
        }

        // ---- 5. Orderly topic config (nameserver KV, read-modify-write outside cluster mode) ------------
        // Outside cluster mode entries merge into a ";"-joined table: writing two topics under one key must keep both.
        try
        {
            admin.CreateOrUpdateOrderConf(orderKey, topicA + ":5", false);
            admin.CreateOrUpdateOrderConf(orderKey, topicB + ":8", false);
            string stored = admin.GetKvConfig(MixAll.NamespaceOrderTopicConfig, orderKey, out string v)
                ? v
                : string.Empty;
            Check("orderly topic config merges two entries instead of overwriting",
                stored.Contains(topicA + ":5") && stored.Contains(topicB + ":8"),
                "stored=" + stored);
        }
        catch (Exception e)
        {
            Check("orderly topic config merges two entries instead of overwriting", false, e.Message);
        }

        try
        {
            admin.CreateOrUpdateOrderConf(orderKey, topicA + ":6", false);
            string stored = admin.GetKvConfig(MixAll.NamespaceOrderTopicConfig, orderKey, out string v)
                ? v
                : string.Empty;
            Check("orderly topic config same key overwrites the old value",
                stored.Contains(topicA + ":6") && !stored.Contains(topicA + ":5"),
                "stored=" + stored);
        }
        catch (Exception e)
        {
            Check("orderly topic config same key overwrites the old value", false, e.Message);
        }

        // ---- 6. Cleanup family ------------------------------------------------------
        // Safe probe with a large window (1 year): the request must be accepted and delete nothing.
        try
        {
            admin.CleanExpiredConsumerQueue(brokerAddr, 24 * 365);
            Check("clean expired consume queues(306)", true);
        }
        catch (Exception e)
        {
            Check("clean expired consume queues(306)", false, e.Message);
        }

        try
        {
            List<string> failed = admin.CleanExpiredConsumerQueueByAddr(
                new List<string> { brokerAddr }, 24 * 365);
            Check("clean expired consume queues(306) ByAddr", failed.Count == 0,
                "failed=" + (failed.Count == 0 ? "[]" : failed[0]));
        }
        catch (Exception e)
        {
            Check("clean expired consume queues(306) ByAddr", false, e.Message);
        }

        try
        {
            admin.DeleteExpiredCommitLog(brokerAddr, 24 * 365);
            Check("delete expired commitlog(329)", true);
        }
        catch (Exception e)
        {
            Check("delete expired commitlog(329)", false, e.Message);
        }

        try
        {
            List<string> failed = admin.DeleteExpiredCommitLogByAddr(
                new List<string> { brokerAddr }, 24 * 365);
            Check("delete expired commitlog(329) ByAddr", failed.Count == 0,
                "failed=" + (failed.Count == 0 ? "[]" : failed[0]));
        }
        catch (Exception e)
        {
            Check("delete expired commitlog(329) ByAddr", false, e.Message);
        }

        // Dead addresses must be reported, not silently swallowed
        try
        {
            List<string> failed = admin.DeleteExpiredCommitLogByAddr(
                new List<string> { "127.0.0.1:1" }, 1);
            Check("cleanup family ByAddr reports failed addresses",
                failed.Count == 1 && failed[0] == "127.0.0.1:1",
                "dead addr should be reported");
        }
        catch (Exception e)
        {
            Check("cleanup family ByAddr reports failed addresses", false, e.Message);
        }

        // ---- 7. Clean unused topic(316): single request, PASS if the broker accepts ----
        try
        {
            admin.CleanUnusedTopicByAddr(brokerAddr);
            Check("clean unused topic(316)", true);
        }
        catch (Exception e)
        {
            Check("clean unused topic(316)", false, e.Message);
        }

        // ---- 8. Consume time span(303): route fan-out aggregation, returns the consumeTimeSpanSet array ----
        try
        {
            JsonValue spans = admin.QueryConsumeTimeSpan(topicA, groupA);
            // An empty group (never consumed) returning an empty array is normal: assert shape only, plus that the request is reachable
            Check("consume time span(303) route fan-out aggregation", spans.IsArray,
                spans.IsArray ? "spans=" + spans.Size() : "not an array");
        }
        catch (Exception e)
        {
            Check("consume time span(303) route fan-out aggregation", false, e.Message);
        }

        // ---- 9. nameserver config(318/319) write-read round trip ---------------------------
        // The namesrv's Configuration.update only accepts real fields (unknown keys are silently dropped),
        // so the round trip uses the real field orderMessageEnable and restores it afterwards.
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

            // Restore first regardless of the assertion result, to avoid polluting cluster state
            admin.UpdateNameServerConfig(new PropertyMap { ["orderMessageEnable"] = oldVal });
            Check("nameserver config(318/319) write-read round trip", val == "true",
                "got=" + val + " want='true'");
        }
        catch (Exception e)
        {
            Check("nameserver config(318/319) write-read round trip", false, e.Message);
        }

        // ---- 10. Static topic(513): broker capability differences, SKIP ----
        Skip("static topic(513)",
            "building the mapping needs full TopicQueueMappingDetail documentation (hostedBrokerIds/scope/"
            + "per-queue mappingInfo), not covered by this suite's assertion set; header wiring is locked by unit tests");

        // ---- Cleanup -----------------------------------------------------------
        foreach (string t in new[] { topicA, topicB })
        {
            try
            {
                admin.DeleteTopicInBroker(brokerAddr, t);
            }
            catch
            {
                // Cleanup failures do not affect the verdict
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
