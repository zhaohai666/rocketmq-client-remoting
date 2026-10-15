# -*- coding: utf-8 -*-
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client.admin import DefaultMQAdminExt, NAMESPACE_ORDER_TOPIC_CONFIG
from client.exception import MQClientException
from common.topic_config import TopicConfig
from remoting.protocol.subscription import SubscriptionGroupConfig

NAMESRV = "127.0.0.1:9876"
STAMP = int(time.time())
TOPIC_A = "PyBatchA_%d" % STAMP
TOPIC_B = "PyBatchB_%d" % STAMP
GROUP_A = "GID_PyBatchA_%d" % STAMP
GROUP_B = "GID_PyBatchB_%d" % STAMP
ORDER_KEY = "PyOrder_%d" % STAMP

results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print("[%s] %s %s" % ("PASS" if ok else "FAIL", name, detail))


def main():
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.start()
    try:
        ci = admin.fetch_broker_cluster_info()
        master = None
        for addrs in ci.broker_addr_table.values():
            # broker_addr_table: {brokerName: {brokerId: addr}}, 0 = MASTER
            if 0 in addrs and addrs[0]:
                master = addrs[0]
                break
        if not master:
            print("no master broker registered")
            return 2
        print("master=%s\n" % master)

        # ---- 1. batch-create topic(18) ------------------------------------------
        cfg_a = TopicConfig(TOPIC_A, 4, 4)
        cfg_b = TopicConfig(TOPIC_B, 6, 6)
        try:
            admin.create_and_update_topic_config_list(master, [cfg_a, cfg_b])
            check("batch-create topic(18) request", True)
        except Exception as e:  # noqa: BLE001
            check("batch-create topic(18) request", False, repr(e))

        ok, detail = True, ""
        for cfg in (cfg_a, cfg_b):
            try:
                got = admin.examine_topic_config(master, cfg.topic_name)
                if got is None or got.read_queue_nums != cfg.read_queue_nums:
                    ok = False
                    detail = "%s readQueueNums=%s want=%s" % (
                        cfg.topic_name,
                        getattr(got, "read_queue_nums", None), cfg.read_queue_nums)
                    break
            except Exception as e:  # noqa: BLE001
                ok, detail = False, "%s: %r" % (cfg.topic_name, e)
                break
        check("batch-create topic(18) took effect with correct queue counts", ok, detail)

        # ---- 2. batch-create subscription group(225) ----------------------------------------
        grp_a = SubscriptionGroupConfig(GROUP_A)
        grp_a.retry_queue_nums = 3
        grp_b = SubscriptionGroupConfig(GROUP_B)
        grp_b.retry_max_times = 5
        try:
            admin.create_and_update_subscription_group_config_list(master, [grp_a, grp_b])
            check("batch-create subscription group(225) request", True)
        except Exception as e:  # noqa: BLE001
            check("batch-create subscription group(225) request", False, repr(e))

        ok, detail = True, ""
        for grp, field, want in ((grp_a, "retry_queue_nums", 3),
                                 (grp_b, "retry_max_times", 5)):
            try:
                got = admin.get_subscription_group_config(master, grp.group_name)
                if got is None or getattr(got, field) != want:
                    ok = False
                    detail = "%s %s=%s want=%s" % (
                        grp.group_name, field, getattr(got, field, None), want)
                    break
            except Exception as e:  # noqa: BLE001
                ok, detail = False, "%s: %r" % (grp.group_name, e)
                break
        check("batch-create subscription group(225) took effect with correct fields", ok, detail)

        # ---- 3. read-forbidden config(353) -----------------------------------------------
        try:
            fb = admin.update_and_get_group_read_forbidden(master, GROUP_A, TOPIC_A, False)
            check("consumer group read-forbidden(353) set read-forbidden",
                  isinstance(fb, dict) and fb.get("readable") is False, repr(fb))
        except Exception as e:  # noqa: BLE001
            check("consumer group read-forbidden(353) set read-forbidden", False, repr(e))

        try:
            fb = admin.update_and_get_group_read_forbidden(master, GROUP_A, TOPIC_A, None)
            check("consumer group read-forbidden(353) query-only does not change",
                  isinstance(fb, dict) and fb.get("readable") is False,
                  "the read-forbidden state was changed by the query call: %r" % (fb,))
        except Exception as e:  # noqa: BLE001
            check("consumer group read-forbidden(353) query-only does not change", False, repr(e))

        try:
            fb = admin.update_and_get_group_read_forbidden(master, GROUP_A, TOPIC_A, True)
            check("consumer group read-forbidden(353) restore readability",
                  isinstance(fb, dict) and fb.get("readable") is True, repr(fb))
        except Exception as e:  # noqa: BLE001
            check("consumer group read-forbidden(353) restore readability", False, repr(e))

        check("consumer group read-forbidden(353) response contains group/topic",
              isinstance(fb, dict) and fb.get("group") == GROUP_A and fb.get("topic") == TOPIC_A,
              repr(fb))

        # ---- 4. resume half message(323) -------------------------------------------
        try:
            resumed = admin.resume_check_half_message(
                master, TOPIC_A, "0A0F0000000000000000000000000000000000")
            check("resume half message(323) returns False rather than erroring on a non-half message", resumed is False,
                  "resumed=%r, returned True on a non-half message" % resumed)
        except Exception as e:  # noqa: BLE001
            check("resume half message(323) returns False rather than erroring on a non-half message", False, repr(e))

        try:
            admin.resume_check_half_message(master, "", "x")
            check("resume half message(323) missing topic is rejected locally", False, "an empty topic was allowed through")
        except MQClientException:
            check("resume half message(323) missing topic is rejected locally", True)
        except Exception as e:  # noqa: BLE001
            check("resume half message(323) missing topic is rejected locally", False,
                  "expected MQClientException, got %r" % e)

        # ---- 5. order topic config (nameserver KV) --------------------------
        try:
            admin.create_or_update_order_conf(ORDER_KEY, "%s:5" % TOPIC_A, False)
            admin.create_or_update_order_conf(ORDER_KEY, "%s:8" % TOPIC_B, False)
            stored = admin.get_kv_config(NAMESPACE_ORDER_TOPIC_CONFIG, ORDER_KEY) or ""
            check("order topic config merges the two entries instead of overwriting",
                  ("%s:5" % TOPIC_A) in stored and ("%s:8" % TOPIC_B) in stored,
                  "stored=%r" % stored)
        except Exception as e:  # noqa: BLE001
            check("order topic config merges the two entries instead of overwriting", False, repr(e))

        try:
            admin.create_or_update_order_conf(ORDER_KEY, "%s:6" % TOPIC_A, False)
            stored = admin.get_kv_config(NAMESPACE_ORDER_TOPIC_CONFIG, ORDER_KEY) or ""
            check("order topic config overwrites the old value on the same key",
                  ("%s:6" % TOPIC_A) in stored and ("%s:5" % TOPIC_A) not in stored,
                  "stored=%r" % stored)
        except Exception as e:  # noqa: BLE001
            check("order topic config overwrites the old value on the same key", False, repr(e))

        # ---- 6. cleanup APIs ----------------------------------------------------
        try:
            admin.clean_expired_consumer_queue(master, 24 * 365)
            check("clean expired consumer queues(306)", True)
        except Exception as e:  # noqa: BLE001
            check("clean expired consumer queues(306)", False, repr(e))

        failed = admin.clean_expired_consumer_queue_by_addr([master], 24 * 365)
        check("clean expired consumer queues(306) ByAddr", not failed, "failed=%r" % failed)

        try:
            admin.delete_expired_commit_log(master, 24 * 365)
            check("delete expired commitlog(329)", True)
        except Exception as e:  # noqa: BLE001
            check("delete expired commitlog(329)", False, repr(e))

        failed = admin.delete_expired_commit_log_by_addr([master], 24 * 365)
        check("delete expired commitlog(329) ByAddr", not failed, "failed=%r" % failed)

        failed = admin.delete_expired_commit_log_by_addr(["127.0.0.1:1"], 1)
        check("cleanup APIs ByAddr report failed addresses", failed == ["127.0.0.1:1"],
              "dead addr should be reported, got %r" % failed)

        # ---- 7. clean unused topic(316) -------------------------------------
        try:
            admin.clean_unused_topic_by_addr(master)
            check("clean unused topic(316)", True)
        except Exception as e:  # noqa: BLE001
            check("clean unused topic(316)", False, repr(e))

        # ---- 8. consume time span(303) -----------------------------------------
        try:
            spans = admin.query_consume_time_span(TOPIC_A, GROUP_A)
            check("consume time span(303) route fan-out aggregation", isinstance(spans, list),
                  "spans=%d entries (an empty group returning an empty list is normal)" % len(spans))
        except Exception as e:  # noqa: BLE001
            check("consume time span(303) route fan-out aggregation", False, repr(e))

        # ---- 9. nameserver config(318/319) ----------------------------------
        # namesrv's Configuration.update only accepts real fields: unknown keys are silently dropped
        # (confirmed on live hardware), so the write-read loop must use orderMessageEnable
        # this real field, and restore the default false afterwards.
        try:
            before = admin.get_name_server_config()
            old_val = (before or {}).get(NAMESRV, {}).get("orderMessageEnable", "false")
            admin.update_name_server_config({"orderMessageEnable": "true"})
            got = admin.get_name_server_config()
            val = (got or {}).get(NAMESRV, {}).get("orderMessageEnable")
            check("nameserver config(318/319) write-read loop", val == "true",
                  "got=%r want='true'" % val)
            admin.update_name_server_config({"orderMessageEnable": old_val})
        except Exception as e:  # noqa: BLE001
            check("nameserver config(318/319) write-read loop", False, repr(e))

        # ---- cleanup ---------------------------------------------------------
        for t in (TOPIC_A, TOPIC_B):
            try:
                admin.delete_topic_in_broker(master, t)
            except Exception:  # noqa: BLE001
                pass
        try:
            admin.delete_kv_config(NAMESPACE_ORDER_TOPIC_CONFIG, ORDER_KEY)
        except Exception:  # noqa: BLE001
            pass

        failed = [r for r in results if not r[1]]
        print("\nPASS=%d FAIL=%d" % (len(results) - len(failed), len(failed)))
        return 1 if failed else 0
    finally:
        admin.shutdown()


if __name__ == "__main__":
    sys.exit(main())
