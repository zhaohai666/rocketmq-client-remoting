# -*- coding: utf-8 -*-
"""批量/静态 topic/读禁配/半消息/顺序配置等 admin 方法的真实集群验证。

对标 go/examples/live_admin_batch：这些方法此前在 Python 侧只有 RequestCode
常量、没有任何业务实现，报文编码错误 mock 层看不见，必须打真实 broker。

覆盖（全部打真实 broker / nameServer，无 mock）：
 1. UPDATE_AND_CREATE_TOPIC_LIST(18)：body={"topicConfigList":[...]}，header 为空
 2. UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST(225)：body={"groupConfigList":[...]}
 3. UPDATE_AND_GET_GROUP_FORBIDDEN(353)：禁读→仅查询不改动→恢复可读→回包字段
 4. RESUME_CHECK_HALF_MESSAGE(323)：非半消息返回 False 而非报错（Java 语义）
 5. createOrUpdateOrderConf：nameserver KV 的合并/覆盖语义
 6. CLEAN_EXPIRED_CONSUMEQUEUE(306) / DELETE_EXPIRED_COMMITLOG(329) + ByAddr
 7. CLEAN_UNUSED_TOPIC(316)：broker 接受即可（清理策略是 broker 侧的）
 8. QUERY_CONSUME_TIME_SPAN(303)：路由扇出聚合
 9. UPDATE/GET_NAMESRV_CONFIG(318/319)：properties 文本 + 回读

用法：scripts/with_cluster.sh python verify_admin_batch_live.py
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rocketmq.client.admin import DefaultMQAdminExt, NAMESPACE_ORDER_TOPIC_CONFIG
from rocketmq.client.exception import MQClientException
from rocketmq.common.topic_config import TopicConfig
from rocketmq.remoting.protocol.subscription import SubscriptionGroupConfig

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
            # broker_addr_table: {brokerName: {brokerId: addr}}，0 = MASTER
            if 0 in addrs and addrs[0]:
                master = addrs[0]
                break
        if not master:
            print("no master broker registered")
            return 2
        print("master=%s\n" % master)

        # ---- 1. 批量建 topic(18) ------------------------------------------
        cfg_a = TopicConfig(TOPIC_A, 4, 4)
        cfg_b = TopicConfig(TOPIC_B, 6, 6)
        try:
            admin.create_and_update_topic_config_list(master, [cfg_a, cfg_b])
            check("批量建 topic(18) 请求", True)
        except Exception as e:  # noqa: BLE001
            check("批量建 topic(18) 请求", False, repr(e))

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
        check("批量建 topic(18) 生效且队列数正确", ok, detail)

        # ---- 2. 批量建订阅组(225) ----------------------------------------
        grp_a = SubscriptionGroupConfig(GROUP_A)
        grp_a.retry_queue_nums = 3
        grp_b = SubscriptionGroupConfig(GROUP_B)
        grp_b.retry_max_times = 5
        try:
            admin.create_and_update_subscription_group_config_list(master, [grp_a, grp_b])
            check("批量建订阅组(225) 请求", True)
        except Exception as e:  # noqa: BLE001
            check("批量建订阅组(225) 请求", False, repr(e))

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
        check("批量建订阅组(225) 生效且字段正确", ok, detail)

        # ---- 3. 读禁配(353) -----------------------------------------------
        try:
            fb = admin.update_and_get_group_read_forbidden(master, GROUP_A, TOPIC_A, False)
            check("消费组读禁配(353) 设置禁读",
                  isinstance(fb, dict) and fb.get("readable") is False, repr(fb))
        except Exception as e:  # noqa: BLE001
            check("消费组读禁配(353) 设置禁读", False, repr(e))

        try:
            fb = admin.update_and_get_group_read_forbidden(master, GROUP_A, TOPIC_A, None)
            check("消费组读禁配(353) 仅查询不改动",
                  isinstance(fb, dict) and fb.get("readable") is False,
                  "禁读状态被查询调用改掉: %r" % (fb,))
        except Exception as e:  # noqa: BLE001
            check("消费组读禁配(353) 仅查询不改动", False, repr(e))

        try:
            fb = admin.update_and_get_group_read_forbidden(master, GROUP_A, TOPIC_A, True)
            check("消费组读禁配(353) 恢复可读",
                  isinstance(fb, dict) and fb.get("readable") is True, repr(fb))
        except Exception as e:  # noqa: BLE001
            check("消费组读禁配(353) 恢复可读", False, repr(e))

        check("消费组读禁配(353) 回包含 group/topic",
              isinstance(fb, dict) and fb.get("group") == GROUP_A and fb.get("topic") == TOPIC_A,
              repr(fb))

        # ---- 4. 恢复半消息(323) -------------------------------------------
        try:
            resumed = admin.resume_check_half_message(
                master, TOPIC_A, "0A0F0000000000000000000000000000000000")
            check("恢复半消息(323) 非半消息返回 False 而非报错", resumed is False,
                  "resumed=%r，对非半消息竟返回 True" % resumed)
        except Exception as e:  # noqa: BLE001
            check("恢复半消息(323) 非半消息返回 False 而非报错", False, repr(e))

        try:
            admin.resume_check_half_message(master, "", "x")
            check("恢复半消息(323) 缺 topic 被本地拒绝", False, "空 topic 竟被放行发出")
        except MQClientException:
            check("恢复半消息(323) 缺 topic 被本地拒绝", True)
        except Exception as e:  # noqa: BLE001
            check("恢复半消息(323) 缺 topic 被本地拒绝", False,
                  "应抛 MQClientException，实际 %r" % e)

        # ---- 5. 顺序 topic 配置（nameserver KV） --------------------------
        try:
            admin.create_or_update_order_conf(ORDER_KEY, "%s:5" % TOPIC_A, False)
            admin.create_or_update_order_conf(ORDER_KEY, "%s:8" % TOPIC_B, False)
            stored = admin.get_kv_config(NAMESPACE_ORDER_TOPIC_CONFIG, ORDER_KEY) or ""
            check("顺序 topic 配置 合并两条而非覆盖",
                  ("%s:5" % TOPIC_A) in stored and ("%s:8" % TOPIC_B) in stored,
                  "stored=%r" % stored)
        except Exception as e:  # noqa: BLE001
            check("顺序 topic 配置 合并两条而非覆盖", False, repr(e))

        try:
            admin.create_or_update_order_conf(ORDER_KEY, "%s:6" % TOPIC_A, False)
            stored = admin.get_kv_config(NAMESPACE_ORDER_TOPIC_CONFIG, ORDER_KEY) or ""
            check("顺序 topic 配置 同 key 覆盖旧值",
                  ("%s:6" % TOPIC_A) in stored and ("%s:5" % TOPIC_A) not in stored,
                  "stored=%r" % stored)
        except Exception as e:  # noqa: BLE001
            check("顺序 topic 配置 同 key 覆盖旧值", False, repr(e))

        # ---- 6. 清理类 ----------------------------------------------------
        try:
            admin.clean_expired_consumer_queue(master, 24 * 365)
            check("清理过期消费队列(306)", True)
        except Exception as e:  # noqa: BLE001
            check("清理过期消费队列(306)", False, repr(e))

        failed = admin.clean_expired_consumer_queue_by_addr([master], 24 * 365)
        check("清理过期消费队列(306) ByAddr", not failed, "failed=%r" % failed)

        try:
            admin.delete_expired_commit_log(master, 24 * 365)
            check("删除过期 commitlog(329)", True)
        except Exception as e:  # noqa: BLE001
            check("删除过期 commitlog(329)", False, repr(e))

        failed = admin.delete_expired_commit_log_by_addr([master], 24 * 365)
        check("删除过期 commitlog(329) ByAddr", not failed, "failed=%r" % failed)

        failed = admin.delete_expired_commit_log_by_addr(["127.0.0.1:1"], 1)
        check("清理类 ByAddr 报告失败地址", failed == ["127.0.0.1:1"],
              "dead addr should be reported, got %r" % failed)

        # ---- 7. 清理未使用 topic(316) -------------------------------------
        try:
            admin.clean_unused_topic_by_addr(master)
            check("清理未使用 topic(316)", True)
        except Exception as e:  # noqa: BLE001
            check("清理未使用 topic(316)", False, repr(e))

        # ---- 8. 消费时间跨度(303) -----------------------------------------
        try:
            spans = admin.query_consume_time_span(TOPIC_A, GROUP_A)
            check("消费时间跨度(303) 路由扇出聚合", isinstance(spans, list),
                  "spans=%d 条（空组返回空列表属正常）" % len(spans))
        except Exception as e:  # noqa: BLE001
            check("消费时间跨度(303) 路由扇出聚合", False, repr(e))

        # ---- 9. nameserver 配置(318/319) ----------------------------------
        # namesrv 的 Configuration.update 只认真实字段：未知键被**静默丢弃**
        # （nodeJs 侧 live_admin_ns 已确认），所以必须用 orderMessageEnable
        # 这个真实字段做写读回环，测完还原为默认值 false。
        try:
            before = admin.get_name_server_config()
            old_val = (before or {}).get(NAMESRV, {}).get("orderMessageEnable", "false")
            admin.update_name_server_config({"orderMessageEnable": "true"})
            got = admin.get_name_server_config()
            val = (got or {}).get(NAMESRV, {}).get("orderMessageEnable")
            check("nameserver 配置(318/319) 写读回环", val == "true",
                  "got=%r want='true'" % val)
            admin.update_name_server_config({"orderMessageEnable": old_val})
        except Exception as e:  # noqa: BLE001
            check("nameserver 配置(318/319) 写读回环", False, repr(e))

        # ---- 清理 ---------------------------------------------------------
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
