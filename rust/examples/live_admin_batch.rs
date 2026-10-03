//! 批量/静态 topic/读禁配/半消息/顺序配置等 admin 方法对**真实 5.5.0 broker**
//! 的联调验证，对标 `go/examples/live_admin_batch` 与 `python/verify_admin_batch_live.py`。
//!
//! 这些方法此前在 Rust 侧只有 RequestCode 常量、没有任何业务实现；报文编码
//! 错误 mock 层看不见，必须打真实 broker。
//!
//! 用法（先起集群）：`cargo run --example live_admin_batch -- 127.0.0.1:9876`
use std::env;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{DefaultMQAdminExt, NAMESPACE_ORDER_TOPIC_CONFIG};
use rocketmq_client_remoting::common::topic_config::TopicConfig;
use rocketmq_client_remoting::remoting::protocol::ext_fields::StringMap;
use rocketmq_client_remoting::remoting::protocol::subscription::SubscriptionGroupConfig;

struct Checker {
    passed: u32,
    failed: Vec<String>,
}

impl Checker {
    fn check(&mut self, name: &str, cond: bool, detail: &str) {
        if cond {
            self.passed += 1;
            println!("PASS  {name}");
        } else {
            println!("FAIL  {name} - {detail}");
            self.failed.push(format!("{name} - {detail}"));
        }
    }
}

fn report(ck: &Checker) -> ExitCode {
    println!("\nPASS={} FAIL={}", ck.passed, ck.failed.len());
    if ck.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[tokio::main]
async fn main() -> ExitCode {
    let argv: Vec<String> = env::args().collect();
    let namesrv = argv
        .get(1)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "127.0.0.1:9876".to_string());
    let stamp = now_millis();
    let topic_a = format!("RsBatchA_{stamp}");
    let topic_b = format!("RsBatchB_{stamp}");
    let group_a = format!("GID_RsBatchA_{stamp}");
    let group_b = format!("GID_RsBatchB_{stamp}");
    let order_key = format!("RsOrder_{stamp}");
    let order_ns_key = NAMESPACE_ORDER_TOPIC_CONFIG;

    let admin = DefaultMQAdminExt::new();
    admin.set_namesrv_addr(&namesrv);
    admin.set_instance_name("RS_ADMIN_BATCH_LIVE");
    if let Err(e) = admin.start().await {
        eprintln!("admin start: {e}");
        return ExitCode::FAILURE;
    }
    let mut ck = Checker { passed: 0, failed: Vec::new() };

    let cluster = match admin.fetch_broker_cluster_info().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("fetchBrokerClusterInfo: {e}");
            admin.shutdown();
            return ExitCode::FAILURE;
        }
    };
    // broker_addr_table: [(brokerName, [(brokerId, addr)])]，0 = MASTER
    let master = cluster
        .broker_addr_table
        .iter()
        .flat_map(|(_, addrs)| addrs.iter())
        .find(|(id, addr)| *id == 0 && !addr.is_empty())
        .map(|(_, addr)| addr.clone());
    let Some(master) = master else {
        eprintln!("no master broker registered");
        admin.shutdown();
        return ExitCode::FAILURE;
    };
    println!("master={master}\n");

    // ---- 1. 批量建 topic(18) ----------------------------------------------
    let mut cfg_a = TopicConfig::new(&topic_a);
    cfg_a.read_queue_nums = 4;
    cfg_a.write_queue_nums = 4;
    let mut cfg_b = TopicConfig::new(&topic_b);
    cfg_b.read_queue_nums = 6;
    cfg_b.write_queue_nums = 6;
    match admin
        .create_and_update_topic_config_list(&master, &[cfg_a.clone(), cfg_b.clone()])
        .await
    {
        Ok(()) => ck.check("批量建 topic(18) 请求", true, ""),
        Err(e) => ck.check("批量建 topic(18) 请求", false, &e.to_string()),
    }
    let mut ok = true;
    let mut detail = String::new();
    for cfg in [&cfg_a, &cfg_b] {
        match admin.examine_topic_config(&master, &cfg.topic_name).await {
            Ok(got) if got.read_queue_nums == cfg.read_queue_nums => {}
            Ok(got) => {
                ok = false;
                detail = format!(
                    "{} readQueueNums={} want={}",
                    cfg.topic_name, got.read_queue_nums, cfg.read_queue_nums
                );
            }
            Err(e) => {
                ok = false;
                detail = format!("{}: {e}", cfg.topic_name);
            }
        }
    }
    ck.check("批量建 topic(18) 生效且队列数正确", ok, &detail);

    // ---- 2. 批量建订阅组(225) ---------------------------------------------
    let mut grp_a = SubscriptionGroupConfig::new(&group_a);
    grp_a.retry_queue_nums = 3;
    let mut grp_b = SubscriptionGroupConfig::new(&group_b);
    grp_b.retry_max_times = 5;
    match admin
        .create_and_update_subscription_group_config_list(&master, &[grp_a.clone(), grp_b.clone()])
        .await
    {
        Ok(()) => ck.check("批量建订阅组(225) 请求", true, ""),
        Err(e) => ck.check("批量建订阅组(225) 请求", false, &e.to_string()),
    }
    let mut ok = true;
    let mut detail = String::new();
    for (grp, field, want) in [
        (&grp_a, "retry_queue_nums", 3i32),
        (&grp_b, "retry_max_times", 5i32),
    ] {
        match admin.get_subscription_group_config(&master, &grp.group_name).await {
            Ok(Some(got)) => {
                let actual = match field {
                    "retry_queue_nums" => got.retry_queue_nums,
                    _ => got.retry_max_times,
                };
                if actual != want {
                    ok = false;
                    detail = format!("{} {field}={actual} want={want}", grp.group_name);
                }
            }
            Ok(None) => {
                ok = false;
                detail = format!("{}: not found", grp.group_name);
            }
            Err(e) => {
                ok = false;
                detail = format!("{}: {e}", grp.group_name);
            }
        }
    }
    ck.check("批量建订阅组(225) 生效且字段正确", ok, &detail);

    // ---- 3. 读禁配(353) ----------------------------------------------------
    match admin
        .update_and_get_group_read_forbidden(&master, &group_a, &topic_a, Some(false))
        .await
    {
        Ok(fb) => ck.check(
            "消费组读禁配(353) 设置禁读",
            fb.get("readable").and_then(serde_json::Value::as_bool) == Some(false),
            &format!("{fb}"),
        ),
        Err(e) => ck.check("消费组读禁配(353) 设置禁读", false, &e.to_string()),
    }
    match admin
        .update_and_get_group_read_forbidden(&master, &group_a, &topic_a, None)
        .await
    {
        Ok(fb) => ck.check(
            "消费组读禁配(353) 仅查询不改动",
            fb.get("readable").and_then(serde_json::Value::as_bool) == Some(false),
            &format!("禁读状态被查询调用改掉: {fb}"),
        ),
        Err(e) => ck.check("消费组读禁配(353) 仅查询不改动", false, &e.to_string()),
    }
    match admin
        .update_and_get_group_read_forbidden(&master, &group_a, &topic_a, Some(true))
        .await
    {
        Ok(fb) => {
            let readable = fb.get("readable").and_then(serde_json::Value::as_bool) == Some(true);
            let fields_ok = fb.get("group").and_then(serde_json::Value::as_str) == Some(group_a.as_str())
                && fb.get("topic").and_then(serde_json::Value::as_str) == Some(topic_a.as_str());
            ck.check("消费组读禁配(353) 恢复可读", readable, &format!("{fb}"));
            ck.check("消费组读禁配(353) 回包含 group/topic", fields_ok, &format!("{fb}"));
        }
        Err(e) => {
            ck.check("消费组读禁配(353) 恢复可读", false, &e.to_string());
            ck.check("消费组读禁配(353) 回包含 group/topic", false, &e.to_string());
        }
    }

    // ---- 4. 恢复半消息(323) ------------------------------------------------
    match admin
        .resume_check_half_message(&master, &topic_a, "0A0F0000000000000000000000000000000000")
        .await
    {
        // Java 语义：非半消息 → broker 拒绝（SYSTEM_ERROR）→ 返回 False 而非报错
        Ok(false) => ck.check("恢复半消息(323) 非半消息返回 false 而非报错", true, ""),
        Ok(true) => ck.check("恢复半消息(323) 非半消息返回 false 而非报错", false, "对非半消息竟返回 true"),
        Err(e) => ck.check("恢复半消息(323) 非半消息返回 false 而非报错", false, &e.to_string()),
    }
    match admin.resume_check_half_message(&master, "", "x").await {
        Err(e) => ck.check(
            "恢复半消息(323) 缺 topic 被本地拒绝",
            e.to_string().contains("topic required"),
            &format!("应本地拒绝，实际 {e}"),
        ),
        Ok(_) => ck.check("恢复半消息(323) 缺 topic 被本地拒绝", false, "空 topic 竟被放行发出"),
    }

    // ---- 5. 顺序 topic 配置（nameserver KV） -------------------------------
    match admin
        .create_or_update_order_conf(&order_key, &format!("{topic_a}:5"), false)
        .await
    {
        Ok(()) => ck.check("顺序 topic 配置 首次写入", true, ""),
        Err(e) => ck.check("顺序 topic 配置 首次写入", false, &e.to_string()),
    }
    match admin
        .create_or_update_order_conf(&order_key, &format!("{topic_b}:8"), false)
        .await
    {
        Ok(()) => {
            let stored = admin.get_kv_config(order_ns_key, &order_key).await.ok().flatten().unwrap_or_default();
            ck.check(
                "顺序 topic 配置 合并两条而非覆盖",
                stored.contains(&format!("{topic_a}:5")) && stored.contains(&format!("{topic_b}:8")),
                &format!("stored={stored:?}"),
            );
        }
        Err(e) => ck.check("顺序 topic 配置 合并两条而非覆盖", false, &e.to_string()),
    }
    match admin
        .create_or_update_order_conf(&order_key, &format!("{topic_a}:6"), false)
        .await
    {
        Ok(()) => {
            let stored = admin.get_kv_config(order_ns_key, &order_key).await.ok().flatten().unwrap_or_default();
            ck.check(
                "顺序 topic 配置 同 key 覆盖旧值",
                stored.contains(&format!("{topic_a}:6")) && !stored.contains(&format!("{topic_a}:5")),
                &format!("stored={stored:?}"),
            );
        }
        Err(e) => ck.check("顺序 topic 配置 同 key 覆盖旧值", false, &e.to_string()),
    }

    // ---- 6. 清理类 ---------------------------------------------------------
    match admin.clean_expired_consumer_queue(&master, 24 * 365).await {
        Ok(()) => ck.check("清理过期消费队列(306)", true, ""),
        Err(e) => ck.check("清理过期消费队列(306)", false, &e.to_string()),
    }
    let failed = admin
        .clean_expired_consumer_queue_by_addr(&[master.clone()], 24 * 365)
        .await;
    ck.check("清理过期消费队列(306) ByAddr", failed.is_empty(), &format!("failed={failed:?}"));

    match admin.delete_expired_commit_log(&master, 24 * 365).await {
        Ok(()) => ck.check("删除过期 commitlog(329)", true, ""),
        Err(e) => ck.check("删除过期 commitlog(329)", false, &e.to_string()),
    }
    let failed = admin
        .delete_expired_commit_log_by_addr(&[master.clone()], 24 * 365)
        .await;
    ck.check("删除过期 commitlog(329) ByAddr", failed.is_empty(), &format!("failed={failed:?}"));

    let failed = admin
        .delete_expired_commit_log_by_addr(&["127.0.0.1:1".to_string()], 1)
        .await;
    ck.check(
        "清理类 ByAddr 报告失败地址",
        failed == vec!["127.0.0.1:1".to_string()],
        &format!("dead addr should be reported, got {failed:?}"),
    );

    // ---- 7. 清理未使用 topic(316) ------------------------------------------
    match admin.clean_unused_topic_by_addr(&master).await {
        Ok(()) => ck.check("清理未使用 topic(316)", true, ""),
        Err(e) => ck.check("清理未使用 topic(316)", false, &e.to_string()),
    }

    // ---- 8. 消费时间跨度(303) ----------------------------------------------
    match admin.query_consume_time_span(&topic_a, &group_a).await {
        Ok(spans) => ck.check(
            "消费时间跨度(303) 路由扇出聚合",
            !spans.is_empty(),
            &format!("spans={} 条", spans.len()),
        ),
        Err(e) => ck.check("消费时间跨度(303) 路由扇出聚合", false, &e.to_string()),
    }

    // ---- 9. nameserver 配置(318/319) ---------------------------------------
    // namesrv 的 Configuration.update 只认真实字段：未知键被**静默丢弃**
    // （nodeJs/Go/Python 侧均已确认），所以用 orderMessageEnable 做回环并还原。
    match admin.get_name_server_config(None).await {
        Ok(before) => {
            let old_val = before
                .iter()
                .find(|(addr, _)| addr == &namesrv)
                .and_then(|(_, props)| props.get("orderMessageEnable").map(str::to_string))
                .unwrap_or_else(|| "false".to_string());
            let mut props = StringMap::new();
            props.insert("orderMessageEnable".to_string(), "true".to_string());
            match admin.update_name_server_config(&props, None).await {
                Ok(()) => match admin.get_name_server_config(None).await {
                    Ok(after) => {
                        let val = after
                            .iter()
                            .find(|(addr, _)| addr == &namesrv)
                            .and_then(|(_, p)| p.get("orderMessageEnable").map(str::to_string));
                        ck.check(
                            "nameserver 配置(318/319) 写读回环",
                            val.as_deref() == Some("true"),
                            &format!("got={val:?} want='true'"),
                        );
                    }
                    Err(e) => ck.check("nameserver 配置(318/319) 写读回环", false, &e.to_string()),
                },
                Err(e) => ck.check("nameserver 配置(318/319) 写读回环", false, &e.to_string()),
            }
            let mut restore = StringMap::new();
            restore.insert("orderMessageEnable".to_string(), old_val.clone());
            let _ = admin.update_name_server_config(&restore, None).await;
        }
        Err(e) => ck.check("nameserver 配置(318/319) 写读回环", false, &e.to_string()),
    }

    // ---- 清理 ---------------------------------------------------------------
    let _ = admin.delete_topic_in_broker(&master, &topic_a);
    let _ = admin.delete_topic_in_broker(&master, &topic_b);
    let _ = admin.delete_kv_config(order_ns_key, &order_key);
    admin.shutdown();
    report(&ck)
}
