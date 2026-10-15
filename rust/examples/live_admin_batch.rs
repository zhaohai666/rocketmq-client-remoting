//! Live verification of admin methods such as batch ops / static topic / read-forbidden / half-message / order
//! config against a **real 5.5.0 broker**.
//!
//! Previously these methods existed on the Rust side only as RequestCode constants, with no business
//! implementation; a wire-encoding mistake is invisible to the mock layer, so a real broker is required.
//!
//! Usage (start the cluster first): `cargo run --example live_admin_batch -- 127.0.0.1:9876`
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
    let mut ck = Checker {
        passed: 0,
        failed: Vec::new(),
    };

    let cluster = match admin.fetch_broker_cluster_info().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("fetchBrokerClusterInfo: {e}");
            admin.shutdown();
            return ExitCode::FAILURE;
        }
    };
    // broker_addr_table: [(brokerName, [(brokerId, addr)])], 0 = MASTER
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

    // ---- 1. batch create topics(18) ----------------------------------------------
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
        Ok(()) => ck.check("batch create topics(18) request", true, ""),
        Err(e) => ck.check("batch create topics(18) request", false, &e.to_string()),
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
    ck.check("batch create topics(18) effective with correct queue counts", ok, &detail);

    // ---- 2. batch create subscription groups(225) ---------------------------------------------
    let mut grp_a = SubscriptionGroupConfig::new(&group_a);
    grp_a.retry_queue_nums = 3;
    let mut grp_b = SubscriptionGroupConfig::new(&group_b);
    grp_b.retry_max_times = 5;
    match admin
        .create_and_update_subscription_group_config_list(&master, &[grp_a.clone(), grp_b.clone()])
        .await
    {
        Ok(()) => ck.check("batch create subscription groups(225) request", true, ""),
        Err(e) => ck.check("batch create subscription groups(225) request", false, &e.to_string()),
    }
    let mut ok = true;
    let mut detail = String::new();
    for (grp, field, want) in [
        (&grp_a, "retry_queue_nums", 3i32),
        (&grp_b, "retry_max_times", 5i32),
    ] {
        match admin
            .get_subscription_group_config(&master, &grp.group_name)
            .await
        {
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
    ck.check("batch create subscription groups(225) effective with correct fields", ok, &detail);

    // ---- 3. read-forbidden(353) ----------------------------------------------------
    match admin
        .update_and_get_group_read_forbidden(&master, &group_a, &topic_a, Some(false))
        .await
    {
        Ok(fb) => ck.check(
            "consumer group read-forbidden(353) set unreadable",
            fb.get("readable").and_then(serde_json::Value::as_bool) == Some(false),
            &format!("{fb}"),
        ),
        Err(e) => ck.check("consumer group read-forbidden(353) set unreadable", false, &e.to_string()),
    }
    match admin
        .update_and_get_group_read_forbidden(&master, &group_a, &topic_a, None)
        .await
    {
        Ok(fb) => ck.check(
            "consumer group read-forbidden(353) query only, no change",
            fb.get("readable").and_then(serde_json::Value::as_bool) == Some(false),
            &format!("read-forbidden state changed by the query call: {fb}"),
        ),
        Err(e) => ck.check("consumer group read-forbidden(353) query only, no change", false, &e.to_string()),
    }
    match admin
        .update_and_get_group_read_forbidden(&master, &group_a, &topic_a, Some(true))
        .await
    {
        Ok(fb) => {
            let readable = fb.get("readable").and_then(serde_json::Value::as_bool) == Some(true);
            let fields_ok = fb.get("group").and_then(serde_json::Value::as_str)
                == Some(group_a.as_str())
                && fb.get("topic").and_then(serde_json::Value::as_str) == Some(topic_a.as_str());
            ck.check("consumer group read-forbidden(353) restore readable", readable, &format!("{fb}"));
            ck.check(
                "consumer group read-forbidden(353) response carries group/topic",
                fields_ok,
                &format!("{fb}"),
            );
        }
        Err(e) => {
            ck.check("consumer group read-forbidden(353) restore readable", false, &e.to_string());
            ck.check(
                "consumer group read-forbidden(353) response carries group/topic",
                false,
                &e.to_string(),
            );
        }
    }

    // ---- 4. resume half message(323) ------------------------------------------------
    match admin
        .resume_check_half_message(&master, &topic_a, "0A0F0000000000000000000000000000000000")
        .await
    {
        Ok(false) => ck.check("resume half message(323) non-half-message returns false instead of an error", true, ""),
        Ok(true) => ck.check(
            "resume half message(323) non-half-message returns false instead of an error",
            false,
            "returned true for a non-half-message",
        ),
        Err(e) => ck.check(
            "resume half message(323) non-half-message returns false instead of an error",
            false,
            &e.to_string(),
        ),
    }
    match admin.resume_check_half_message(&master, "", "x").await {
        Err(e) => ck.check(
            "resume half message(323) missing topic rejected locally",
            e.to_string().contains("topic required"),
            &format!("expected a local rejection, got {e}"),
        ),
        Ok(_) => ck.check(
            "resume half message(323) missing topic rejected locally",
            false,
            "an empty topic was let through",
        ),
    }

    // ---- 5. order topic config (nameserver KV) -------------------------------
    match admin
        .create_or_update_order_conf(&order_key, &format!("{topic_a}:5"), false)
        .await
    {
        Ok(()) => ck.check("order topic config first write", true, ""),
        Err(e) => ck.check("order topic config first write", false, &e.to_string()),
    }
    match admin
        .create_or_update_order_conf(&order_key, &format!("{topic_b}:8"), false)
        .await
    {
        Ok(()) => {
            let stored = admin
                .get_kv_config(order_ns_key, &order_key)
                .await
                .ok()
                .flatten()
                .unwrap_or_default();
            ck.check(
                "order topic config merges two entries instead of overwriting",
                stored.contains(&format!("{topic_a}:5"))
                    && stored.contains(&format!("{topic_b}:8")),
                &format!("stored={stored:?}"),
            );
        }
        Err(e) => ck.check("order topic config merges two entries instead of overwriting", false, &e.to_string()),
    }
    match admin
        .create_or_update_order_conf(&order_key, &format!("{topic_a}:6"), false)
        .await
    {
        Ok(()) => {
            let stored = admin
                .get_kv_config(order_ns_key, &order_key)
                .await
                .ok()
                .flatten()
                .unwrap_or_default();
            ck.check(
                "order topic config same key overwrites the old value",
                stored.contains(&format!("{topic_a}:6"))
                    && !stored.contains(&format!("{topic_a}:5")),
                &format!("stored={stored:?}"),
            );
        }
        Err(e) => ck.check("order topic config same key overwrites the old value", false, &e.to_string()),
    }

    // ---- 6. cleanup operations ---------------------------------------------------------
    match admin.clean_expired_consumer_queue(&master, 24 * 365).await {
        Ok(()) => ck.check("clean expired consumer queue(306)", true, ""),
        Err(e) => ck.check("clean expired consumer queue(306)", false, &e.to_string()),
    }
    let failed = admin
        .clean_expired_consumer_queue_by_addr(std::slice::from_ref(&master), 24 * 365)
        .await;
    ck.check(
        "clean expired consumer queue(306) ByAddr",
        failed.is_empty(),
        &format!("failed={failed:?}"),
    );

    match admin.delete_expired_commit_log(&master, 24 * 365).await {
        Ok(()) => ck.check("delete expired commitlog(329)", true, ""),
        Err(e) => ck.check("delete expired commitlog(329)", false, &e.to_string()),
    }
    let failed = admin
        .delete_expired_commit_log_by_addr(std::slice::from_ref(&master), 24 * 365)
        .await;
    ck.check(
        "delete expired commitlog(329) ByAddr",
        failed.is_empty(),
        &format!("failed={failed:?}"),
    );

    let failed = admin
        .delete_expired_commit_log_by_addr(&["127.0.0.1:1".to_string()], 1)
        .await;
    ck.check(
        "cleanup ByAddr reports failed addresses",
        failed == vec!["127.0.0.1:1".to_string()],
        &format!("dead addr should be reported, got {failed:?}"),
    );

    // ---- 7. clean unused topics(316) ------------------------------------------
    match admin.clean_unused_topic_by_addr(&master).await {
        Ok(()) => ck.check("clean unused topics(316)", true, ""),
        Err(e) => ck.check("clean unused topics(316)", false, &e.to_string()),
    }

    // ---- 8. consume time span(303) ----------------------------------------------
    match admin.query_consume_time_span(&topic_a, &group_a).await {
        Ok(spans) => ck.check(
            "consume time span(303) aggregated over the route fan-out",
            !spans.is_empty(),
            &format!("spans={} entries", spans.len()),
        ),
        Err(e) => ck.check("consume time span(303) aggregated over the route fan-out", false, &e.to_string()),
    }

    // ---- 9. nameserver config(318/319) ---------------------------------------
    // The namesrv Configuration.update only accepts real fields: unknown keys are **silently dropped**,
    // so orderMessageEnable is used for a write-read loop and then restored.
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
                            "nameserver config(318/319) write-read loop",
                            val.as_deref() == Some("true"),
                            &format!("got={val:?} want='true'"),
                        );
                    }
                    Err(e) => ck.check("nameserver config(318/319) write-read loop", false, &e.to_string()),
                },
                Err(e) => ck.check("nameserver config(318/319) write-read loop", false, &e.to_string()),
            }
            let mut restore = StringMap::new();
            restore.insert("orderMessageEnable".to_string(), old_val.clone());
            let _ = admin.update_name_server_config(&restore, None).await;
        }
        Err(e) => ck.check("nameserver config(318/319) write-read loop", false, &e.to_string()),
    }

    // ---- cleanup ---------------------------------------------------------------
    // Note the `await`: these three are real cleanup RPCs; dropping the future without awaiting it means they never ran.
    let _ = admin.delete_topic_in_broker(&master, &topic_a).await;
    let _ = admin.delete_topic_in_broker(&master, &topic_b).await;
    let _ = admin.delete_kv_config(order_ns_key, &order_key).await;
    admin.shutdown();
    report(&ck)
}
