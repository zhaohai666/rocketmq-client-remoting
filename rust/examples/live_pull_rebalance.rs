//! Live verification of the pull-side rebalance (Java `DefaultMQPullConsumer.enableRebalance`),
//! against a real RocketMQ 5.5.1 cluster.
//!
//! Java's pull consumer rebalances: `DefaultMQPullConsumer.java:97` defaults `enableRebalance`
//! to **true**, and the shared `RebalanceService` thread drives `tryRebalance()` every 20s,
//! calling `MessageQueueListener.messageQueueChanged(topic, mqAll, mqDivided)` **when the
//! allocation changed**. This probe proves against a real broker:
//!
//!   L1  two instances of one group split a multi-queue topic -- disjoint shares whose
//!       union is the whole topic (only possible if both read the same broker-side
//!       consumer list and run the same allocation formula);
//!   L2  the listener really fired with `mqAll` = the topic's full queue set;
//!   L3  `enableRebalance=false` stops the loop (no callbacks, empty table) while
//!       `fetch_message_queues_in_balance` keeps computing on the spot.
//!
//! Single broker, one topic with 8 queues -- enough for all three: the split is across
//! *queues* within one broker, not across brokers.
//!
//! Usage: cargo run --example live_pull_rebalance -- <namesrv>   (e.g. 127.0.0.1:9876)

use std::collections::{BTreeMap, BTreeSet};
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::DefaultMQAdminExt;
use rocketmq_client_remoting::client::consumer::MessageQueueListener;
use rocketmq_client_remoting::client::pull_consumer::DefaultMQPullConsumer;
use rocketmq_client_remoting::common::message::MessageQueue;
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;

const N_QUEUE: i32 = 8;
// Shorter than Java's 20s so the probe finishes quickly; the loop shape is the point.
const WAIT_MILLIS: u64 = 3000;
const MIN_MILLIS: u64 = 500;

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

struct Recorder {
    calls: Mutex<RecorderState>,
}

#[derive(Default)]
struct RecorderState {
    calls: u64,
    last_all: Vec<MessageQueue>,
    last_divided: Vec<MessageQueue>,
}

impl Recorder {
    fn new() -> Recorder {
        Recorder {
            calls: Mutex::new(RecorderState::default()),
        }
    }

    fn count(&self) -> u64 {
        self.calls.lock().unwrap().calls
    }

    fn last_all_size(&self) -> usize {
        self.calls.lock().unwrap().last_all.len()
    }

    fn last_divided_ids(&self) -> BTreeSet<String> {
        let state = self.calls.lock().unwrap();
        state.last_divided.iter().map(id_of).collect()
    }
}

impl MessageQueueListener for Recorder {
    fn message_queue_changed(
        &self,
        _topic: &str,
        mq_all: &[MessageQueue],
        mq_divided: &[MessageQueue],
    ) {
        let mut state = self.calls.lock().unwrap();
        state.calls += 1;
        state.last_all = mq_all.to_vec();
        state.last_divided = mq_divided.to_vec();
    }
}

// Identity by (broker_name, queue_id): the share comparisons must go through the identity
// tuple, not through object identity.
fn id_of(mq: &MessageQueue) -> String {
    format!("{}#{}", mq.broker_name, mq.queue_id)
}

async fn make_consumer(
    ns: &str,
    inst: &str,
    group: &str,
    topic: &str,
    rec: Option<Arc<Recorder>>,
    enable: bool,
) -> Result<DefaultMQPullConsumer, Box<dyn std::error::Error>> {
    let c = DefaultMQPullConsumer::new(group)?;
    // A distinct instanceName gives a distinct clientId; the clientId is what the broker's
    // GET_CONSUMER_LIST_BY_GROUP returns, so without this both facades would be one client.
    c.set_instance_name(inst);
    c.set_namesrv_addr(ns);
    c.set_rebalance_wait_millis(WAIT_MILLIS);
    c.set_rebalance_min_interval_millis(MIN_MILLIS);
    c.register_topic(topic);
    if let Some(rec) = rec {
        c.set_message_queue_listener(rec);
    }
    c.set_enable_rebalance(enable);
    c.start().await?;
    Ok(c)
}

#[tokio::main]
async fn main() -> ExitCode {
    let ns = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9876".to_string());
    let prefix = format!("PullRebLiveRs_{}", now_millis() % 1_000_000_000);
    let topic = format!("{}Topic", prefix);
    let group = format!("{}Group", prefix);

    let mut pass: u32 = 0;
    let mut fail: u32 = 0;
    let mut check = |name: &str, ok: bool, detail: String| {
        if ok {
            pass += 1;
        } else {
            fail += 1;
        }
        println!(
            "  {} {}{}",
            if ok { "[PASS]" } else { "[FAIL]" },
            name,
            if detail.is_empty() {
                String::new()
            } else {
                format!("  {}", detail)
            }
        );
    };

    println!(
        "{}: pull-side rebalance live (ns={}, topic={}, {} queues)",
        prefix, ns, topic, N_QUEUE
    );

    // Create the topic up front: a consumer gets no route for an uncreated topic (TBW102
    // is a producer-side fallback only). Then wait until the name server really serves
    // the topic's own route with all 8 read queues.
    {
        let admin = DefaultMQAdminExt::new();
        admin.set_namesrv_addr(&ns);
        if let Err(e) = admin.start().await {
            println!("FATAL admin start: {}", e);
            return ExitCode::from(2);
        }
        if let Err(e) = admin.create_topic("TBW102", &topic, N_QUEUE, 0).await {
            println!("FATAL create_topic: {}", e);
            return ExitCode::from(2);
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut ready = false;
        while tokio::time::Instant::now() < deadline {
            match admin.examine_topic_route(&topic).await {
                Ok(TopicRouteData { queue_datas, .. }) => {
                    let queues: i32 = queue_datas.iter().map(|qd| qd.read_queue_nums).sum();
                    if queues >= N_QUEUE {
                        ready = true;
                        break;
                    }
                }
                Err(_) => {}
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        admin.shutdown();
        if !ready {
            println!("FATAL route for {} never showed {} queues", topic, N_QUEUE);
            return ExitCode::from(2);
        }
    }
    println!("  created {} with {} queues", topic, N_QUEUE);

    let rec_a = Arc::new(Recorder::new());
    let rec_b = Arc::new(Recorder::new());
    let a = match make_consumer(&ns, &format!("{}InstA", prefix), &group, &topic, Some(rec_a.clone()), true).await {
        Ok(c) => c,
        Err(e) => {
            println!("FATAL consumer A start: {}", e);
            return ExitCode::from(2);
        }
    };
    let b = match make_consumer(&ns, &format!("{}InstB", prefix), &group, &topic, Some(rec_b.clone()), true).await {
        Ok(c) => c,
        Err(e) => {
            println!("FATAL consumer B start: {}", e);
            return ExitCode::from(2);
        }
    };

    let table = |c: &DefaultMQPullConsumer| -> Vec<MessageQueue> {
        let t: BTreeMap<String, Vec<MessageQueue>> = c.rebalance_table();
        t.get(&topic).cloned().unwrap_or_default()
    };

    println!("  started two instances, waiting for the first rebalance rounds...");

    // L1: both must end up with a non-empty share of the topic.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut got = false;
    while tokio::time::Instant::now() < deadline {
        if !table(&a).is_empty() && !table(&b).is_empty() {
            got = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let share_a = table(&a);
    let share_b = table(&b);
    println!(
        "  INFO share A={} queues, share B={} queues",
        share_a.len(),
        share_b.len()
    );
    check(
        "L1-both instances ended up with a share of the topic",
        got,
        format!("A={} B={}", share_a.len(), share_b.len()),
    );

    let ids_a: BTreeSet<String> = share_a.iter().map(id_of).collect();
    let ids_b: BTreeSet<String> = share_b.iter().map(id_of).collect();
    let overlap = ids_a.intersection(&ids_b).count();
    check(
        "L1-the two shares are disjoint (one queue is never owned twice)",
        overlap == 0,
        format!("overlap={}", overlap),
    );
    let mut union = ids_a.clone();
    union.extend(ids_b.iter().cloned());
    let full = union.len() == N_QUEUE as usize
        && (0..N_QUEUE).all(|i| union.contains(&format!("broker-a#{}", i)));
    check(
        "L1-the two shares cover every queue of the topic exactly once",
        full,
        format!("union={} of {}", union.len(), N_QUEUE),
    );

    // L2: the listener really ran, and mqAll was the topic's full queue set -- proof that
    // the allocation was computed against the real route, not invented locally.
    check(
        "L2-the allocation change was reported to the listener",
        rec_a.count() >= 1 && rec_b.count() >= 1,
        format!("A calls={} B calls={}", rec_a.count(), rec_b.count()),
    );
    check(
        "L2-messageQueueChanged carried the topic's full queue set as mqAll",
        rec_a.last_all_size() == N_QUEUE as usize,
        format!("mqAll={}", rec_a.last_all_size()),
    );
    {
        let divided = rec_a.last_divided_ids();
        let same = divided.len() == ids_a.len() && divided.iter().all(|id| ids_a.contains(id));
        check(
            "L2-mqDivided matches what this instance reports as its share",
            same,
            format!("mqDivided={} table={}", divided.len(), ids_a.len()),
        );
    }

    // The table is the background rebalance's product, so fetch_message_queues_in_balance
    // must now answer from it (Java reads the process-queue keys).
    match a.fetch_message_queues_in_balance(&topic).await {
        Ok(in_balance) => {
            let in_balance_ids: BTreeSet<String> = in_balance.iter().map(id_of).collect();
            let same =
                in_balance_ids.len() == ids_a.len() && in_balance_ids.iter().all(|id| ids_a.contains(id));
            check(
                "L1-fetch_message_queues_in_balance answers from the rebalance table",
                same,
                format!("got={} expect={}", in_balance_ids.len(), ids_a.len()),
            );
        }
        Err(e) => check(
            "L1-fetch_message_queues_in_balance answers from the rebalance table",
            false,
            format!("error={}", e),
        ),
    }

    // L3: the switch really gates the loop.
    let rec_c = Arc::new(Recorder::new());
    let c = match make_consumer(&ns, &format!("{}InstC", prefix), &group, &topic, Some(rec_c.clone()), false).await {
        Ok(c) => c,
        Err(e) => {
            println!("FATAL consumer C start: {}", e);
            return ExitCode::from(2);
        }
    };
    println!("  started a third instance with enableRebalance=false, waiting 2 rounds...");
    tokio::time::sleep(Duration::from_millis(WAIT_MILLIS * 2 + 4000)).await;
    check(
        "L3-enableRebalance=false stops the loop: no callback at all",
        rec_c.count() == 0,
        format!("calls={}", rec_c.count()),
    );
    check(
        "L3-enableRebalance=false leaves the rebalance table empty",
        c.rebalance_table().is_empty(),
        format!("table size={}", c.rebalance_table().len()),
    );
    match c.fetch_message_queues_in_balance(&topic).await {
        Ok(on_the_spot) => check(
            "L3-with the loop off, fetch_message_queues_in_balance still computes on the spot",
            !on_the_spot.is_empty(),
            format!("got={} queues", on_the_spot.len()),
        ),
        Err(e) => check(
            "L3-with the loop off, fetch_message_queues_in_balance still computes on the spot",
            false,
            format!("error={}", e),
        ),
    }

    c.shutdown();
    b.shutdown();
    a.shutdown();

    println!("\n{}: {} passed, {} failed", prefix, pass, fail);
    if fail > 0 {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
