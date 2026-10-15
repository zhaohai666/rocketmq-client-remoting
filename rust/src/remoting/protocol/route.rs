//! Route payload models: `QueueData` / `BrokerData` /
//! `TopicRouteData`. These three types are the body of the nameserver `GET_ROUTE_INFO_BY_TOPIC(105)` response,
//! and they are the entire data structure of the client route table.
//!
//! ## Three wire behaviors that must be copied exactly

use std::cmp::Ordering;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value};

use super::admin_body::{
    expect_object, jarray, jboolean, jentries, jfield, jint, jstring, jstring_or, json_object,
    MessageQueueKey,
};
use super::serialize::RemotingSerializable;
use crate::common::mix_all::MixAll;
use crate::common::sysflag::PermName;
use crate::error::{Error, Result};

// ---------------------------------------------------------------- QueueData

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct QueueData {
    pub broker_name: String,
    pub read_queue_nums: i32,
    pub write_queue_nums: i32,
    pub perm: i32,
    pub topic_sys_flag: i32,
}

impl QueueData {
    pub fn new(
        broker_name: impl Into<String>,
        read_queue_nums: i32,
        write_queue_nums: i32,
        perm: i32,
        topic_sys_flag: i32,
    ) -> QueueData {
        QueueData {
            broker_name: broker_name.into(),
            read_queue_nums,
            write_queue_nums,
            perm,
            topic_sys_flag,
        }
    }

    /// Sort comparator: compares only brokerName.
    pub fn compare_to(&self, other: &QueueData) -> Ordering {
        self.broker_name.cmp(&other.broker_name)
    }

    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            ("brokerName", Value::String(self.broker_name.clone())),
            ("readQueueNums", Value::from(self.read_queue_nums)),
            ("writeQueueNums", Value::from(self.write_queue_nums)),
            ("perm", Value::from(self.perm)),
            ("topicSysFlag", Value::from(self.topic_sys_flag)),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<QueueData> {
        expect_object(value, "QueueData")?;
        Ok(QueueData {
            broker_name: jstring_or(value, "brokerName", ""),
            read_queue_nums: jint(value, "readQueueNums", 0),
            write_queue_nums: jint(value, "writeQueueNums", 0),
            perm: jint(value, "perm", 0),
            topic_sys_flag: jint(value, "topicSysFlag", 0),
        })
    }
}

// ---------------------------------------------------------------- BrokerData

/// `broker_addrs` uses `Vec<(i64, String)>` instead of `HashMap`: the brokerId order in the
/// payload must be preserved, otherwise both the candidate order of "randomly pick a slave" and the encoded bytes would drift.
#[derive(Debug, Clone, Default)]
pub struct BrokerData {
    pub cluster: String,
    pub broker_name: String,
    pub broker_addrs: Vec<(i64, String)>,
    pub zone_name: String,
    pub enable_acting_master: bool,
}

impl BrokerData {
    pub fn new(
        cluster: impl Into<String>,
        broker_name: impl Into<String>,
        broker_addrs: Vec<(i64, String)>,
        zone_name: impl Into<String>,
    ) -> BrokerData {
        BrokerData {
            cluster: cluster.into(),
            broker_name: broker_name.into(),
            broker_addrs,
            zone_name: zone_name.into(),
            enable_acting_master: false,
        }
    }

    pub fn select_broker_addr(&self) -> Option<String> {
        let master_id = MixAll::MASTER_ID as i64;
        if let Some((_, addr)) = self.broker_addrs.iter().find(|(id, _)| *id == master_id) {
            return Some(addr.clone());
        }
        self.broker_addrs
            .get(pseudo_random_index(self.broker_addrs.len()))
            .map(|(_, addr)| addr.clone())
    }

    /// Sort comparator: compares only brokerName.
    pub fn compare_to(&self, other: &BrokerData) -> Ordering {
        self.broker_name.cmp(&other.broker_name)
    }

    pub fn to_json_value(&self) -> Value {
        let mut addrs = Map::new();
        for (id, addr) in &self.broker_addrs {
            addrs.insert(id.to_string(), Value::String(addr.clone()));
        }
        json_object(vec![
            ("cluster", Value::String(self.cluster.clone())),
            ("brokerName", Value::String(self.broker_name.clone())),
            ("brokerAddrs", Value::Object(addrs)),
            ("zoneName", Value::String(self.zone_name.clone())),
            ("enableActingMaster", Value::Bool(self.enable_acting_master)),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<BrokerData> {
        expect_object(value, "BrokerData")?;
        let mut addrs = Vec::new();
        for (k, v) in jentries(value, "brokerAddrs")? {
            // The payload has `"0"` (or fastjson2's bare `0`, which also parses back as a string).
            // Dirty keys (no number parses out) are skipped instead of raising, so one usable route is not invalidated wholesale.
            let id = match k.trim().parse::<i64>() {
                Ok(id) => id,
                Err(_) => continue,
            };
            let addr = match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            addrs.push((id, addr));
        }
        Ok(BrokerData {
            cluster: jstring_or(value, "cluster", ""),
            broker_name: jstring_or(value, "brokerName", ""),
            broker_addrs: addrs,
            zone_name: jstring_or(value, "zoneName", ""),
            // enableActingMaster reads as false for both missing key and false value
            enable_acting_master: jboolean(value, "enableActingMaster", false),
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<BrokerData> {
        BrokerData::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

impl PartialEq for BrokerData {
    fn eq(&self, other: &BrokerData) -> bool {
        self.cluster == other.cluster
            && self.broker_name == other.broker_name
            && self.broker_addrs == other.broker_addrs
    }
}

impl Eq for BrokerData {}

/// No `rand` dependency: nanosecond clock + process-level counter fed into xorshift64; the only
/// requirement is "not the same pick every time", load balancing does not need cryptographic quality.
pub(crate) fn pseudo_random_index(bound: usize) -> usize {
    if bound <= 1 {
        return 0;
    }
    static COUNTER: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut x = COUNTER.fetch_add(0x5174_C57B_92B6_53AE, AtomicOrdering::Relaxed) ^ nanos;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    (x.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 32) as usize % bound
}

// ---------------------------------------------------------------- TopicRouteData

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TopicRouteData {
    pub order_topic_conf: Option<String>,
    pub queue_datas: Vec<QueueData>,
    pub broker_datas: Vec<BrokerData>,
    /// brokerAddr -> filterServer list, preserving payload order.
    pub filter_server_table: Vec<(String, Vec<String>)>,
    /// brokerName -> `TopicQueueMappingInfo`; the protocol layer does not model that structure, the JSON passes through as is.
    /// `None` (key absent) and `Some(empty)` (`{}`) differ on the wire and must not be merged.
    pub topic_queue_mapping_by_broker: Option<Vec<(String, Value)>>,
}

impl TopicRouteData {
    pub fn to_json_value(&self) -> Value {
        let mut pairs = vec![
            (
                "orderTopicConf",
                match &self.order_topic_conf {
                    Some(v) => Value::String(v.clone()),
                    None => Value::Null,
                },
            ),
            (
                "queueDatas",
                Value::Array(self.queue_datas.iter().map(|q| q.to_json_value()).collect()),
            ),
            (
                "brokerDatas",
                Value::Array(self.broker_datas.iter().map(|b| b.to_json_value()).collect()),
            ),
            ("filterServerTable", encode_string_list_map(&self.filter_server_table)),
        ];
        if let Some(mapping) = &self.topic_queue_mapping_by_broker {
            let mut map = Map::new();
            for (broker, info) in mapping {
                map.insert(broker.clone(), info.clone());
            }
            pairs.push(("topicQueueMappingByBroker", Value::Object(map)));
        }
        json_object(pairs)
    }

    pub fn from_json_value(value: &Value) -> Result<TopicRouteData> {
        expect_object(value, "TopicRouteData")?;
        let mut queue_datas = Vec::new();
        for item in jarray(value, "queueDatas")? {
            queue_datas.push(QueueData::from_json_value(item)?);
        }
        let mut broker_datas = Vec::new();
        for item in jarray(value, "brokerDatas")? {
            broker_datas.push(BrokerData::from_json_value(item)?);
        }
        let mut filter_server_table = Vec::new();
        for (addr, servers) in jentries(value, "filterServerTable")? {
            let items = servers
                .as_array()
                .ok_or_else(|| Error::Decode("filterServerTable is not a json array".into()))?;
            filter_server_table.push((
                addr.clone(),
                items
                    .iter()
                    .map(|v| match v {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .collect(),
            ));
        }
        // `topicQueueMappingByBroker`: missing key / null are both None,
        // an explicit `{}` is Some(empty), and the key must be written back when re-encoding.
        let topic_queue_mapping_by_broker = match jfield(value, "topicQueueMappingByBroker") {
            None => None,
            Some(Value::Object(map)) => Some(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
            Some(_) => {
                return Err(Error::Decode(
                    "topicQueueMappingByBroker is not a json object".into(),
                ))
            }
        };
        Ok(TopicRouteData {
            order_topic_conf: jstring(value, "orderTopicConf"),
            queue_datas,
            broker_datas,
            filter_server_table,
            topic_queue_mapping_by_broker,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<TopicRouteData> {
        TopicRouteData::from_json_value(&RemotingSerializable::decode(data)?)
    }

    pub fn get_broker_datas(&self) -> &[BrokerData] {
        &self.broker_datas
    }

    /// `topic` must be the real topic: after send/pull the route table is looked up again with `mq.topic`,
    /// and a wrong backfill produces "route exists but lookup finds nothing".
    pub fn get_all_message_queue(&self, topic: &str) -> Vec<MessageQueueKey> {
        let mut mqs = Vec::new();
        for qd in &self.queue_datas {
            if !PermName::check_perm(qd.perm, PermName::PERM_WRITE) {
                continue;
            }
            let Some(bd) = self
                .broker_datas
                .iter()
                .find(|bd| bd.broker_name == qd.broker_name)
            else {
                continue;
            };
            if !bd
                .broker_addrs
                .iter()
                .any(|(id, _)| *id == MixAll::MASTER_ID as i64)
            {
                continue;
            }
            for i in 0..qd.write_queue_nums {
                mqs.push(MessageQueueKey::new(topic, &qd.broker_name, i));
            }
        }
        mqs
    }

    pub fn get_all_subscribe_message_queue(&self, topic: &str) -> Vec<MessageQueueKey> {
        let mut mqs = Vec::new();
        for qd in &self.queue_datas {
            if !PermName::check_perm(qd.perm, PermName::PERM_READ) {
                continue;
            }
            for i in 0..qd.read_queue_nums {
                mqs.push(MessageQueueKey::new(topic, &qd.broker_name, i));
            }
        }
        mqs
    }

    pub fn clone_topic_route_data(&self) -> TopicRouteData {
        TopicRouteData {
            order_topic_conf: self.order_topic_conf.clone(),
            queue_datas: self.queue_datas.clone(),
            broker_datas: self.broker_datas.clone(),
            filter_server_table: self
                .filter_server_table
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            topic_queue_mapping_by_broker: self.topic_queue_mapping_by_broker.clone(),
        }
    }

    /// Whether the route changed (the short-circuit condition of `updateTopicRouteInfoFromNameServer`).
    pub fn topic_route_data_changed(&self, old: Option<&TopicRouteData>) -> bool {
        let Some(old) = old else {
            return true;
        };
        let sort_queues = |list: &[QueueData]| {
            let mut sorted: Vec<QueueData> = list.to_vec();
            // The sort key item by item is `(broker_name, read, write, perm)`;
            // topicSysFlag is excluded so a broker that only changes sysFlag is not misjudged as a route change.
            sorted.sort_by(|a, b| {
                (
                    &a.broker_name,
                    a.read_queue_nums,
                    a.write_queue_nums,
                    a.perm,
                )
                    .cmp(&(
                        &b.broker_name,
                        b.read_queue_nums,
                        b.write_queue_nums,
                        b.perm,
                    ))
            });
            sorted
        };
        let sort_brokers = |list: &[BrokerData]| {
            let mut sorted: Vec<BrokerData> = list.to_vec();
            sorted.sort_by(|a, b| a.broker_name.cmp(&b.broker_name));
            sorted
        };
        !(sort_queues(&self.queue_datas) == sort_queues(&old.queue_datas)
            && sort_brokers(&self.broker_datas) == sort_brokers(&old.broker_datas))
    }
}

fn encode_string_list_map(table: &[(String, Vec<String>)]) -> Value {
    let mut map = Map::new();
    for (addr, servers) in table {
        map.insert(
            addr.clone(),
            Value::Array(servers.iter().map(|s| Value::String(s.clone())).collect()),
        );
    }
    Value::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::serialize::fastjson;

    // The literals below are compact-JSON (no space separators) ground truths captured from the wire.

    const WIRE_QUEUE_DATA: &str =
        r#"{"brokerName":"broker-a","readQueueNums":8,"writeQueueNums":8,"perm":6,"topicSysFlag":0}"#;

    const WIRE_BROKER_DATA: &str = r#"{"cluster":"DefaultCluster","brokerName":"broker-a","brokerAddrs":{"0":"127.0.0.1:10911","1":"127.0.0.1:10912"},"zoneName":"zone-1","enableActingMaster":false}"#;

    const EMPTY_TOPIC_ROUTE: &str =
        r#"{"orderTopicConf":null,"queueDatas":[],"brokerDatas":[],"filterServerTable":{}}"#;

    const TOPIC_ROUTE: &str = r#"{"orderTopicConf":null,"queueDatas":[{"brokerName":"broker-a","readQueueNums":8,"writeQueueNums":8,"perm":6,"topicSysFlag":0},{"brokerName":"broker-b","readQueueNums":4,"writeQueueNums":4,"perm":6,"topicSysFlag":1}],"brokerDatas":[{"cluster":"DefaultCluster","brokerName":"broker-a","brokerAddrs":{"0":"127.0.0.1:10911","1":"127.0.0.1:10912"},"zoneName":"zone-1","enableActingMaster":false}],"filterServerTable":{"127.0.0.1:10911":["127.0.0.1:9876"]}}"#;

    /// Real nameserver response shape: the keys of `brokerAddrs` are fastjson2 bare numbers.
    const NS_ROUTE: &str = r#"{"brokerDatas":[{"brokerAddrs":{0:"127.0.0.1:10911"},"brokerName":"broker-a","cluster":"DefaultCluster","enableActingMaster":false,"zoneName":""}],"filterServerTable":{},"queueDatas":[{"brokerName":"broker-a","perm":6,"readQueueNums":8,"topicSysFlag":0,"writeQueueNums":8}],"orderTopicConf":null}"#;

    /// Result of decoding `NS_ROUTE` and re-encoding with this client's key order (matches the wire capture).
    const NS_ROUTE_REWRITTEN: &str = r#"{"orderTopicConf":null,"queueDatas":[{"brokerName":"broker-a","readQueueNums":8,"writeQueueNums":8,"perm":6,"topicSysFlag":0}],"brokerDatas":[{"cluster":"DefaultCluster","brokerName":"broker-a","brokerAddrs":{"0":"127.0.0.1:10911"},"zoneName":"","enableActingMaster":false}],"filterServerTable":{}}"#;

    const ROUTE_WITH_MAPPING: &str = r#"{"orderTopicConf":"1 2 3","queueDatas":[],"brokerDatas":[],"filterServerTable":{},"topicQueueMappingByBroker":{"broker-a":{"epoch":3,"queueId":0}}}"#;

    fn value(text: &str) -> Value {
        fastjson::from_str(text).unwrap()
    }

    #[test]
    fn queue_data_golden_json_and_round_trip() {
        let qd = QueueData::new("broker-a", 8, 8, 6, 0);
        assert_eq!(
            RemotingSerializable::to_json_string(&qd.to_json_value()),
            WIRE_QUEUE_DATA
        );
        assert_eq!(QueueData::from_json_value(&value(WIRE_QUEUE_DATA)).unwrap(), qd);
        assert_eq!(qd.clone().compare_to(&qd), Ordering::Equal);
        assert_eq!(
            QueueData::new("broker-b", 0, 0, 0, 0).compare_to(&qd),
            Ordering::Greater
        );
    }

    #[test]
    fn broker_data_golden_json_and_master_selection() {
        let bd = BrokerData::new(
            "DefaultCluster",
            "broker-a",
            vec![
                (0, "127.0.0.1:10911".to_string()),
                (1, "127.0.0.1:10912".to_string()),
            ],
            "zone-1",
        );
        assert_eq!(
            RemotingSerializable::to_json_string(&bd.to_json_value()),
            WIRE_BROKER_DATA
        );
        assert_eq!(
            bd.select_broker_addr().as_deref(),
            Some("127.0.0.1:10911"),
            "when brokerId=0 is in the table, master must be preferred"
        );
        assert_eq!(BrokerData::from_json_value(&value(WIRE_BROKER_DATA)).unwrap(), bd);
        let decoded = BrokerData::decode(WIRE_BROKER_DATA.as_bytes()).unwrap();
        assert_eq!(decoded.zone_name, "zone-1");
        assert_eq!(decoded.broker_addrs.len(), 2);
        assert_eq!(
            RemotingSerializable::to_json_string(&BrokerData::default().to_json_value()),
            r#"{"cluster":"","brokerName":"","brokerAddrs":{},"zoneName":"","enableActingMaster":false}"#
        );
    }

    #[test]
    fn broker_data_without_master_picks_a_slave() {
        let bd = BrokerData::new(
            "c",
            "broker-a",
            vec![
                (1, "slave-1".to_string()),
                (2, "slave-2".to_string()),
                (3, "slave-3".to_string()),
            ],
            "",
        );
        // Randomness only guarantees "within the candidate set", not which one is picked.
        for _ in 0..32 {
            let picked = bd.select_broker_addr().unwrap();
            assert!(picked == "slave-1" || picked == "slave-2" || picked == "slave-3");
        }
        assert_eq!(
            BrokerData::new("c", "b", vec![(0, "m".to_string())], "")
                .select_broker_addr()
                .as_deref(),
            Some("m")
        );
        assert_eq!(BrokerData::default().select_broker_addr(), None);
    }

    #[test]
    fn broker_data_equality_ignores_zone_and_acting_master() {
        let a = BrokerData::new("c", "b", vec![(0, "x".to_string())], "z1");
        let mut b = BrokerData::new("c", "b", vec![(0, "x".to_string())], "z2");
        b.enable_acting_master = true;
        assert_eq!(a, b);
        assert_ne!(
            a,
            BrokerData::new("c", "b", vec![(1, "x".to_string())], "z1")
        );
    }

    #[test]
    fn topic_route_data_matches_golden_json() {
        let trd = TopicRouteData::from_json_value(&value(TOPIC_ROUTE)).unwrap();
        assert_eq!(
            RemotingSerializable::to_json_string(&trd.to_json_value()),
            TOPIC_ROUTE
        );
        assert_eq!(trd, TopicRouteData::decode(TOPIC_ROUTE.as_bytes()).unwrap());
        assert_eq!(
            RemotingSerializable::to_json_string(&TopicRouteData::default().to_json_value()),
            EMPTY_TOPIC_ROUTE
        );
        assert_eq!(trd.filter_server_table[0].0, "127.0.0.1:10911");
        assert_eq!(
            trd.filter_server_table[0].1,
            vec!["127.0.0.1:9876".to_string()]
        );
        assert_eq!(trd.get_broker_datas().len(), 1);
    }

    #[test]
    fn topic_route_mapping_key_is_absent_not_empty() {
        // When None in the payload the whole key is absent; with `{}` it writes `"topicQueueMappingByBroker":{}`.
        let with = TopicRouteData {
            topic_queue_mapping_by_broker: Some(Vec::new()),
            ..Default::default()
        };
        let text = RemotingSerializable::to_json_string(&with.to_json_value());
        assert!(text.contains(r#""topicQueueMappingByBroker":{}"#), "{text}");

        let without = TopicRouteData::decode(EMPTY_TOPIC_ROUTE.as_bytes()).unwrap();
        assert_eq!(without.topic_queue_mapping_by_broker, None);
        assert!(!RemotingSerializable::to_json_string(&without.to_json_value())
            .contains("topicQueueMappingByBroker"));

        let trd = TopicRouteData::decode(ROUTE_WITH_MAPPING.as_bytes()).unwrap();
        assert_eq!(trd.order_topic_conf.as_deref(), Some("1 2 3"));
        let table = trd.topic_queue_mapping_by_broker.clone().unwrap();
        assert_eq!(table[0].0, "broker-a");
        assert_eq!(table[0].1["epoch"], 3);
        assert_eq!(
            RemotingSerializable::to_json_string(&trd.to_json_value()),
            ROUTE_WITH_MAPPING
        );
    }

    #[test]
    fn nameserver_payload_with_bare_numeric_keys_decodes() {
        let trd = TopicRouteData::decode(NS_ROUTE.as_bytes()).unwrap();
        assert_eq!(trd.queue_datas.len(), 1);
        assert_eq!(trd.broker_datas[0].broker_addrs, vec![(0, "127.0.0.1:10911".into())]);
        assert!(trd.filter_server_table.is_empty());
        assert_eq!(
            RemotingSerializable::to_json_string(&trd.to_json_value()),
            NS_ROUTE_REWRITTEN
        );
    }

    #[test]
    fn get_all_message_queue_skips_non_writable_and_unknown_broker() {
        let trd = TopicRouteData::from_json_value(&value(TOPIC_ROUTE)).unwrap();
        // queueDatas: broker-a 8 queues (perm 6) + broker-b 4 queues, but broker-b is not in the route.
        let mqs = trd.get_all_message_queue("MyTopic");
        assert_eq!(mqs.len(), 8);
        assert_eq!(mqs[0], MessageQueueKey::new("MyTopic", "broker-a", 0));
        assert_eq!(mqs[7], MessageQueueKey::new("MyTopic", "broker-a", 7));

        let ro = TopicRouteData {
            queue_datas: vec![
                QueueData::new("broker-b", 2, 2, 4, 0),
                QueueData::new("broker-a", 2, 2, 6, 0),
            ],
            broker_datas: vec![
                BrokerData::new("c", "broker-b", vec![(0, "b".into())], ""),
                BrokerData::new("c", "broker-a", vec![(0, "a".into())], ""),
            ],
            ..Default::default()
        };
        // Expected result of the read-only queue filtering: same topic / brokerName, both brokerId 0 and 1 kept
        assert_eq!(
            ro.get_all_message_queue("T"),
            vec![
                MessageQueueKey::new("T", "broker-a", 0),
                MessageQueueKey::new("T", "broker-a", 1),
            ]
        );
    }

    #[test]
    fn get_all_message_queue_skips_broker_without_master() {
        let ro = TopicRouteData {
            queue_datas: vec![
                QueueData::new("broker-a", 4, 4, 6, 0),
                QueueData::new("broker-b", 4, 4, 6, 0),
            ],
            broker_datas: vec![
                // Shape after the master drops offline: the same brokerName has only brokerId=1 left
                BrokerData::new("c", "broker-a", vec![(1, "slave".into())], ""),
                BrokerData::new(
                    "c",
                    "broker-b",
                    vec![(0, "master".into()), (1, "slave".into())],
                    "",
                ),
            ],
            ..Default::default()
        };
        assert_eq!(
            ro.get_all_message_queue("T"),
            (0..4)
                .map(|i| MessageQueueKey::new("T", "broker-b", i))
                .collect::<Vec<_>>()
        );

        // Negative control: put the master back and broker-a's 4 queues return immediately -- what gets skipped is "no master",
        // not the broker-a name.
        let mut with_master = ro.clone_topic_route_data();
        with_master.broker_datas[0].broker_addrs =
            vec![(0, "master".into()), (1, "slave".into())];
        assert_eq!(with_master.get_all_message_queue("T").len(), 8);
    }

    #[test]
    fn get_all_subscribe_message_queue_keeps_masterless_broker() {
        let ro = TopicRouteData {
            queue_datas: vec![
                QueueData::new("broker-a", 4, 4, 6, 0),
                // Read-only topic: perm=4 (PERM_READ), readQueueNums=3, writeQueueNums=1
                QueueData::new("broker-ro", 3, 1, 4, 0),
            ],
            broker_datas: vec![
                // Shape after the master drops offline: only brokerId=1 left -- subscription info still keeps its queues
                BrokerData::new("c", "broker-a", vec![(1, "slave".into())], ""),
            ],
            ..Default::default()
        };
        // Publish side: perm=6 but only enters with a master; broker-a has no master ⇒ empty
        assert!(ro.get_all_message_queue("T").is_empty());
        // Subscribe side: broker-a's 4 + broker-ro's 3 (even though broker-ro is not in brokerDatas)
        assert_eq!(
            ro.get_all_subscribe_message_queue("T"),
            (0..4)
                .map(|i| MessageQueueKey::new("T", "broker-a", i))
                .chain((0..3).map(|i| MessageQueueKey::new("T", "broker-ro", i)))
                .collect::<Vec<_>>()
        );

        // Negative control: with perm clearing the read bit (2 = write-only), broker-ro disappears and broker-a is unaffected.
        let mut write_only = ro.clone_topic_route_data();
        write_only.queue_datas[1].perm = 2;
        assert_eq!(write_only.get_all_subscribe_message_queue("T").len(), 4);
    }

    #[test]
    fn clone_and_change_detection_match_golden() {
        let trd = TopicRouteData::from_json_value(&value(TOPIC_ROUTE)).unwrap();
        assert!(!trd.topic_route_data_changed(Some(&trd.clone_topic_route_data())));
        assert!(trd.topic_route_data_changed(None));

        // Order shuffled but content identical -> compare after sorting, still treated as unchanged.
        let mut shuffled = trd.clone_topic_route_data();
        shuffled.queue_datas.reverse();
        shuffled.broker_datas.reverse();
        assert!(!shuffled.topic_route_data_changed(Some(&trd)));

        // Only zoneName changes: BrokerData's equality does not cover that field, so it does not count as a change.
        let mut zone_only = trd.clone_topic_route_data();
        zone_only.broker_datas[0].zone_name = "zone-2".to_string();
        assert!(!zone_only.topic_route_data_changed(Some(&trd)));

        // A write-queue-count change must count as a change (writeQueueNums is in the sort key).
        let mut write_only = trd.clone_topic_route_data();
        write_only.queue_datas[0].write_queue_nums = 4;
        assert!(write_only.topic_route_data_changed(Some(&trd)));

        let cloned = trd.clone_topic_route_data();
        assert_eq!(cloned, trd);
    }

    #[test]
    fn malformed_route_bodies_return_decode_errors() {
        assert!(matches!(TopicRouteData::decode(b""), Err(Error::Decode(_))));
        assert!(matches!(
            TopicRouteData::decode(b"[1,2]"),
            Err(Error::Decode(_))
        ));
        assert!(matches!(
            TopicRouteData::from_json_value(&value(r#"{"queueDatas":{}}"#)),
            Err(Error::Decode(_))
        ));
        assert!(matches!(
            TopicRouteData::from_json_value(&value(r#"{"filterServerTable":{"a":1}}"#)),
            Err(Error::Decode(_))
        ));
        assert!(matches!(
            TopicRouteData::from_json_value(&value(r#"{"topicQueueMappingByBroker":[]}"#)),
            Err(Error::Decode(_))
        ));
        assert!(matches!(
            QueueData::from_json_value(&value(r#"[1]"#)),
            Err(Error::Decode(_))
        ));
    }
}
