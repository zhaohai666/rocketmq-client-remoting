//! Topic 路由数据（对应 `org.apache.rocketmq.remoting.protocol.route.*`）。
//!
//! 移植 `python/rocketmq/remoting/protocol/route.py`：`QueueData` / `BrokerData` /
//! `TopicRouteData`。这三个类是 nameserver `GET_ROUTE_INFO_BY_TOPIC(105)` 应答的 body，
//! 也是客户端路由表的全部数据结构。
//!
//! ## 三处必须照抄的线上行为
//!
//! 1. `brokerAddrs` 的键是 Java `Long`。fastjson2 写成**不带引号**的数字键
//!    （`{0:"127.0.0.1:10911"}`），`serialize::fastjson` 读回时已规约成字符串键；
//!    写出一侧 serde_json 造不出裸数字键，故与 Python 一致写 `"0"`（只影响请求体，
//!    路由表在客户端是只读的，broker 两种写法都收）。
//! 2. `orderTopicConf` 为 null 时 Python **恒定写出** `"orderTopicConf":null`
//!    （`route.py:to_dict` 直接放键），而 `topicQueueMappingByBroker` 为 null 时整键
//!    不出现 —— 两种 null 语义不同，不能统一处理。
//! 3. `MessageQueue` 在本层复用 [`MessageQueueKey`]：协议层不依赖 common 层，
//!    见 `admin_body.rs` 的模块说明。
//!
//! ## `topic_route_data_changed` 与 Java 的口径差异
//!
//! Java 排序后比较**整个** `TopicRouteData`（含 orderTopicConf / filterServerTable /
//! topicQueueMappingByBroker）；Python 只比较排过序的 queueDatas 与 brokerDatas，
//! 这里沿用 Python 的窄口径（否则 broker 只改可用区就会多触发一次 rebalance）。

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

/// 对应 `org.apache.rocketmq.remoting.protocol.route.QueueData`。
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

    /// 对应 `QueueData.compareTo`（Python 只比 brokerName）。
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

/// 对应 `org.apache.rocketmq.remoting.protocol.route.BrokerData`。
///
/// `broker_addrs` 用 `Vec<(i64, String)>` 而不是 `HashMap`：要保住报文里的 brokerId
/// 顺序，否则「随机挑一个从节点」的候选次序和编码字节都会漂。
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

    /// Java `BrokerData.selectBrokerAddr`：`MixAll.MASTER_ID(=0)` 在表里就用 master，
    /// 否则随机取一个（值本身不校验空串，与 Java/Python 一致）。
    pub fn select_broker_addr(&self) -> Option<String> {
        let master_id = MixAll::MASTER_ID as i64;
        if let Some((_, addr)) = self.broker_addrs.iter().find(|(id, _)| *id == master_id) {
            return Some(addr.clone());
        }
        self.broker_addrs
            .get(pseudo_random_index(self.broker_addrs.len()))
            .map(|(_, addr)| addr.clone())
    }

    /// 对应 `BrokerData.compareTo`（Python 只比 brokerName）。
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
            // 报文里是 `"0"`（或 fastjson2 的裸 `0`，解析后同样是字符串）。
            // Python 用 `int(k)`，脏键会抛；这里跳过，不让一份可用路由整个作废。
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
            // Python: `d.get("enableActingMaster", False) or False`
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

/// Java `BrokerData.equals` / Python `__eq__` 的口径：只看
/// cluster / brokerName / brokerAddrs，**不含** zoneName 与 enableActingMaster。
/// 路由变更判定依赖它，加进去会让 broker 换可用区时误判成路由变化。
impl PartialEq for BrokerData {
    fn eq(&self, other: &BrokerData) -> bool {
        self.cluster == other.cluster
            && self.broker_name == other.broker_name
            && self.broker_addrs == other.broker_addrs
    }
}

impl Eq for BrokerData {}

/// 无 master 时随机挑从节点（对应 Java `random.nextInt(size)`）。
///
/// 不引入 `rand` 依赖：纳秒时钟 + 进程级计数器做 xorshift64，只需保证「不是每次取同一
/// 个」，负载均衡不要求密码学质量。
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

/// 对应 `org.apache.rocketmq.remoting.protocol.route.TopicRouteData`。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TopicRouteData {
    pub order_topic_conf: Option<String>,
    pub queue_datas: Vec<QueueData>,
    pub broker_datas: Vec<BrokerData>,
    /// brokerAddr -> filterServer 列表，保持报文顺序。
    pub filter_server_table: Vec<(String, Vec<String>)>,
    /// brokerName -> `TopicQueueMappingInfo`；协议层不建模该结构，原样透传 JSON。
    /// `None`（键不出现）与 `Some(空表)`（`{}`）在 wire 上不同，不能合并。
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
        // Python: `d.get("topicQueueMappingByBroker")` —— 缺键 / null 都是 None，
        // 显式 `{}` 是 Some(空表)，重新编码时要把键写回去。
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

    /// 对应 Java `topicRouteData.getBrokerDatas()`。
    pub fn get_broker_datas(&self) -> &[BrokerData] {
        &self.broker_datas
    }

    /// 按 queueDatas + brokerDatas 组装全部可写队列（对应 Java
    /// `topicRouteData2TopicPublishInfo` 的组装循环）。
    ///
    /// `topic` 必须传真实 topic：send/pull 之后拿 `mq.topic` 回查路由表，
    /// 回填错了会出现「有路由却查不到」。
    pub fn get_all_message_queue(&self, topic: &str) -> Vec<MessageQueueKey> {
        let mut mqs = Vec::new();
        for qd in &self.queue_datas {
            if !PermName::check_perm(qd.perm, PermName::PERM_WRITE) {
                continue;
            }
            if !self
                .broker_datas
                .iter()
                .any(|bd| bd.broker_name == qd.broker_name)
            {
                continue;
            }
            for i in 0..qd.write_queue_nums {
                mqs.push(MessageQueueKey::new(topic, &qd.broker_name, i));
            }
        }
        mqs
    }

    /// 对应 Java `cloneTopicRouteData()`：队列/broker 元素浅拷贝，两张表深拷贝。
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

    /// 路由是否变化（`updateTopicRouteInfoFromNameServer` 的短路条件）。
    pub fn topic_route_data_changed(&self, old: Option<&TopicRouteData>) -> bool {
        let Some(old) = old else {
            return true;
        };
        let sort_queues = |list: &[QueueData]| {
            let mut sorted: Vec<QueueData> = list.to_vec();
            // 排序键逐项对应 Python 的 `(broker_name, read, write, perm)`；
            // topicSysFlag 不参与，避免 broker 只改 sysFlag 就误判成路由变化。
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

    // 以下字面量由 Python 参考实现（route.py 的 to_dict + json.dumps 紧凑分隔符）打印得到。

    const JAVA_QUEUE_DATA: &str =
        r#"{"brokerName":"broker-a","readQueueNums":8,"writeQueueNums":8,"perm":6,"topicSysFlag":0}"#;

    const JAVA_BROKER_DATA: &str = r#"{"cluster":"DefaultCluster","brokerName":"broker-a","brokerAddrs":{"0":"127.0.0.1:10911","1":"127.0.0.1:10912"},"zoneName":"zone-1","enableActingMaster":false}"#;

    const EMPTY_TOPIC_ROUTE: &str =
        r#"{"orderTopicConf":null,"queueDatas":[],"brokerDatas":[],"filterServerTable":{}}"#;

    const TOPIC_ROUTE: &str = r#"{"orderTopicConf":null,"queueDatas":[{"brokerName":"broker-a","readQueueNums":8,"writeQueueNums":8,"perm":6,"topicSysFlag":0},{"brokerName":"broker-b","readQueueNums":4,"writeQueueNums":4,"perm":6,"topicSysFlag":1}],"brokerDatas":[{"cluster":"DefaultCluster","brokerName":"broker-a","brokerAddrs":{"0":"127.0.0.1:10911","1":"127.0.0.1:10912"},"zoneName":"zone-1","enableActingMaster":false}],"filterServerTable":{"127.0.0.1:10911":["127.0.0.1:9876"]}}"#;

    /// nameserver 真实回包形状：`brokerAddrs` 的键是 fastjson2 裸数字。
    const NS_ROUTE: &str = r#"{"brokerDatas":[{"brokerAddrs":{0:"127.0.0.1:10911"},"brokerName":"broker-a","cluster":"DefaultCluster","enableActingMaster":false,"zoneName":""}],"filterServerTable":{},"queueDatas":[{"brokerName":"broker-a","perm":6,"readQueueNums":8,"topicSysFlag":0,"writeQueueNums":8}],"orderTopicConf":null}"#;

    /// `NS_ROUTE` 解码后按 Python 的键序重新编码的结果（Python 实测）。
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
            JAVA_QUEUE_DATA
        );
        assert_eq!(QueueData::from_json_value(&value(JAVA_QUEUE_DATA)).unwrap(), qd);
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
            JAVA_BROKER_DATA
        );
        assert_eq!(
            bd.select_broker_addr().as_deref(),
            Some("127.0.0.1:10911"),
            "brokerId=0 在表里时必须优先 master"
        );
        assert_eq!(BrokerData::from_json_value(&value(JAVA_BROKER_DATA)).unwrap(), bd);
        let decoded = BrokerData::decode(JAVA_BROKER_DATA.as_bytes()).unwrap();
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
        // 随机只能保证「落在候选集内」，不保证具体挑中哪个。
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
    fn topic_route_data_golden_json_matches_python() {
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
        // Python: None 时整个键不出现；`{}` 时写出 `"topicQueueMappingByBroker":{}`。
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
        // queueDatas: broker-a 8 队列(perm 6) + broker-b 4 队列，但路由里没有 broker-b。
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
        // Python ALL_MQ_READONLY_FILTER == [("T","broker-a",0),("T","broker-a",1)]
        assert_eq!(
            ro.get_all_message_queue("T"),
            vec![
                MessageQueueKey::new("T", "broker-a", 0),
                MessageQueueKey::new("T", "broker-a", 1),
            ]
        );
    }

    #[test]
    fn clone_and_change_detection_match_python() {
        let trd = TopicRouteData::from_json_value(&value(TOPIC_ROUTE)).unwrap();
        assert!(!trd.topic_route_data_changed(Some(&trd.clone_topic_route_data())));
        assert!(trd.topic_route_data_changed(None));

        // 顺序打乱但内容相同 -> 排序后比较，仍视为未变化。
        let mut shuffled = trd.clone_topic_route_data();
        shuffled.queue_datas.reverse();
        shuffled.broker_datas.reverse();
        assert!(!shuffled.topic_route_data_changed(Some(&trd)));

        // 只动 zoneName：BrokerData 相等口径不含该字段，故不算变化。
        let mut zone_only = trd.clone_topic_route_data();
        zone_only.broker_datas[0].zone_name = "zone-2".to_string();
        assert!(!zone_only.topic_route_data_changed(Some(&trd)));

        // 写队列数变化必须算变化（Python 排序键里有 writeQueueNums）。
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
