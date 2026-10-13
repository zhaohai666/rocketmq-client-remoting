
use std::collections::{BTreeMap, HashMap};

use serde_json::Value;

use crate::common::message::MessageQueue;
use crate::remoting::protocol::route::TopicRouteData;

/// `MixAll.METADATA_SCOPE_GLOBAL`。
pub const METADATA_SCOPE_GLOBAL: &str = "__global__";
/// `MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX`。
pub const LOGICAL_QUEUE_MOCK_BROKER_PREFIX: &str = "__syslo__";
/// `MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST`。
pub const LOGICAL_QUEUE_MOCK_NOT_EXIST: &str = "__syslo__none__";

pub(crate) fn is_logical_queue_mock_broker_name(broker_name: &str) -> bool {
    broker_name.starts_with(LOGICAL_QUEUE_MOCK_BROKER_PREFIX)
}

/// `getMockBrokerName`：`__global__`（scope 本身）剥掉头两个字符，其余 scope 原样加前缀。
pub fn get_mock_broker_name(scope: &str) -> String {
    if scope == METADATA_SCOPE_GLOBAL {
        format!("{}{}", LOGICAL_QUEUE_MOCK_BROKER_PREFIX, &scope[2..])
    } else {
        format!("{}{}", LOGICAL_QUEUE_MOCK_BROKER_PREFIX, scope)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct TopicQueueMappingInfo {
    pub scope: String,
    pub total_queues: i32,
    pub bname: String,
    pub epoch: i64,
    /// globalId -> 物理 queueId。物理 id 不参与端点推导，只保留给排查用。
    pub curr_id_map: BTreeMap<i32, i32>,
}

pub(crate) fn parse_topic_queue_mapping_info(raw: &Value) -> Option<TopicQueueMappingInfo> {
    let obj = raw.as_object()?;
    let mut info = TopicQueueMappingInfo {
        scope: METADATA_SCOPE_GLOBAL.to_string(),
        ..Default::default()
    };
    if let Some(Value::String(s)) = obj.get("scope") {
        info.scope = s.clone();
    }
    if let Some(v) = obj.get("totalQueues").and_then(Value::as_i64) {
        info.total_queues = v as i32;
    }
    if let Some(Value::String(s)) = obj.get("bname") {
        info.bname = s.clone();
    }
    if let Some(v) = obj.get("epoch").and_then(Value::as_i64) {
        info.epoch = v;
    }
    if let Some(Value::Object(m)) = obj.get("currIdMap") {
        for (k, v) in m {
            let Ok(gid) = k.parse::<i32>() else {
                continue;
            };
            let Some(phys) = v.as_i64() else {
                continue;
            };
            info.curr_id_map.insert(gid, phys as i32);
        }
    }
    Some(info)
}

/// `ClientMetadata.topicRouteData2EndpointsForStaticTopic`：构建逻辑队列集
/// `MessageQueue(topic, mockBrokerName(scope), globalId) -> 承载它的真实 broker 名`
/// （total 范围内没有承载者的逻辑 id 指向 `__syslo__none__`）。
/// 空映射（非静态主题）返回 `None`。
pub fn topic_route_data_2_endpoints_for_static_topic(
    topic: &str,
    route: &TopicRouteData,
) -> Option<HashMap<MessageQueue, String>> {
    let mapping = route.topic_queue_mapping_by_broker.as_ref()?;
    if mapping.is_empty() {
        return None;
    }

    let mut infos_by_scope: HashMap<String, Vec<(String, TopicQueueMappingInfo)>> = HashMap::new();
    for (broker, raw) in mapping {
        let Some(info) = parse_topic_queue_mapping_info(raw) else {
            continue;
        };
        infos_by_scope
            .entry(info.scope.clone())
            .or_default()
            .push((broker.clone(), info));
    }

    let mut endpoints: HashMap<MessageQueue, String> = HashMap::new();
    for (scope, mut infos) in infos_by_scope {
        infos.sort_by(|a, b| a.0.cmp(&b.0));
        infos.sort_by_key(|a| std::cmp::Reverse(a.1.epoch));

        let mut max_total_nums = 0i32;
        let max_total_num_of_epoch: i64 = -1;
        let mock_name = get_mock_broker_name(&scope);
        // MessageQueue -> (epoch, bname)。
        let mut mq_end_points: HashMap<MessageQueue, (i64, String)> = HashMap::new();
        for (_, info) in &infos {
            if info.epoch >= max_total_num_of_epoch && info.total_queues > max_total_nums {
                max_total_nums = info.total_queues;
            }
            // 只有 global id（键）形状决定端点集；物理 id 是 broker 内部的。
            for global_id in info.curr_id_map.keys() {
                let mq = MessageQueue::new(topic, &mock_name, *global_id);
                match mq_end_points.get(&mq) {
                    Some(&(old_epoch, _)) if old_epoch > info.epoch => {}
                    _ => {
                        mq_end_points.insert(mq, (info.epoch, info.bname.clone()));
                    }
                }
            }
        }

        // 补齐静态逻辑队列：total 之下的每个 id 都存在；没有承载者的指向
        // NOT_EXIST 哨兵。
        for i in 0..max_total_nums {
            let mq = MessageQueue::new(topic, &mock_name, i);
            let real = match mq_end_points.get(&mq) {
                Some((_, bname)) => bname.clone(),
                None => LOGICAL_QUEUE_MOCK_NOT_EXIST.to_string(),
            };
            endpoints.insert(mq, real);
        }
    }
    if endpoints.is_empty() {
        return None;
    }
    Some(endpoints)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn raw_mapping(
        scope: &str,
        total_queues: i32,
        bname: &str,
        epoch: i64,
        curr_id_map: Value,
    ) -> Value {
        json!({
            "scope": scope,
            "totalQueues": total_queues,
            "bname": bname,
            "epoch": epoch,
            "dirty": false,
            "currIdMap": curr_id_map,
        })
    }

    fn mapping_route(entries: Vec<(&str, Value)>) -> TopicRouteData {
        TopicRouteData {
            topic_queue_mapping_by_broker: Some(
                entries
                    .into_iter()
                    .map(|(b, v)| (b.to_string(), v))
                    .collect(),
            ),
            ..Default::default()
        }
    }

    #[test]
    fn mock_broker_name_strips_global_underscores() {
        assert_eq!(get_mock_broker_name("__global__"), "__syslo__global__");
        assert_eq!(get_mock_broker_name("zone-a"), "__syslo__zone-a");
        assert!(is_logical_queue_mock_broker_name("__syslo__global__"));
        assert!(!is_logical_queue_mock_broker_name("broker-a"));
    }

    #[test]
    fn parse_defaults_scope_to_global() {
        let info = parse_topic_queue_mapping_info(&json!({
            "totalQueues": 2,
            "bname": "b1",
            "epoch": 3,
        }))
        .unwrap();
        assert_eq!(info.scope, "__global__");
        assert_eq!(info.total_queues, 2);
        assert_eq!(info.bname, "b1");
        assert_eq!(info.epoch, 3);
        assert!(parse_topic_queue_mapping_info(&json!("not-an-object")).is_none());
    }

    #[test]
    fn endpoints_single_scope_with_unhosted_queue() {
        let route = mapping_route(vec![(
            "broker-a",
            raw_mapping(
                "__global__",
                4,
                "broker-a",
                0,
                json!({"0": 0, "1": 1, "2": 2}),
            ),
        )]);
        let endpoints = topic_route_data_2_endpoints_for_static_topic("StaticTopic", &route)
            .expect("static route must yield endpoints");
        assert_eq!(endpoints.len(), 4, "hosted + unhosted");
        for id in 0..3 {
            let mq = MessageQueue::new("StaticTopic", "__syslo__global__", id);
            assert_eq!(endpoints[&mq], "broker-a", "global {id}");
        }
        let unhosted = MessageQueue::new("StaticTopic", "__syslo__global__", 3);
        assert_eq!(endpoints[&unhosted], "__syslo__none__");

        let empty = TopicRouteData::default();
        assert!(topic_route_data_2_endpoints_for_static_topic("T", &empty).is_none());
    }

    #[test]
    fn scopes_stay_apart_and_epoch_fences() {
        let route = mapping_route(vec![
            (
                "broker-old",
                raw_mapping("__global__", 1, "broker-old", 3, json!({"0": 0})),
            ),
            (
                "broker-new",
                raw_mapping("__global__", 1, "broker-new", 7, json!({"0": 0})),
            ),
            (
                "broker-b",
                raw_mapping("zone-x", 1, "broker-b", 9, json!({"0": 0})),
            ),
        ]);
        let endpoints = topic_route_data_2_endpoints_for_static_topic("T", &route).unwrap();
        let global0 = MessageQueue::new("T", "__syslo__global__", 0);
        assert_eq!(endpoints[&global0], "broker-new", "higher epoch fences");
        let zx0 = MessageQueue::new("T", "__syslo__zone-x", 0);
        assert_eq!(endpoints[&zx0], "broker-b");
        assert_eq!(endpoints.len(), 2, "scopes do not merge");
    }

    #[test]
    fn instance_rewrites_mock_broker_name_via_end_points_table() {
        let inst = crate::client::mq_client::MQClientInstance::new(
            "static-test@DEFAULT",
            vec!["127.0.0.1:9876".to_string()],
        );
        let endpoints = HashMap::from([
            (
                MessageQueue::new("StaticTopic", "__syslo__global__", 0),
                "broker-a".to_string(),
            ),
            (
                MessageQueue::new("StaticTopic", "__syslo__global__", 1),
                "broker-b".to_string(),
            ),
        ]);
        inst.seed_end_points_for_test("StaticTopic", endpoints);

        let mq0 = MessageQueue::new("StaticTopic", "__syslo__global__", 0);
        assert_eq!(inst.get_broker_name_from_message_queue(&mq0), "broker-a");
        let mq1 = MessageQueue::new("StaticTopic", "__syslo__global__", 1);
        assert_eq!(inst.get_broker_name_from_message_queue(&mq1), "broker-b");
        // 表里没有的队列 / 非静态主题原样透传自己的 broker 名。
        let mq99 = MessageQueue::new("StaticTopic", "__syslo__global__", 99);
        assert_eq!(
            inst.get_broker_name_from_message_queue(&mq99),
            "__syslo__global__"
        );
        let plain = MessageQueue::new("Plain", "broker-a", 0);
        assert_eq!(inst.get_broker_name_from_message_queue(&plain), "broker-a");
    }
}
