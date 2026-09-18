//! 队列分配策略（rebalance 时把某个 topic 的队列分给同一消费组里的各个消费者）。
//!
//! 对应 Java `org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy` 与实现类
//! `org.apache.rocketmq.client.consumer.rebalance.{AbstractAllocateMessageQueueStrategy,
//! AllocateMessageQueueAveragely, AllocateMessageQueueAveragelyByCircle,
//! AllocateMessageQueueByConfig}`（`#allocate` / `#getName` / `#check`）；
//! 逐条对齐参考实现 `python/rocketmq/client/consumer.py:152-210`。
//!
//! 注：本仓库的 Java 快照把这些实现类放在 `client.consumer.rebalance` 包下（部分上游版本
//! 叫 `client.consumer.allocation`），类名与方法名完全一致，下文引用一律用快照里的真实路径。
//!
//! ## 移植范围
//!
//! Python 参考实现只有 4 个（接口 + 3 个实现），这里也就只移植这 4 个。Java 另有
//! `AllocateMessageQueueByMachineRoom`（`getName()` = `MACHINE_ROOM`）、
//! `AllocateMessageQueueConsistentHash`（`CONSISTENT_HASH`）、`AllocateMachineRoomNearby`
//! （`MACHINE_ROOM_NEARBY-<delegate>`）三个策略，Python 侧没有对应实现，本文件**不臆造**移植。
//!
//! ## 与 Java 的有意差异（一律跟随 Python）
//!
//! 1. **守卫不抛异常**：Java `AbstractAllocateMessageQueueStrategy#check` 在 `currentCID`
//!    为空串、`mqAll` 为空、`cidAll` 为空这三种情况下抛
//!    `IllegalArgumentException("currentCID is empty" / "mqAll is null or mqAll empty" /
//!    "cidAll is null or cidAll empty")`；Python（`consumer.py:165-168`、`191-194`）
//!    一律 `return []`。这里跟随 Python：非法入参 → 空结果，**不**返回 `Err`。
//!    理由是 rebalance 是后台周期任务，一条脏入参不该把消费者打挂。
//! 2. **`AllocateMessageQueueByConfig` 的未配置态**：Java 直接 `return this.messageQueueList`，
//!    没配过就是 `null`；Python 的 `__init__` 把它规整成 `[]` 且 `allocate` 返回
//!    `list(...)` 副本（`consumer.py:206/210`）。这里取 Python 口径：空列表 + 返回副本。
//! 3. **`allocate` 的 `Result`**：因为守卫不再报错，本模块三个内置策略恒返回 `Ok`；
//!    `Result` 是给自定义策略（业务方自己实现 [`AllocateMessageQueueStrategy`]）留的出错口子，
//!    与 Java「策略里抛异常 → rebalance 记 error」的扩展点位置一致。
//! 4. **`check_config` 的判定顺序**：Java 先看 `currentCID`、再看 `mqAll`、最后看 `cidAll`；
//!    Python 把 `mqAll` 放到了最前。结果完全相同（三种情况都是空结果），这里保留 Java 顺序，
//!    并把 Java `check` 里那条 `[BUG] ... not in cidAll` 的 info 日志一并移植
//!    （Python 只静默返回 `[]`；日志不改变行为）。

use std::sync::RwLock;

use crate::common::message::MessageQueue;
use crate::error::Result;
use crate::rmq_info;

/// 队列分配策略接口，对应 Java `AllocateMessageQueueStrategy#allocate` +
/// `#getName`（Python `consumer.py:152` 的 `AllocateMessageQueueStrategy`）。
///
/// 刻意保持 **object-safe**（两个方法都只吃 `&self`，没有泛型参数、没有 `Self` 返回），
/// 这样调用方可以像 Java `RebalanceImpl.allocateMessageQueueStrategy` 字段那样
/// 用 `Arc<dyn AllocateMessageQueueStrategy>` 在 rebalance 线程上共享它。
pub trait AllocateMessageQueueStrategy: Send + Sync {
    /// 对应 Java `AllocateMessageQueueStrategy#allocate(consumerGroup, currentCID, mqAll, cidAll)`。
    ///
    /// - `consumer_group`：当前消费组，只在「不在 `cid_all` 里」那条日志里出现（Java 同）；
    /// - `current_cid`：本客户端 id，Java `cidAll.indexOf(currentCID)` 的下标决定分哪一段；
    /// - `mq_all`：该 topic 的全部队列。**调用方负责排序**：Java
    ///   `RebalanceImpl#rebalanceByTopic` 对 `cidAll`/`mqAll` 都排过序，Python 亦然
    ///   （`consumer.py:1052` `sorted(..., key=_mq_sort_key)`）；
    /// - `cid_all`：该消费组的全部客户端 id，同样要求已排序（决定每个客户端拿到哪一段）。
    ///
    /// 策略本身不排序、不去重，只保证「输出是输入的一个有序子序列」，因此结果确定可复现。
    fn allocate(
        &self,
        consumer_group: &str,
        current_cid: &str,
        mq_all: &[MessageQueue],
        cid_all: &[String],
    ) -> Result<Vec<MessageQueue>>;

    /// 对应 Java `AllocateMessageQueueStrategy#getName`：算法名（`AVG` / `AVG_BY_CIRCLE` /
    /// `CONFIG`），Java 侧用于日志与策略识别。
    fn get_name(&self) -> &str;
}

/// 对应 Java `AbstractAllocateMessageQueueStrategy#check(consumerGroup, currentCID, mqAll, cidAll)`
/// （Python 把同样的守卫内联在每个 `allocate` 开头，见模块头差异 1）。
///
/// 守卫通过时返回 `current_cid` 在 `cid_all` 中的下标（Java 的 `cidAll.indexOf(currentCID)`），
/// 不通过返回 `None`，由调用方给出空结果。
///
/// Python 的 `not current_cid` 只把空串当假值，Java 这里是 `StringUtils.isEmpty`，
/// 两者对 `""` 一致（都不接受纯空白串以外的差异：空白串在两边都算合法入参）。
fn check_config(
    consumer_group: &str,
    current_cid: &str,
    mq_all: &[MessageQueue],
    cid_all: &[String],
) -> Option<usize> {
    if current_cid.is_empty() {
        return None;
    }
    if mq_all.is_empty() {
        return None;
    }
    if cid_all.is_empty() {
        return None;
    }
    if let Some(index) = cid_all.iter().position(|cid| cid == current_cid) {
        return Some(index);
    }
    // Java `AbstractAllocateMessageQueueStrategy#check` 的同名日志（Python 静默返回 []）。
    rmq_info!(
        "[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {:?}",
        consumer_group,
        current_cid,
        cid_all
    );
    None
}

// --------------------------------------------------------- Averagely（AVG）

/// 平均分配：把 `mq_all` 切成**连续区间**，余数 `mod` 依次分给前 `mod` 个消费者，
/// 即前 `mod` 个各拿 `average_size + 1` 条，其余拿 `average_size` 条。
///
/// 对应 Java `org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely`
/// `#allocate`（`getName()` = `"AVG"`）、Python `AllocateMessageQueueAveragely.allocate`
/// （`consumer.py:160-183`）。
///
/// 两边的算式**逐值等价**（`AllocateMessageQueueAveragelyTest#
/// testAllocateMessageQueueAveragely` 的 10 队列 / 4 消费者 → `{3,3,2,2}`）：
/// - Java：`averageSize = mq<=cid ? 1 : (mod>0 && index<mod ? mq/cid+1 : mq/cid)`，
///   `startIndex = (mod>0 && index<mod) ? index*averageSize : index*averageSize+mod`，
///   `range = Math.min(averageSize, mq - startIndex)`；
/// - Python（本实现照抄）：`start/end` 双分支 + `mq_all[start:end]` 切片。
///   `index*avg + mod` 与 `mod*(avg+1) + (index-mod)*avg` 展开后同为 `index*avg + mod`；
///   Java 的 `Math.min` 削零则等价于 Python 的 `average_size == 0` 早返回分支
///   （队列数 ≤ 消费者数时只有 `index < mod` 的客户端拿到 1 条，其余为空）。
#[derive(Debug, Clone, Copy, Default)]
pub struct AllocateMessageQueueAveragely;

impl AllocateMessageQueueStrategy for AllocateMessageQueueAveragely {
    fn allocate(
        &self,
        consumer_group: &str,
        current_cid: &str,
        mq_all: &[MessageQueue],
        cid_all: &[String],
    ) -> Result<Vec<MessageQueue>> {
        let Some(index) = check_config(consumer_group, current_cid, mq_all, cid_all) else {
            return Ok(Vec::new());
        };
        let mq_size = mq_all.len();
        let cid_size = cid_all.len();
        let modulo = mq_size % cid_size; // Python `mod` / Java `mod`
        let average_size = mq_size / cid_size; // Python `average_size`

        // consumer.py:172-173：队列数少于消费者数时，只有下标 < modulo 的客户端拿到 1 条。
        // Java 靠 `averageSize = 1` + `Math.min(averageSize, mq - startIndex)` 得到同样结果。
        if average_size == 0 {
            return Ok(if index < mq_size {
                vec![mq_all[index].clone()]
            } else {
                Vec::new()
            });
        }

        // consumer.py:174-183
        let (start_index, end_index) = if modulo > 0 && index < modulo {
            let start = index * (average_size + 1);
            (start, start + average_size + 1)
        } else {
            // 这里 index >= modulo（或 modulo == 0），减法不会下溢。
            let start = modulo * (average_size + 1) + (index - modulo) * average_size;
            (start, start + average_size)
        };
        // Java `Math.min(averageSize, mqAll.size() - startIndex)`：越界削成空/截断。
        let end_index = end_index.min(mq_size);
        if start_index >= end_index {
            return Ok(Vec::new());
        }
        Ok(mq_all[start_index..end_index].to_vec())
    }

    fn get_name(&self) -> &str {
        "AVG"
    }
}

// ------------------------------------------------- AveragelyByCircle（AVG_BY_CIRCLE）

/// 环形平均分配：第 `index` 个消费者取走下标 `≡ index (mod cid_size)` 的队列，
/// 即 `mq_all[index], mq_all[index + cid_size], ...`。
///
/// 对应 Java `AllocateMessageQueueAveragelyByCircle#allocate`（`getName()` =
/// `"AVG_BY_CIRCLE"`）、Python `AllocateMessageQueueAveragelyByCircle.allocate`
/// （`consumer.py:186-199`）。
#[derive(Debug, Clone, Copy, Default)]
pub struct AllocateMessageQueueAveragelyByCircle;

impl AllocateMessageQueueStrategy for AllocateMessageQueueAveragelyByCircle {
    fn allocate(
        &self,
        consumer_group: &str,
        current_cid: &str,
        mq_all: &[MessageQueue],
        cid_all: &[String],
    ) -> Result<Vec<MessageQueue>> {
        let Some(index) = check_config(consumer_group, current_cid, mq_all, cid_all) else {
            return Ok(Vec::new());
        };
        // Python: `for i in range(index, len(mq_all), len(cid_all))`
        // Java:   `for (i = index; i < mqAll.size(); i++) if (i % cidAll.size() == index) add`
        // 两者等价，都是「从自己的下标出发、步长 = 消费者数」的等差取数。
        // `check_config` 已保证 `cid_all` 非空，步长恒 >= 1，`step_by(0)` 的 panic 不可能发生。
        Ok(mq_all
            .iter()
            .skip(index)
            .step_by(cid_all.len())
            .cloned()
            .collect())
    }

    fn get_name(&self) -> &str {
        "AVG_BY_CIRCLE"
    }
}

// ------------------------------------------------------------- ByConfig（CONFIG）

/// 按显式配置分配：完全无视 `mq_all` / `cid_all`，把配置进去的队列列表原样发给**每一个**
/// 消费者（通常用于广播式手工绑队列）。
///
/// 对应 Java `AllocateMessageQueueByConfig`（`#allocate` / `#getMessageQueueList` /
/// `#setMessageQueueList`，`getName()` = `"CONFIG"`）、Python
/// `AllocateMessageQueueByConfig`（`consumer.py:202-210`）。
///
/// Java 与 Python 的 `allocate` 都**不调 `check`**，所以空 group / 空 `cid_all` 也照样返回配置值。
#[derive(Debug, Default)]
pub struct AllocateMessageQueueByConfig {
    message_queue_list: RwLock<Vec<MessageQueue>>,
}

impl AllocateMessageQueueByConfig {
    /// 对应 Python `AllocateMessageQueueByConfig(message_queue_list=[...])`
    /// （`consumer.py:205`）；Java 只有隐式无参构造 + `setMessageQueueList`。
    pub fn new(message_queue_list: Vec<MessageQueue>) -> AllocateMessageQueueByConfig {
        AllocateMessageQueueByConfig {
            message_queue_list: RwLock::new(message_queue_list),
        }
    }

    /// 对应 Java `AllocateMessageQueueByConfig#setMessageQueueList(List)`。
    ///
    /// 取 `&self`（而不是 `&mut self`）：Java 里策略对象是引用，注册到
    /// `RebalanceImpl` 之后再改列表依然对所有 rebalance 生效；Python 同样能随时改属性。
    /// 用 `&mut self` 就要求调用方独占这个对象，与两边语义都不符
    /// （口径同 [`crate::client::latency::MQFaultStrategy::set_send_latency_fault_enable`]）。
    pub fn set_message_queue_list(&self, message_queue_list: Vec<MessageQueue>) {
        *self
            .message_queue_list
            .write()
            .unwrap_or_else(|e| e.into_inner()) = message_queue_list;
    }

    /// 对应 Java `AllocateMessageQueueByConfig#getMessageQueueList()`。
    /// 返回副本（与 Python `list(self.message_queue_list)` 一致，不交出共享可变引用）。
    pub fn get_message_queue_list(&self) -> Vec<MessageQueue> {
        self.message_queue_list
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

impl AllocateMessageQueueStrategy for AllocateMessageQueueByConfig {
    fn allocate(
        &self,
        _consumer_group: &str,
        _current_cid: &str,
        _mq_all: &[MessageQueue],
        _cid_all: &[String],
    ) -> Result<Vec<MessageQueue>> {
        // Java: `return this.messageQueueList;`（未配置 = null）
        // Python: `return list(self.message_queue_list)`（未配置 = []）→ 这里跟 Python。
        Ok(self.get_message_queue_list())
    }

    fn get_name(&self) -> &str {
        "CONFIG"
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    /// Java 测试里的 `createMessageQueueList(size)`：`new MessageQueue("topic", "brokerName", i)`。
    fn create_message_queue_list(size: i32) -> Vec<MessageQueue> {
        (0..size)
            .map(|i| MessageQueue::new("topic", "brokerName", i))
            .collect()
    }

    /// Java 测试里的 `createConsumerIdList(size)`：`"CID_PREFIX" + i`。
    fn create_consumer_id_list(size: usize) -> Vec<String> {
        (0..size).map(|i| format!("CID_PREFIX{}", i)).collect()
    }

    fn queue_ids(mqs: &[MessageQueue]) -> Vec<i32> {
        mqs.iter().map(|mq| mq.get_queue_id()).collect()
    }

    fn allocate_ids(
        strategy: &dyn AllocateMessageQueueStrategy,
        consumer_group: &str,
        current_cid: &str,
        mq_all: &[MessageQueue],
        cid_all: &[String],
    ) -> Vec<i32> {
        let got = strategy
            .allocate(consumer_group, current_cid, mq_all, cid_all)
            .expect("内置策略不返回 Err");
        queue_ids(&got)
    }

    /// 一条对拍用例：队列数 / 消费者数 / 逐个消费者（按 `CID_PREFIX0..N` 顺序）的期望队列下标。
    #[derive(Debug)]
    struct Case {
        mq_size: i32,
        cid_size: usize,
        expect: &'static [&'static [i32]],
    }

    const AVERAGELY_CASES: &[Case] = &[
        // Java AllocateMessageQueueAveragelyTest：10 队列 / 4 消费者 → size {3,3,2,2}
        Case {
            mq_size: 10,
            cid_size: 4,
            expect: &[&[0, 1, 2], &[3, 4, 5], &[6, 7], &[8, 9]],
        },
        // 任务要求的另一种不整除：8 / 3 → mod=2，前两个各 3 条
        Case {
            mq_size: 8,
            cid_size: 3,
            expect: &[&[0, 1, 2], &[3, 4, 5], &[6, 7]],
        },
        // 整除
        Case {
            mq_size: 9,
            cid_size: 3,
            expect: &[&[0, 1, 2], &[3, 4, 5], &[6, 7, 8]],
        },
        // 队列数 == 消费者数
        Case {
            mq_size: 4,
            cid_size: 4,
            expect: &[&[0], &[1], &[2], &[3]],
        },
        // 队列比消费者少：只有前 2 个各 1 条，其余空（Python average_size == 0 分支）
        Case {
            mq_size: 2,
            cid_size: 4,
            expect: &[&[0], &[1], &[], &[]],
        },
        Case {
            mq_size: 1,
            cid_size: 3,
            expect: &[&[0], &[], &[]],
        },
        // 单消费者全拿
        Case {
            mq_size: 3,
            cid_size: 1,
            expect: &[&[0, 1, 2]],
        },
        // 空 mq_all（守卫）
        Case {
            mq_size: 0,
            cid_size: 2,
            expect: &[&[], &[]],
        },
    ];

    const CIRCLE_CASES: &[Case] = &[
        // Java AllocateMessageQueueAveragelyByCircleTest：10 / 4 → {0,4,8} {1,5,9} {2,6} {3,7}
        Case {
            mq_size: 10,
            cid_size: 4,
            expect: &[&[0, 4, 8], &[1, 5, 9], &[2, 6], &[3, 7]],
        },
        Case {
            mq_size: 8,
            cid_size: 3,
            expect: &[&[0, 3, 6], &[1, 4, 7], &[2, 5]],
        },
        Case {
            mq_size: 9,
            cid_size: 3,
            expect: &[&[0, 3, 6], &[1, 4, 7], &[2, 5, 8]],
        },
        Case {
            mq_size: 4,
            cid_size: 4,
            expect: &[&[0], &[1], &[2], &[3]],
        },
        Case {
            mq_size: 2,
            cid_size: 4,
            expect: &[&[0], &[1], &[], &[]],
        },
        Case {
            mq_size: 1,
            cid_size: 3,
            expect: &[&[0], &[], &[]],
        },
        Case {
            mq_size: 3,
            cid_size: 1,
            expect: &[&[0, 1, 2]],
        },
        Case {
            mq_size: 0,
            cid_size: 2,
            expect: &[&[], &[]],
        },
    ];

    fn run_table(strategy: &dyn AllocateMessageQueueStrategy, cases: &[Case]) {
        for case in cases {
            let mq_all = create_message_queue_list(case.mq_size);
            let cid_all = create_consumer_id_list(case.cid_size);
            assert_eq!(
                case.expect.len(),
                case.cid_size,
                "用例 cid 数与期望数不一致: {:?}",
                case
            );
            for (index, expected) in case.expect.iter().enumerate() {
                let got = allocate_ids(
                    strategy,
                    "ConsumerGroupTest",
                    &cid_all[index],
                    &mq_all,
                    &cid_all,
                );
                assert_eq!(
                    got,
                    *expected,
                    "{} mq_size={} cid_size={} index={}",
                    strategy.get_name(),
                    case.mq_size,
                    case.cid_size,
                    index
                );
            }
        }
    }

    #[test]
    fn averagely_partition_table() {
        run_table(&AllocateMessageQueueAveragely, AVERAGELY_CASES);
    }

    #[test]
    fn averagely_by_circle_partition_table() {
        run_table(&AllocateMessageQueueAveragelyByCircle, CIRCLE_CASES);
    }

    /// Java `AllocateMessageQueueAveragelyTest`：只断言 size，这里原样对拍一遍。
    #[test]
    fn averagely_matches_java_unit_test_sizes() {
        let cid_all = create_consumer_id_list(4);
        let mq_all = create_message_queue_list(10);
        let sizes: Vec<usize> = cid_all
            .iter()
            .map(|cid| {
                AllocateMessageQueueAveragely
                    .allocate("", cid, &mq_all, &cid_all)
                    .expect("内置策略不返回 Err")
                    .len()
            })
            .collect();
        assert_eq!(sizes, vec![3, 3, 2, 2]);
    }

    /// Java `AllocateMessageQueueAveragelyByCircleTest` 的第一段断言：
    /// `currentCID` 不在 `cidAll` 里 → size 0（其余守卫场景见 [`guards_return_empty_result`]）。
    #[test]
    fn circle_matches_java_unit_test_not_in_cid_all() {
        let cid_all = create_consumer_id_list(4);
        let mq_all = create_message_queue_list(10);
        let got =
            AllocateMessageQueueAveragelyByCircle.allocate("", "CID_PREFIX", &mq_all, &cid_all);
        assert_eq!(got.expect("内置策略不返回 Err").len(), 0);
    }

    /// 守卫：与 Python 一致返回空，而不是像 Java 那样抛 `IllegalArgumentException`。
    #[test]
    fn guards_return_empty_result() {
        // (用例名, current_cid, mq_all 条数, cid_all 条数) —— 每行各触发一条守卫
        let cases: &[(&str, &str, i32, usize)] = &[
            ("blank current cid", "", 4, 2),
            ("empty mq_all", "CID_PREFIX0", 0, 2),
            ("empty cid_all", "CID_PREFIX0", 4, 0),
            ("current cid not in cid_all", "CID_NOT_IN_LIST", 4, 2),
        ];
        let strategies: Vec<(&str, Arc<dyn AllocateMessageQueueStrategy>)> = vec![
            ("AVG", Arc::new(AllocateMessageQueueAveragely)),
            (
                "AVG_BY_CIRCLE",
                Arc::new(AllocateMessageQueueAveragelyByCircle),
            ),
        ];
        for (name, current_cid, mq_size, cid_size) in cases {
            let mq_all = create_message_queue_list(*mq_size);
            let cid_all = create_consumer_id_list(*cid_size);
            for (strategy_name, strategy) in &strategies {
                let got = allocate_ids(
                    strategy.as_ref(),
                    "ConsumerGroupTest",
                    current_cid,
                    &mq_all,
                    &cid_all,
                );
                assert!(
                    got.is_empty(),
                    "{} / {} 应返回空结果（Java 此处抛 IllegalArgumentException）: {:?}",
                    strategy_name,
                    name,
                    got
                );
            }
        }
    }

    /// Java `AllocateMessageQueueByConfigTest`：setMessageQueueList(4 个队列)，
    /// 2 个消费者都拿到 `[0,1,2,3]`。
    #[test]
    fn by_config_matches_java_unit_test() {
        let cid_all = create_consumer_id_list(2);
        let mq_all = create_message_queue_list(4);
        let strategy = AllocateMessageQueueByConfig::default();
        strategy.set_message_queue_list(mq_all.clone());
        for cid in &cid_all {
            let got = allocate_ids(&strategy, "", cid, &mq_all, &cid_all);
            assert_eq!(got, vec![0, 1, 2, 3]);
        }
        assert_eq!(
            queue_ids(&strategy.get_message_queue_list()),
            vec![0, 1, 2, 3]
        );
    }

    /// Java `AllocateMessageQueueByConfig#allocate` 不做 `check`，Python 亦然：
    /// 守卫场景（空 group / 空 cid_all / current_cid 不在列表）照样返回配置值。
    #[test]
    fn by_config_ignores_all_guards() {
        let strategy = AllocateMessageQueueByConfig::new(create_message_queue_list(2));
        let cid_all: Vec<String> = vec![];
        assert_eq!(
            allocate_ids(&strategy, "", "", &create_message_queue_list(0), &cid_all),
            vec![0, 1]
        );
        assert_eq!(
            allocate_ids(
                &strategy,
                "",
                "anyCID",
                &create_message_queue_list(5),
                &cid_all
            ),
            vec![0, 1]
        );
    }

    /// 未配置 = 空列表（Python 口径），且 `allocate` 给的是副本、改配置不影响已拿到的结果。
    #[test]
    fn by_config_defaults_empty_and_returns_copy() {
        let strategy = AllocateMessageQueueByConfig::default();
        assert!(strategy.get_message_queue_list().is_empty());
        let before = strategy
            .allocate("", "CID0", &[], &[])
            .expect("内置策略不返回 Err");
        strategy.set_message_queue_list(create_message_queue_list(3));
        assert!(before.is_empty(), "返回的必须是副本");
        assert_eq!(queue_ids(&before), Vec::<i32>::new());
        assert_eq!(
            queue_ids(
                &strategy
                    .allocate("", "CID0", &[], &[])
                    .expect("内置策略不返回 Err")
            ),
            vec![0, 1, 2]
        );
    }

    /// 策略被 `Arc` 共享后仍可重新配置（Java 引用语义）。
    #[test]
    fn by_config_is_configurable_through_shared_reference() {
        let shared = Arc::new(AllocateMessageQueueByConfig::default());
        let reader: Arc<dyn AllocateMessageQueueStrategy> = shared.clone();
        assert!(allocate_ids(
            reader.as_ref(),
            "G",
            "CID0",
            &create_message_queue_list(1),
            &create_consumer_id_list(1)
        )
        .is_empty());
        shared.set_message_queue_list(create_message_queue_list(2));
        assert_eq!(
            allocate_ids(
                reader.as_ref(),
                "G",
                "CID0",
                &create_message_queue_list(1),
                &create_consumer_id_list(1)
            ),
            vec![0, 1]
        );
    }

    /// Java `getName()` 的字面值，同时验证 trait 是 object-safe 的。
    #[test]
    fn names_match_java_and_trait_is_object_safe() {
        let strategies: Vec<Arc<dyn AllocateMessageQueueStrategy>> = vec![
            Arc::new(AllocateMessageQueueAveragely),
            Arc::new(AllocateMessageQueueAveragelyByCircle),
            Arc::new(AllocateMessageQueueByConfig::default()),
        ];
        let names: Vec<&str> = strategies.iter().map(|s| s.get_name()).collect();
        assert_eq!(names, vec!["AVG", "AVG_BY_CIRCLE", "CONFIG"]);
    }

    /// `Default` 构造口（Java 无参构造）。
    #[test]
    fn default_constructible() {
        let averagely: AllocateMessageQueueAveragely = Default::default();
        let circle: AllocateMessageQueueAveragelyByCircle = Default::default();
        let by_config: AllocateMessageQueueByConfig = Default::default();
        let strategies: Vec<&dyn AllocateMessageQueueStrategy> =
            vec![&averagely, &circle, &by_config];
        assert_eq!(
            strategies
                .iter()
                .map(|s| s.get_name())
                .collect::<Vec<&str>>(),
            vec!["AVG", "AVG_BY_CIRCLE", "CONFIG"]
        );
    }

    /// 全覆盖 + 不重叠：任意 (队列数, 消费者数) 组合下，所有消费者的并集恰为 `mq_all`。
    #[test]
    fn partition_covers_every_queue_exactly_once() {
        for mq_size in 0..=7i32 {
            for cid_size in 1..=5usize {
                let mq_all = create_message_queue_list(mq_size);
                let cid_all = create_consumer_id_list(cid_size);
                for strategy in [
                    &AllocateMessageQueueAveragely as &dyn AllocateMessageQueueStrategy,
                    &AllocateMessageQueueAveragelyByCircle,
                ] {
                    let mut all: Vec<i32> = Vec::new();
                    for cid in &cid_all {
                        all.extend(allocate_ids(strategy, "G", cid, &mq_all, &cid_all));
                    }
                    assert_eq!(
                        all.len(),
                        mq_size as usize,
                        "{} {}×{}",
                        strategy.get_name(),
                        mq_size,
                        cid_size
                    );
                    all.sort_unstable();
                    let distinct: Vec<i32> = all
                        .into_iter()
                        .collect::<std::collections::HashSet<i32>>()
                        .into_iter()
                        .collect();
                    assert_eq!(
                        distinct.len(),
                        mq_size as usize,
                        "{} 分配重叠",
                        strategy.get_name()
                    );
                }
            }
        }
    }

    /// 稳定性：同一输入重复分配结果逐位相同，且输出保持输入里的相对顺序。
    #[test]
    fn allocation_is_deterministic_and_order_preserving() {
        // 故意打乱（降序 + 混 broker 名），策略按下标切，不按值排序。
        let mq_all: Vec<MessageQueue> = (0..10i32)
            .rev()
            .map(|i| MessageQueue::new("topic", if i % 2 == 0 { "brokerA" } else { "brokerB" }, i))
            .collect();
        let cid_all = create_consumer_id_list(4);
        for strategy in [
            &AllocateMessageQueueAveragely as &dyn AllocateMessageQueueStrategy,
            &AllocateMessageQueueAveragelyByCircle,
        ] {
            for cid in &cid_all {
                let once = strategy.allocate("G", cid, &mq_all, &cid_all).expect("ok");
                let twice = strategy.allocate("G", cid, &mq_all, &cid_all).expect("ok");
                assert_eq!(once, twice, "{} 结果不稳定", strategy.get_name());
                // 输出必须是输入的一个有序子序列（相对顺序不变）
                let mut rest = mq_all.as_slice();
                for mq in &once {
                    let pos = rest
                        .iter()
                        .position(|x| x == mq)
                        .expect("输出必须是输入的元素");
                    rest = &rest[pos + 1..];
                }
                let ids = queue_ids(&once);
                let positions: Vec<usize> = ids
                    .iter()
                    .map(|id| {
                        mq_all
                            .iter()
                            .position(|mq| mq.get_queue_id() == *id)
                            .expect("存在")
                    })
                    .collect();
                assert!(
                    positions.windows(2).all(|w| w[0] < w[1]),
                    "输出顺序被打乱: {:?}",
                    ids
                );
            }
        }
    }

    /// 逐值对拍 Python `AllocateMessageQueueAveragely.allocate`（`consumer.py:163-183`）
    /// 与 `AllocateMessageQueueAveragelyByCircle.allocate`（`consumer.py:189-199`）。
    #[test]
    fn matches_python_reference_arithmetic() {
        // 直译 Python 源码，不参与 Rust 实现，仅用于差分测试。
        fn python_averagely(mq_all: &[i32], cid_all: &[String], current_cid: &str) -> Vec<i32> {
            if mq_all.is_empty() {
                return vec![];
            }
            if cid_all.is_empty() || !cid_all.iter().any(|c| c == current_cid) {
                return vec![];
            }
            let index = cid_all
                .iter()
                .position(|c| c == current_cid)
                .expect("positioned");
            let modulo = mq_all.len() % cid_all.len();
            let average_size = mq_all.len() / cid_all.len();
            if average_size == 0 {
                return if index < mq_all.len() {
                    vec![mq_all[index]]
                } else {
                    vec![]
                };
            }
            let start_index;
            let end_index;
            if modulo > 0 && index < modulo {
                start_index = index * (average_size + 1);
                end_index = start_index + average_size + 1;
            } else {
                start_index = modulo * (average_size + 1) + (index - modulo) * average_size;
                end_index = start_index + average_size;
            }
            let end = end_index.min(mq_all.len());
            if start_index >= end {
                return vec![];
            }
            mq_all[start_index..end].to_vec()
        }

        fn python_circle(mq_all: &[i32], cid_all: &[String], current_cid: &str) -> Vec<i32> {
            if mq_all.is_empty() {
                return vec![];
            }
            if cid_all.is_empty() || !cid_all.iter().any(|c| c == current_cid) {
                return vec![];
            }
            let index = cid_all
                .iter()
                .position(|c| c == current_cid)
                .expect("positioned");
            let mut out = vec![];
            let mut i = index;
            while i < mq_all.len() {
                out.push(mq_all[i]);
                i += cid_all.len();
            }
            out
        }

        for mq_size in 0..=12i32 {
            for cid_size in 0..=5usize {
                let mq_all = create_message_queue_list(mq_size);
                let cid_all = create_consumer_id_list(cid_size);
                let ids: Vec<i32> = (0..mq_size).collect();
                let probes = if cid_size == 0 {
                    vec!["CID_PREFIX0".to_string(), String::new()]
                } else {
                    let mut probes: Vec<String> = cid_all.clone();
                    probes.push("CID_NOT_IN_LIST".to_string());
                    probes.push(String::new());
                    probes
                };
                for cid in &probes {
                    assert_eq!(
                        allocate_ids(&AllocateMessageQueueAveragely, "G", cid, &mq_all, &cid_all),
                        python_averagely(&ids, &cid_all, cid),
                        "Python 对拍不一致 AVG {}×{} cid={:?}",
                        mq_size,
                        cid_size,
                        cid
                    );
                    assert_eq!(
                        allocate_ids(
                            &AllocateMessageQueueAveragelyByCircle,
                            "G",
                            cid,
                            &mq_all,
                            &cid_all
                        ),
                        python_circle(&ids, &cid_all, cid),
                        "Python 对拍不一致 CIRCLE {}×{} cid={:?}",
                        mq_size,
                        cid_size,
                        cid
                    );
                }
            }
        }
    }

    /// 真实队列（同 topic 挂多个 brokerName）也一律按下标切分，与 broker 名 / queueId 无关。
    #[test]
    fn allocate_real_queues_from_multiple_brokers() {
        let mq_all: Vec<MessageQueue> = [
            ("broker-a", 0),
            ("broker-a", 1),
            ("broker-b", 0),
            ("broker-b", 1),
            ("broker-c", 0),
        ]
        .iter()
        .map(|(broker, queue_id)| MessageQueue::new("TopicTest", broker, *queue_id))
        .collect();
        let cid_all = create_consumer_id_list(2);
        let first = AllocateMessageQueueAveragely
            .allocate("G", &cid_all[0], &mq_all, &cid_all)
            .expect("ok");
        let second = AllocateMessageQueueAveragely
            .allocate("G", &cid_all[1], &mq_all, &cid_all)
            .expect("ok");
        assert_eq!(first.len(), 3);
        assert_eq!(second.len(), 2);
        assert_eq!(first[2].get_broker_name(), "broker-b");
        assert_eq!(first[2].get_queue_id(), 0);
        assert_eq!(second[0].get_broker_name(), "broker-b");
        assert_eq!(second[0].get_queue_id(), 1);
    }
}
