//! 队列分配策略（rebalance 时把某个 topic 的队列分给同一消费组里的各个消费者）。
//!
//! 对应 Java `org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy` 与实现类
//! `org.apache.rocketmq.client.consumer.rebalance.{AbstractAllocateMessageQueueStrategy,
//! AllocateMessageQueueAveragely, AllocateMessageQueueAveragelyByCircle,
//! AllocateMessageQueueByConfig, AllocateMessageQueueConsistentHash,
//! AllocateMessageQueueByMachineRoom, AllocateMachineRoomNearby}`
//! （`#allocate` / `#getName` / `#check`）；
//! 逐条对齐参考实现 `python/rocketmq/client/consumer.py:153-259`（机房/一致性哈希三个
//! 类在 `consumer.py:264-620`）。
//!
//! 注：本仓库的 Java 快照把这些实现类放在 `client.consumer.rebalance` 包下（部分上游版本
//! 叫 `client.consumer.allocation`），类名与方法名完全一致，下文引用一律用快照里的真实路径。
//!
//! ## 移植范围
//!
//! Java 快照里的 6 个策略全部移植：`AVG` / `AVG_BY_CIRCLE` / `CONFIG` /
//! `CONSISTENT_HASH`（要 [`crate::common::consistent_hash`] 的一致性哈希环）/
//! `MACHINE_ROOM` / `MACHINE_ROOM_NEARBY-<内层>`（代理模式，见
//! [`AllocateMachineRoomNearby`]）。
//!
//! ## 与 Java 的有意差异（一律跟随 Python）
//!
//! 1. **守卫不抛异常**：Java `AbstractAllocateMessageQueueStrategy#check` 在 `currentCID`
//!    为空串、`mqAll` 为空、`cidAll` 为空这三种情况下抛
//!    `IllegalArgumentException("currentCID is empty" / "mqAll is null or mqAll empty" /
//!    "cidAll is null or cidAll empty")`；Python（`consumer.py:195-200`、`227-232`）
//!    一律 `return []`。这里跟随 Python：非法入参 → 空结果，**不**返回 `Err`。
//!    理由是 rebalance 是后台周期任务，一条脏入参不该把消费者打挂。
//!    唯一的例外是 [`AllocateMachineRoomNearby`]：resolver 给出空机房时 Java 抛
//!    `IllegalArgumentException`，Python 与本文件一律**照抛**（`Err`），因为静默返回空
//!    等于把整个 topic 的队列撤走，而 rebalance 抓住异常反而能保住现有分配。
//! 2. **`AllocateMessageQueueByConfig` 的未配置态**：Java 直接 `return this.messageQueueList`，
//!    没配过就是 `null`；Python 的 `__init__` 把它规整成 `[]` 且 `allocate` 返回
//!    `list(...)` 副本（`consumer.py:252/256`）。这里取 Python 口径：空列表 + 返回副本。
//!    同理，Java 的 `AllocateMachineRoomNearby` 构造器对 null 参数抛 NPE，这里由
//!    `Arc<dyn ...>` 在类型上就排除了空值，没有对应的失败分支。
//! 3. **`allocate` 的 `Result`**：因为守卫不再报错，本模块的策略只在"配置本身不合法"
//!    （构造期 `virtual_node_cnt < 0`、resolver 给出空机房、内层策略报错）时返回 `Err`；
//!    `Result` 同时也是自定义策略（业务方自己实现 [`AllocateMessageQueueStrategy`]）的
//!    出错口子，与 Java「策略里抛异常 → rebalance 记 error」的扩展点位置一致。
//! 4. **`check_config` 的判定顺序**：Java 先看 `currentCID`、再看 `mqAll`、最后看 `cidAll`，
//!    三处守卫都只影响结果、不影响彼此；本模块按 Java 顺序，并把 Java `check` 里那条
//!    `[BUG] ... not in cidAll` 的 info 日志一并移植（Python 同款，日志不改变行为）。
//! 5. **broker 名的 `@` 切分**：Java `AllocateMessageQueueByMachineRoom` 用
//!    `String#split("@")`，它会**丢掉末尾空段**（`"room1@"` → 1 段、`"room1@b@"` → 2 段），
//!    而 `str::split` / Python `str.split` 都不丢。本模块用 [`java_split`] 复刻 Java 口径，
//!    否则一条队列参不参与分配会与 Java 客户端不一致。

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::{Arc, RwLock};

use crate::common::consistent_hash::{ClientNode, ConsistentHashRouter, HashFunction, Node};
use crate::common::message::MessageQueue;
use crate::error::{Error, Result};
use crate::rmq_info;

/// 队列分配策略接口，对应 Java `AllocateMessageQueueStrategy#allocate` +
/// `#getName`（Python `consumer.py:153` 的 `AllocateMessageQueueStrategy`）。
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
    ///   （`consumer.py:1122` `sorted(..., key=_mq_sort_key)`）；
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
    /// `CONFIG` / `CONSISTENT_HASH` / `MACHINE_ROOM` / `MACHINE_ROOM_NEARBY-<内层>`），
    /// Java 侧用于日志与策略识别。
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
/// （`consumer.py:189-215`）。
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

        // consumer.py:204-205：队列数少于消费者数时，只有下标 < modulo 的客户端拿到 1 条。
        // Java 靠 `averageSize = 1` + `Math.min(averageSize, mq - startIndex)` 得到同样结果。
        if average_size == 0 {
            return Ok(if index < mq_size {
                vec![mq_all[index].clone()]
            } else {
                Vec::new()
            });
        }

        // consumer.py:209-215
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
/// （`consumer.py:224-237`）。
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
/// `AllocateMessageQueueByConfig`（`consumer.py:243-256`）。
///
/// Java 与 Python 的 `allocate` 都**不调 `check`**，所以空 group / 空 `cid_all` 也照样返回配置值。
#[derive(Debug, Default)]
pub struct AllocateMessageQueueByConfig {
    message_queue_list: RwLock<Vec<MessageQueue>>,
}

impl AllocateMessageQueueByConfig {
    /// 对应 Python `AllocateMessageQueueByConfig(message_queue_list=[...])`
    /// （`consumer.py:252`）；Java 只有隐式无参构造 + `setMessageQueueList`。
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

// --------------------------------------------------------- ConsistentHash（CONSISTENT_HASH）

/// 对应 Java `String#split(String)`（limit = 0）：**丢掉末尾的空段**。
///
/// 与 `str::split` / Python `str.split` 的唯一区别就在尾上空段：
/// `"room1@"` Java 得 `["room1"]`（1 段），Rust/Python 得 `["room1", ""]`（2 段）。
/// 但**没命中分隔符时** Java 走的是 `Pattern#split` 里 "If no match was found, return
/// this" 那条早返回，整串原样给出（哪怕它本身就是空串），所以不能无条件裁尾。
/// 上面的取值是 JDK 17 实测：`""`→1 段、`"@"`→0 段、`"@@\n"`→…、`"room1@b@"`→2 段。
fn java_split(text: &str, sep: char) -> Vec<&str> {
    let mut parts: Vec<&str> = text.split(sep).collect();
    if parts.len() == 1 {
        return parts;
    }
    while parts.last().is_some_and(|part| part.is_empty()) {
        parts.pop();
    }
    parts
}

/// 一致性哈希分配：把每个 clientId 哈希成 `virtual_node_cnt` 个虚拟节点铺在环上，
/// 再把每个队列（按 Java `MessageQueue#toString`）路由到顺时针第一个节点所属的消费者。
///
/// 对应 Java `AllocateMessageQueueConsistentHash`（`getName()` = `"CONSISTENT_HASH"`）、
/// Python `AllocateMessageQueueConsistentHash`（`consumer.py:388-431`）。
///
/// 与 AVG / AVG_BY_CIRCLE 的差别不是"分得均不均"，而是**稳定性**：队列数或消费者数变化时，
/// 只有落在新增/移除节点之间弧段上的队列会换主（Java 单测
/// `AllocateMessageQueueConsitentHashTest` 正断言这点），AVG 会把所有人的分界整体挪掉。
///
/// ⚠ 哈希的 key 必须是 [`MessageQueue::to_java_string`]：换一种写法就是另一个环，
/// 与 Java 客户端混跑时表现为全体队列换主（重复 / 漏消费），而不是"分得稍有不均"。
pub struct AllocateMessageQueueConsistentHash {
    virtual_node_cnt: i32,
    custom_hash_function: Option<Arc<dyn HashFunction>>,
}

/// 手写 `Debug`：`Arc<dyn HashFunction>` 没有 `Debug`，派生会因它失败。
impl fmt::Debug for AllocateMessageQueueConsistentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AllocateMessageQueueConsistentHash")
            .field("virtual_node_cnt", &self.virtual_node_cnt)
            .field("custom_hash_function", &self.custom_hash_function.is_some())
            .finish()
    }
}

impl Default for AllocateMessageQueueConsistentHash {
    /// 对应 Java 无参构造 `this(10)`：默认 10 个虚拟节点、默认 MD5Hash。
    fn default() -> AllocateMessageQueueConsistentHash {
        AllocateMessageQueueConsistentHash {
            virtual_node_cnt: 10,
            custom_hash_function: None,
        }
    }
}

impl AllocateMessageQueueConsistentHash {
    /// 对应 Java `AllocateMessageQueueConsistentHash()`。
    pub fn new() -> AllocateMessageQueueConsistentHash {
        Default::default()
    }

    /// 对应 Java `AllocateMessageQueueConsistentHash(int virtualNodeCnt)`。
    pub fn with_virtual_node_cnt(virtual_node_cnt: i32) -> Result<AllocateMessageQueueConsistentHash> {
        Self::with_hash_function(virtual_node_cnt, None)
    }

    /// 对应 Java `AllocateMessageQueueConsistentHash(int, HashFunction)`。
    ///
    /// `virtual_node_cnt < 0` 在 Java 抛 `IllegalArgumentException("illegal virtualNodeCnt :")`，
    /// 这里同抛（`Err`）：构造期错误，不在 rebalance 后台路径上，模块头差异 1 的"守卫返回空"
    /// 口径不适用。
    pub fn with_hash_function(
        virtual_node_cnt: i32,
        custom_hash_function: Option<Arc<dyn HashFunction>>,
    ) -> Result<AllocateMessageQueueConsistentHash> {
        if virtual_node_cnt < 0 {
            return Err(Error::client(format!(
                "illegal virtualNodeCnt :{virtual_node_cnt}"
            )));
        }
        Ok(AllocateMessageQueueConsistentHash {
            virtual_node_cnt,
            custom_hash_function,
        })
    }

    /// 对应 Java 的 `virtualNodeCnt` 字段（只有 getter 语义，改它要重建策略）。
    pub fn get_virtual_node_cnt(&self) -> i32 {
        self.virtual_node_cnt
    }
}

impl AllocateMessageQueueStrategy for AllocateMessageQueueConsistentHash {
    fn allocate(
        &self,
        consumer_group: &str,
        current_cid: &str,
        mq_all: &[MessageQueue],
        cid_all: &[String],
    ) -> Result<Vec<MessageQueue>> {
        if check_config(consumer_group, current_cid, mq_all, cid_all).is_none() {
            return Ok(Vec::new());
        }
        // Java 把 cidAll 逐个包成 ClientNode 建环；customHashFunction 为 null 时用 MD5Hash。
        let cid_nodes: Vec<Arc<dyn Node>> = cid_all
            .iter()
            .map(|cid| Arc::new(ClientNode::new(cid)) as Arc<dyn Node>)
            .collect();
        let router = match &self.custom_hash_function {
            Some(hash_function) => ConsistentHashRouter::with_hash_function(
                &cid_nodes,
                self.virtual_node_cnt,
                hash_function.clone(),
            )?,
            None => ConsistentHashRouter::new(&cid_nodes, self.virtual_node_cnt)?,
        };
        let mut result = Vec::new();
        for mq in mq_all {
            // Java：`routeNode(mq.toString())` 再比 `currentCID.equals(node.getKey())`。
            // 环空时 routeNode 返回 null —— 这里 cid_all 非空 + 虚拟节点数可为 0，
            // 所以 `None` 分支真的会走到（vc=0 ⇒ 环空 ⇒ 谁都拿不到队列）。
            if let Some(node) = router.route_node(&mq.to_java_string()) {
                if node.get_key() == current_cid {
                    result.push(mq.clone());
                }
            }
        }
        Ok(result)
    }

    fn get_name(&self) -> &str {
        "CONSISTENT_HASH"
    }
}

// ------------------------------------------------------- ByMachineRoom（MACHINE_ROOM）

/// 按机房分配：broker 名约定为 `<机房>@<brokerName>`，只有前缀在 `consumeridcs` 白名单里的
/// 队列参与分配，然后在这批队列内部再切一次"平均"。
///
/// 对应 Java `AllocateMessageQueueByMachineRoom`（`getName()` = `"MACHINE_ROOM"`，类注释
/// 里的场景是"支付宝逻辑机房"）、Python `AllocateMessageQueueByMachineRoom`
/// （`consumer.py:434-482`）。
///
/// 分片算式与 [`AllocateMessageQueueAveragely`] **看着像但不同**：Java 这里是
/// `mod = premqAll.size() / cidAll.size()`、`rem = premqAll.size() % cidAll.size()`，
/// 余数队列按 `rem > currentIndex` 分给**前 rem 个**消费者（AVG 的余数是
/// `index < mod` 时给前 mod 个各多一条，切分区间也跟着挪），所以 10 队列 / 5 条属于
/// room1 / 2 消费者的结果是 `[0,1,4]` + `[2,3]`，而不是 AVG 的 `[0,1,2]` + `[3,4]`。
///
/// ⚠ broker 名用 [`java_split`] 切，判据是 Java 的"裁掉尾空段后正好 2 段"。
/// ⚠ Java 的 `consumeridcs` 字段无默认值，没 set 就 `contains` → NPE；这里默认空集合，
/// 表现为"一条都不分"（同模块头差异 1 的口径）。
#[derive(Debug, Default)]
pub struct AllocateMessageQueueByMachineRoom {
    consumeridcs: RwLock<HashSet<String>>,
}

impl AllocateMessageQueueByMachineRoom {
    /// 对应 Python `AllocateMessageQueueByMachineRoom({"room1", ...})`；
    /// Java 只有隐式无参构造 + `setConsumeridcs(Set)`。
    pub fn new<I, S>(consumeridcs: I) -> AllocateMessageQueueByMachineRoom
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        AllocateMessageQueueByMachineRoom {
            consumeridcs: RwLock::new(consumeridcs.into_iter().map(Into::into).collect()),
        }
    }

    /// 对应 Java `#setConsumeridcs(Set<String>)`。取 `&self` 的理由同
    /// [`AllocateMessageQueueByConfig::set_message_queue_list`]。
    pub fn set_consumeridcs<I, S>(&self, consumeridcs: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        *self
            .consumeridcs
            .write()
            .unwrap_or_else(|e| e.into_inner()) =
            consumeridcs.into_iter().map(Into::into).collect();
    }

    /// 对应 Java `#getConsumeridcs()`：返回副本。
    pub fn get_consumeridcs(&self) -> HashSet<String> {
        self.consumeridcs
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

impl AllocateMessageQueueStrategy for AllocateMessageQueueByMachineRoom {
    fn allocate(
        &self,
        consumer_group: &str,
        current_cid: &str,
        mq_all: &[MessageQueue],
        cid_all: &[String],
    ) -> Result<Vec<MessageQueue>> {
        let Some(current_index) =
            check_config(consumer_group, current_cid, mq_all, cid_all)
        else {
            return Ok(Vec::new());
        };
        let rooms = self
            .consumeridcs
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let premq_all: Vec<MessageQueue> = mq_all
            .iter()
            .filter(|mq| {
                // Java: `String[] temp = mq.getBrokerName().split("@");`
                //        `if (temp.length == 2 && consumeridcs.contains(temp[0]))`
                let temp = java_split(mq.get_broker_name(), '@');
                temp.len() == 2 && rooms.contains(temp[0])
            })
            .cloned()
            .collect();

        let cid_size = cid_all.len(); // check_config 已保证 >= 1
        let modulo = premq_all.len() / cid_size;
        let rem = premq_all.len() % cid_size;
        let start_index = modulo * current_index;
        let end_index = start_index + modulo;
        let mut result: Vec<MessageQueue> = premq_all[start_index..end_index].to_vec();
        if rem > current_index {
            // 越界不可能：`current_index < rem` 且 `rem < cid_size`
            // ⇒ `current_index + modulo * cid_size < rem + modulo * cid_size == premq_all.len()`。
            result.push(premq_all[current_index + modulo * cid_size].clone());
        }
        Ok(result)
    }

    fn get_name(&self) -> &str {
        "MACHINE_ROOM"
    }
}

// ------------------------------------------------------------- 机房就近（MACHINE_ROOM_NEARBY）

/// 对应 Java `AllocateMachineRoomNearby.MachineRoomResolver`：告诉策略"谁在哪个机房"。
///
/// Java 注释明确写了两个方法**都不能返回空串/null**（这里按 Java 的
/// `StringUtils.isNoneEmpty` 判空，空串即报错），否则该队列/消费者会被判定为"机房未知"。
pub trait MachineRoomResolver: Send + Sync {
    /// 对应 Java `#brokerDeployIn(MessageQueue)`。
    fn broker_deploy_in(&self, message_queue: &MessageQueue) -> String;

    /// 对应 Java `#consumerDeployIn(String clientID)`。
    fn consumer_deploy_in(&self, client_id: &str) -> String;
}

/// 机房就近代理：把实际的分配算法包一层。
///
/// 对应 Java `AllocateMachineRoomNearby`（`getName()` =
/// `"MACHINE_ROOM_NEARBY" + "-" + 内层策略.getName()`）、Python
/// `AllocateMachineRoomNearby`（`consumer.py:497-...`）。
///
/// 1. 与当前消费者**同机房**的队列只分给同机房的消费者（用内层策略）；
/// 2. **机房里没有任何活消费者**的那些队列，交给全部消费者按内层策略瓜分 ——
///    否则它们就没人消费了。
///
/// Java 的 TreeMap ⇒ 机房按**字典序**遍历，这里用 `BTreeMap` 保持同样的处理顺序。
///
/// 构造参数缺失：Java 抛 `NullPointerException`，Rust 由 `Arc` 在类型上排除（模块头差异 2）。
/// resolver 给出空机房：Java 抛 `IllegalArgumentException`，这里返回 `Err` 而不是空结果
/// （模块头差异 1 的例外，理由同 Python：静默返回空等于把整个 topic 的队列撤走）。
pub struct AllocateMachineRoomNearby {
    allocate_message_queue_strategy: Arc<dyn AllocateMessageQueueStrategy>,
    machine_room_resolver: Arc<dyn MachineRoomResolver>,
    /// Java 的 `getName()` 是拼出来的；Rust 的 `get_name(&self) -> &str` 要求有地方存，
    /// 所以在构造期算好。
    name: String,
}

impl AllocateMachineRoomNearby {
    /// 对应 Java `AllocateMachineRoomNearby(AllocateMessageQueueStrategy, MachineRoomResolver)`。
    pub fn new(
        allocate_message_queue_strategy: Arc<dyn AllocateMessageQueueStrategy>,
        machine_room_resolver: Arc<dyn MachineRoomResolver>,
    ) -> AllocateMachineRoomNearby {
        let name = format!(
            "MACHINE_ROOM_NEARBY-{}",
            allocate_message_queue_strategy.get_name()
        );
        AllocateMachineRoomNearby {
            allocate_message_queue_strategy,
            machine_room_resolver,
            name,
        }
    }
}

impl fmt::Debug for AllocateMachineRoomNearby {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AllocateMachineRoomNearby")
            .field("inner", &self.name)
            .finish()
    }
}

impl AllocateMessageQueueStrategy for AllocateMachineRoomNearby {
    fn allocate(
        &self,
        consumer_group: &str,
        current_cid: &str,
        mq_all: &[MessageQueue],
        cid_all: &[String],
    ) -> Result<Vec<MessageQueue>> {
        if check_config(consumer_group, current_cid, mq_all, cid_all).is_none() {
            return Ok(Vec::new());
        }

        // 按机房分组（Java 的两个 TreeMap）。
        let mut mr_2_mq: BTreeMap<String, Vec<MessageQueue>> = BTreeMap::new();
        for mq in mq_all {
            let room = self.machine_room_resolver.broker_deploy_in(mq);
            if room.is_empty() {
                return Err(Error::client(format!(
                    "Machine room is null for mq {}",
                    mq.to_java_string()
                )));
            }
            mr_2_mq.entry(room).or_default().push(mq.clone());
        }
        let mut mr_2_c: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for cid in cid_all {
            let room = self.machine_room_resolver.consumer_deploy_in(cid);
            if room.is_empty() {
                return Err(Error::client(format!(
                    "Machine room is null for consumer id {cid}"
                )));
            }
            mr_2_c.entry(room).or_default().push(cid.clone());
        }

        let mut allocate_results: Vec<MessageQueue> = Vec::new();

        // 1. 本消费者所在机房的队列：只在同机房消费者之间分。
        //    Java 先 `mr2Mq.remove(currentMachineRoom)` 再 `mr2c.get(...)`，所以该机房
        //    不会重复出现在第 2 步的共享池里。
        let current_machine_room = self.machine_room_resolver.consumer_deploy_in(current_cid);
        let mq_in_this_machine_room = mr_2_mq.remove(&current_machine_room);
        // Java 的 `mr2c.get(room)` 可能为 null（同机房的 cid 一个都没有），此时 check()
        // 直接判假 ⇒ 结果为空；Rust 用空切片表达同一件事。
        let consumer_in_this_machine_room: Vec<String> = mr_2_c
            .get(&current_machine_room)
            .cloned()
            .unwrap_or_default();
        if let Some(mq_in_this_machine_room) =
            mq_in_this_machine_room.filter(|mqs| !mqs.is_empty())
        {
            allocate_results.extend(self.allocate_message_queue_strategy.allocate(
                consumer_group,
                current_cid,
                &mq_in_this_machine_room,
                &consumer_in_this_machine_room,
            )?);
        }

        // 2. 没有活消费者的机房：队列不能没人消费，交给全部消费者。
        for (room, mqs) in &mr_2_mq {
            if !mr_2_c.contains_key(room) {
                allocate_results.extend(self.allocate_message_queue_strategy.allocate(
                    consumer_group,
                    current_cid,
                    mqs,
                    cid_all,
                )?);
            }
        }
        Ok(allocate_results)
    }

    fn get_name(&self) -> &str {
        &self.name
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

    /// 逐值对拍 Python `AllocateMessageQueueAveragely.allocate`（`consumer.py:189-215`）
    /// 与 `AllocateMessageQueueAveragelyByCircle.allocate`（`consumer.py:224-237`）。
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

    // ------------------------------------------------ 一致性哈希（CONSISTENT_HASH）

    /// Java `AllocateMessageQueueConsitentHashTest` 的 `createConsumerIdList`：`CID-i`。
    /// 与 [`create_consumer_id_list`] 的 `CID_PREFIX i` **刻意不同名**——clientId 是要被
    /// 哈希的对象，改一个字符整张表就废了。
    fn ch_cids(size: usize) -> Vec<String> {
        (0..size).map(|i| format!("CID-{i}")).collect()
    }

    /// Java `AllocateMessageQueueConsitentHashTest` 的 `createMessageQueueList`：
    /// `("topic", "brokerName", 0..N)`。这张表里队列的 `toString()` 同样参与哈希。
    fn ch_queues(size: i32) -> Vec<MessageQueue> {
        (0..size)
            .map(|i| MessageQueue::new("topic", "brokerName", i))
            .collect()
    }

    fn run_consistent_hash_table(strategy: &dyn AllocateMessageQueueStrategy, cases: &[Case]) {
        for case in cases {
            let mq_all = ch_queues(case.mq_size);
            let cid_all = ch_cids(case.cid_size);
            assert_eq!(case.expect.len(), case.cid_size, "用例 cid 数不一致: {case:?}");
            for (index, expected) in case.expect.iter().enumerate() {
                let got = allocate_ids(
                    strategy,
                    "testConsumerGroup",
                    &cid_all[index],
                    &mq_all,
                    &cid_all,
                );
                assert_eq!(
                    got,
                    *expected,
                    "{} mq={} cid={} index={} 与 Java 环不一致",
                    strategy.get_name(),
                    case.mq_size,
                    case.cid_size,
                    index
                );
            }
        }
    }

    /// 落点表（`virtualNodeCnt = 3`）。
    ///
    /// 期望值是**真实 Java 5.5.1 客户端**跑出来的（`client/target/classes` 上的
    /// `AllocateMessageQueueConsistentHash`），与 Python
    /// `tests/test_allocate_strategy.py::CONSISTENT_HASH_CASES`、C++/.NET 的表逐值相同：
    /// 四语言 + Java 必须算出同一个环，否则混跑时全体队列换主。
    const CONSISTENT_HASH_CASES: &[Case] = &[
        Case {
            mq_size: 6,
            cid_size: 2,
            expect: &[&[2], &[0, 1, 3, 4, 5]],
        },
        Case {
            mq_size: 6,
            cid_size: 3,
            expect: &[&[2], &[1, 5], &[0, 3, 4]],
        },
        Case {
            mq_size: 10,
            cid_size: 4,
            expect: &[&[2], &[], &[0, 3, 4, 8], &[1, 5, 6, 7, 9]],
        },
        Case {
            mq_size: 20,
            cid_size: 10,
            expect: &[
                &[2, 14, 15],
                &[17],
                &[8, 11],
                &[],
                &[],
                &[1, 5, 9, 10],
                &[6, 7, 12],
                &[13, 18],
                &[16],
                &[0, 3, 4, 19],
            ],
        },
    ];

    /// 同一张算法、默认 `virtualNodeCnt = 10`（Java 无参构造）。
    const CONSISTENT_HASH_DEFAULT_VC_CASES: &[Case] = &[
        Case {
            mq_size: 4,
            cid_size: 2,
            expect: &[&[0, 2], &[1, 3]],
        },
        Case {
            mq_size: 8,
            cid_size: 3,
            expect: &[&[0, 2, 4], &[3, 5, 6, 7], &[1]],
        },
    ];

    #[test]
    fn consistent_hash_matches_the_java_ring_table() {
        let vc3 = AllocateMessageQueueConsistentHash::with_virtual_node_cnt(3).expect("vc>=0");
        run_consistent_hash_table(&vc3, CONSISTENT_HASH_CASES);
        run_consistent_hash_table(&AllocateMessageQueueConsistentHash::new(), CONSISTENT_HASH_DEFAULT_VC_CASES);
        assert_eq!(vc3.get_virtual_node_cnt(), 3);
        assert_eq!(AllocateMessageQueueConsistentHash::default().get_virtual_node_cnt(), 10);
    }

    /// Java `verifyAllocateAll`：任意规模下每条队列恰好分给一个消费者（不重不漏）。
    #[test]
    fn consistent_hash_covers_every_queue_once() {
        let strategy = AllocateMessageQueueConsistentHash::with_virtual_node_cnt(3).expect("ok");
        for mq_size in 1..=11i32 {
            for cid_size in 1..=7usize {
                let mq_all = ch_queues(mq_size);
                let cid_all = ch_cids(cid_size);
                let mut flat: Vec<i32> = Vec::new();
                for cid in &cid_all {
                    flat.extend(allocate_ids(&strategy, "g", cid, &mq_all, &cid_all));
                }
                flat.sort_unstable();
                let expected: Vec<i32> = (0..mq_size).collect();
                assert_eq!(flat, expected, "CONSISTENT_HASH {mq_size}×{cid_size}");
            }
        }
    }

    /// 一致性哈希的全部意义：成员变化只动涉及的那段弧，其它消费者的队列不换主。
    #[test]
    fn consistent_hash_is_stable_when_membership_changes() {
        let strategy = AllocateMessageQueueConsistentHash::with_virtual_node_cnt(3).expect("ok");
        let mq_all = ch_queues(9);
        let cid_all = ch_cids(4);
        let owner_of = |cids: &[String]| -> Vec<String> {
            let mut map: Vec<Option<String>> = vec![None; 9];
            for cid in cids {
                for mq in allocate(&strategy, "g", cid, &mq_all, cids) {
                    map[mq.get_queue_id() as usize] = Some(cid.to_string());
                }
            }
            map.into_iter().map(|o| o.expect("每条队列都有归属")).collect()
        };
        let before = owner_of(&cid_all);

        // 摘掉 CID-0：它原来的队列会被别人接走（那是必须发生的），
        // 但**原本就在别人手里**的队列必须还在那个人手里。
        let remaining: Vec<String> = cid_all[1..].to_vec();
        let after_remove = owner_of(&remaining);
        for (qid, owner) in before.iter().enumerate() {
            if owner != "CID-0" {
                assert_eq!(&after_remove[qid], owner, "摘队时 qid={qid} 不该换主");
            }
        }

        // 加一个新消费者：同理，只有分给 CID-NEW 的队列是新增的。
        let mut joined = remaining.clone();
        joined.push("CID-NEW".to_string());
        let after_add = owner_of(&joined);
        for (qid, owner) in before.iter().enumerate() {
            if after_add[qid] != "CID-NEW" {
                assert_eq!(&after_add[qid], owner, "加人时 qid={qid} 不该换主");
            }
        }
    }

    fn allocate(
        strategy: &dyn AllocateMessageQueueStrategy,
        group: &str,
        cid: &str,
        mq_all: &[MessageQueue],
        cid_all: &[String],
    ) -> Vec<MessageQueue> {
        strategy.allocate(group, cid, mq_all, cid_all).expect("内置策略不返回 Err")
    }

    /// Java 构造函数就抛 `IllegalArgumentException("illegal virtualNodeCnt :")`；
    /// `0` 合法（Java 只挡 `<0`）⇒ 环上没有虚拟节点 ⇒ 一条都分不到。
    #[test]
    fn consistent_hash_rejects_negative_virtual_node_cnt() {
        let err = AllocateMessageQueueConsistentHash::with_virtual_node_cnt(-1)
            .expect_err("必须拒绝");
        assert!(err.to_string().contains("illegal virtualNodeCnt :-1"), "{err}");
        let zero = AllocateMessageQueueConsistentHash::with_virtual_node_cnt(0).expect("0 合法");
        assert!(allocate(&zero, "g", "CID-0", &ch_queues(4), &ch_cids(2)).is_empty());
    }

    /// 自定义 `HashFunction` 必须真的参与建环（Java 允许注入）。
    #[test]
    fn consistent_hash_uses_the_injected_hash_function() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        /// Java 探针里的 `key.charAt(0)`（UTF-16 首码元）。
        struct FirstChar(AtomicUsize);
        impl HashFunction for FirstChar {
            fn hash(&self, key: &str) -> i64 {
                self.0.fetch_add(1, Ordering::Relaxed);
                i64::from(key.encode_utf16().next().unwrap_or(0))
            }
        }

        let counter = Arc::new(FirstChar(AtomicUsize::new(0)));
        let strategy = AllocateMessageQueueConsistentHash::with_hash_function(
            2,
            Some(counter.clone()),
        )
        .expect("vc>=0");
        let mq_all = ch_queues(4);
        let cid_all = ch_cids(2);
        // Java 同一场景的真值：两个 cid 的虚拟节点 key（"CID-0-0"…）首字符都是 'C'
        // ⇒ 哈希相同 ⇒ TreeMap.put 后者覆盖前者，环上只剩 CID-1 ⇒ 队列全归它。
        assert_eq!(allocate_ids(&strategy, "g", "CID-0", &mq_all, &cid_all), Vec::<i32>::new());
        assert_eq!(allocate_ids(&strategy, "g", "CID-1", &mq_all, &cid_all), vec![0, 1, 2, 3]);
        assert!(counter.0.load(Ordering::Relaxed) > 0, "自定义哈希没被调用");
    }

    /// 守卫口径与其它策略一致。
    #[test]
    fn consistent_hash_guards_return_empty() {
        let strategy = AllocateMessageQueueConsistentHash::new();
        let mq_all = ch_queues(4);
        let cid_all = ch_cids(2);
        assert!(allocate(&strategy, "g", "CID-NOT-HERE", &mq_all, &cid_all).is_empty());
        assert!(allocate(&strategy, "g", "", &mq_all, &cid_all).is_empty());
        assert!(allocate(&strategy, "g", "CID-0", &[], &cid_all).is_empty());
        assert!(allocate(&strategy, "g", "CID-0", &mq_all, &[]).is_empty());
    }

    // ----------------------------------------------------- 机房策略（MACHINE_ROOM）

    /// Java `AllocateMessageQueueByMachineRoomTest`：10 队列（0..4 在 room1）+
    /// consumeridcs={room1} + 2 消费者 → `[0,1,4]` / `[2,3]`（真实 Java 客户端复核过）。
    ///
    /// 余数队列给**前 rem 个**消费者（`rem > currentIndex`），与 AVG 的切法不同，
    /// 所以第 3 条落 CID_PREFIX0 而不是 CID_PREFIX1。
    #[test]
    fn by_machine_room_matches_java_unit_test() {
        let mq_all: Vec<MessageQueue> = (0..10i32)
            .map(|i| {
                MessageQueue::new("topic", if i < 5 { "room1@broker-a" } else { "room2@broker-b" }, i)
            })
            .collect();
        let cid_all = create_consumer_id_list(2);
        let strategy = AllocateMessageQueueByMachineRoom::new(["room1"]);
        assert_eq!(
            allocate_ids(&strategy, "G", &cid_all[0], &mq_all, &cid_all),
            vec![0, 1, 4]
        );
        assert_eq!(
            allocate_ids(&strategy, "G", &cid_all[1], &mq_all, &cid_all),
            vec![2, 3]
        );
        assert_eq!(strategy.get_consumeridcs().len(), 1);
    }

    /// broker 名必须是 `机房@名字`（且切分按 Java 的裁尾口径）。
    /// `parts` 列的是 Java `String#split("@")` 的真实段数（JDK 17 实测）。
    #[test]
    fn by_machine_room_uses_java_split_on_the_broker_name() {
        let cid_all = create_consumer_id_list(1);
        let strategy = AllocateMessageQueueByMachineRoom::new(["room1"]);
        let allocate_one = |broker: &str| -> Vec<i32> {
            allocate_ids(
                &strategy,
                "G",
                &cid_all[0],
                &[MessageQueue::new("topic", broker, 0)],
                &cid_all,
            )
        };
        // (broker 名, Java 段数, 是否参与分配)
        let cases: &[(&str, usize, bool)] = &[
            ("room1@broker-a", 2, true),
            ("room1@", 1, false),          // 尾空段被 Java 丢掉
            ("room1@b@", 2, true),         // 裁尾后仍是 2 段
            ("@room1", 2, false),          // 2 段，但机房是空串、不在白名单
            ("room1@broker@a", 3, false),
            ("@", 0, false),
            ("broker-a", 1, false),
            ("", 1, false),
        ];
        for (broker, java_parts, allocated) in cases {
            assert_eq!(
                java_split(broker, '@').len(),
                *java_parts,
                "java_split({broker:?}) 与 Java String#split 段数不一致"
            );
            assert_eq!(
                !allocate_one(broker).is_empty(),
                *allocated,
                "broker={broker:?} 的参与/剔除判定与 Java 不一致"
            );
        }
        // 没配机房 = 一条都不分（Java 此处是 NPE，本端口按"守卫返回空"口径）
        assert!(AllocateMessageQueueByMachineRoom::default()
            .allocate(
                "G",
                &cid_all[0],
                &[MessageQueue::new("topic", "room1@b", 0)],
                &cid_all
            )
            .expect("ok")
            .is_empty());
        // 白名单可替换（Java setter），换完立刻对同一条队列生效
        strategy.set_consumeridcs(["room2"]);
        assert!(strategy.get_consumeridcs().contains("room2"));
        assert!(allocate_one("room1@broker-a").is_empty(), "换白名单后 room1 不再参与");
    }

    /// 机房策略同样吃守卫（Java 的 `check`）。
    #[test]
    fn by_machine_room_guards_return_empty() {
        let mq_all: Vec<MessageQueue> = (0..4i32)
            .map(|i| MessageQueue::new("topic", "room1@broker-a", i))
            .collect();
        let strategy = AllocateMessageQueueByMachineRoom::new(["room1"]);
        for (cid, cid_all) in [
            ("", create_consumer_id_list(2)),
            ("CID_PREFIX0", Vec::new()),
            ("CID_NOT_IN_LIST", create_consumer_id_list(2)),
        ] {
            assert!(
                allocate(&strategy, "G", cid, &mq_all, &cid_all).is_empty(),
                "cid={cid:?} cid_all={:?}",
                cid_all.len()
            );
        }
        assert!(allocate(&strategy, "G", "CID_PREFIX0", &[], &create_consumer_id_list(2)).is_empty());
    }

    // --------------------------------------------- 机房就近代理（MACHINE_ROOM_NEARBY）

    /// Java 测试同款 resolver：broker `IDCx-brokerName` / 消费者 `IDCx-CID-i` 取 '-' 前段。
    struct DashRoom;

    impl MachineRoomResolver for DashRoom {
        fn broker_deploy_in(&self, message_queue: &MessageQueue) -> String {
            message_queue
                .get_broker_name()
                .split('-')
                .next()
                .unwrap_or_default()
                .to_string()
        }

        fn consumer_deploy_in(&self, client_id: &str) -> String {
            client_id.split('-').next().unwrap_or_default().to_string()
        }
    }

    /// Java `AllocateMachineRoomNearbyTest#createMessageQueueList`：
    /// idc_size 个机房 × 每机房 queue_size 条队列。
    fn nearby_mq(idc_size: usize, queue_size: i32) -> Vec<MessageQueue> {
        let mut out = Vec::new();
        for i in 1..=idc_size {
            for q in 0..queue_size {
                out.push(MessageQueue::new("topic", &format!("IDC{i}-brokerName"), q));
            }
        }
        out
    }

    /// Java `#createConsumerIdList`：idc_size 个机房 × 每机房 consumer_size 个消费者。
    fn nearby_cids(idc_size: usize, consumer_size: usize) -> Vec<String> {
        let mut out = Vec::new();
        for i in 1..=idc_size {
            for q in 0..consumer_size {
                out.push(format!("IDC{i}-CID-{q}"));
            }
        }
        out
    }

    /// Java `testWhenIDCSizeEquals`：机房数相等时，每个消费者只拿到**同机房**的队列，
    /// 且全员并集恰好是全集（不重不漏）。四组规模与 Java 参数化用例一致。
    #[test]
    fn nearby_allocates_same_room_only_and_covers_everything() {
        let strategy = AllocateMachineRoomNearby::new(
            Arc::new(AllocateMessageQueueAveragely),
            Arc::new(DashRoom),
        );
        for (idc_size, queue_size, consumer_size) in [(5, 20i32, 10), (5, 20, 20), (5, 20, 30), (5, 20, 1)] {
            let mq_all = nearby_mq(idc_size, queue_size);
            let cid_all = nearby_cids(idc_size, consumer_size);
            let mut flat: Vec<(String, i32)> = Vec::new();
            for cid in &cid_all {
                for mq in allocate(&strategy, "Test-C-G", cid, &mq_all, &cid_all) {
                    assert_eq!(
                        DashRoom.broker_deploy_in(&mq),
                        DashRoom.consumer_deploy_in(cid),
                        "{} 拿到了别的机房的队列 {:?}",
                        cid,
                        mq
                    );
                    flat.push((mq.get_broker_name().to_string(), mq.get_queue_id()));
                }
            }
            let mut expected: Vec<(String, i32)> = mq_all
                .iter()
                .map(|mq| (mq.get_broker_name().to_string(), mq.get_queue_id()))
                .collect();
            assert_eq!(flat.len(), expected.len(), "{idc_size}×{queue_size}×{consumer_size} 有漏/重");
            flat.sort();
            expected.sort();
            assert_eq!(flat, expected, "{idc_size}×{queue_size}×{consumer_size} 并集不等于全集");
        }
    }

    /// Java `testWhenConsumerIDCIsLess`：broker 机房多于消费者机房时，**没有活消费者**的
    /// 机房要交给全部消费者共享（否则没人消费），有消费者的机房仍然只给自己的消费者。
    #[test]
    fn nearby_shares_rooms_that_have_no_consumer() {
        let strategy = AllocateMachineRoomNearby::new(
            Arc::new(AllocateMessageQueueAveragely),
            Arc::new(DashRoom),
        );
        // 真实 Java 客户端：mqs = IDC2×4 + IDC1×2，cids = IDC1 的两个消费者
        let mut mq_all: Vec<MessageQueue> = Vec::new();
        for q in 0..4i32 {
            mq_all.push(MessageQueue::new("topic", "IDC2-brokerName", q));
        }
        for q in 0..2i32 {
            mq_all.push(MessageQueue::new("topic", "IDC1-brokerName", q));
        }
        let cid_all = vec!["IDC1-CID-0".to_string(), "IDC1-CID-1".to_string()];
        assert_eq!(
            queue_ids(&allocate(&strategy, "G", "IDC1-CID-0", &mq_all, &cid_all)),
            vec![0, 0, 1],
            "IDC1 自己 2 条 + 空机房 IDC2 的 4 条按 AVG 分一半"
        );
        let first: Vec<(String, i32)> = allocate(&strategy, "G", "IDC1-CID-0", &mq_all, &cid_all)
            .iter()
            .map(|mq| (mq.get_broker_name().to_string(), mq.get_queue_id()))
            .collect();
        assert_eq!(
            first,
            vec![
                ("IDC1-brokerName".to_string(), 0),
                ("IDC2-brokerName".to_string(), 0),
                ("IDC2-brokerName".to_string(), 1),
            ],
            "同机房队列必须先收，再补空机房的共享队列"
        );
        let second: Vec<(String, i32)> = allocate(&strategy, "G", "IDC1-CID-1", &mq_all, &cid_all)
            .iter()
            .map(|mq| (mq.get_broker_name().to_string(), mq.get_queue_id()))
            .collect();
        assert_eq!(
            second,
            vec![
                ("IDC1-brokerName".to_string(), 1),
                ("IDC2-brokerName".to_string(), 2),
                ("IDC2-brokerName".to_string(), 3),
            ]
        );

        // 5 个机房、只有前 2 个有消费者：每条队列都得有人消费，健康机房不外流。
        let mq_all = nearby_mq(5, 4);
        let cid_all = nearby_cids(2, 3);
        let mut claimed = 0;
        for cid in &cid_all {
            for mq in allocate(&strategy, "Test-C-G", cid, &mq_all, &cid_all) {
                let room = DashRoom.broker_deploy_in(&mq);
                if room == "IDC1" || room == "IDC2" {
                    assert_eq!(room, DashRoom.consumer_deploy_in(cid), "{room} 的队列外流了");
                }
                claimed += 1;
            }
        }
        assert_eq!(claimed, mq_all.len(), "有空机房的场景下必须不重不漏");
    }

    /// `getName()` = `MACHINE_ROOM_NEARBY-<内层策略名>`（Java 复核：
    /// `MACHINE_ROOM_NEARBY-AVG` / `MACHINE_ROOM_NEARBY-AVG_BY_CIRCLE`）。
    #[test]
    fn nearby_name_exposes_the_inner_strategy() {
        let avg = AllocateMachineRoomNearby::new(
            Arc::new(AllocateMessageQueueAveragely),
            Arc::new(DashRoom),
        );
        let circle = AllocateMachineRoomNearby::new(
            Arc::new(AllocateMessageQueueAveragelyByCircle),
            Arc::new(DashRoom),
        );
        assert_eq!(avg.get_name(), "MACHINE_ROOM_NEARBY-AVG");
        assert_eq!(circle.get_name(), "MACHINE_ROOM_NEARBY-AVG_BY_CIRCLE");
        // 内层是 CONFIG 时也一样拼出来
        let by_config = AllocateMachineRoomNearby::new(
            Arc::new(AllocateMessageQueueByConfig::new(create_message_queue_list(1))),
            Arc::new(DashRoom),
        );
        assert_eq!(by_config.get_name(), "MACHINE_ROOM_NEARBY-CONFIG");
    }

    /// resolver 给出空机房 ⇒ Java 抛 IllegalArgumentException。这里返回 `Err` 而不是空结果：
    /// 静默返回空等于把整个 topic 的队列撤走，而 rebalance 抓住错误会保住现有分配。
    #[test]
    fn nearby_raises_when_a_room_is_unknown() {
        struct BlankBrokerRoom;
        impl MachineRoomResolver for BlankBrokerRoom {
            fn broker_deploy_in(&self, _mq: &MessageQueue) -> String {
                String::new()
            }
            fn consumer_deploy_in(&self, _client_id: &str) -> String {
                "IDC1".to_string()
            }
        }
        struct BlankConsumerRoom;
        impl MachineRoomResolver for BlankConsumerRoom {
            fn broker_deploy_in(&self, _mq: &MessageQueue) -> String {
                "IDC1".to_string()
            }
            fn consumer_deploy_in(&self, _client_id: &str) -> String {
                String::new()
            }
        }
        let mq_all = ch_queues(2);
        let cid_all = ch_cids(1);
        let strategy = AllocateMachineRoomNearby::new(
            Arc::new(AllocateMessageQueueAveragely),
            Arc::new(BlankBrokerRoom),
        );
        let err = strategy
            .allocate("G", "CID-0", &mq_all, &cid_all)
            .expect_err("空机房必须报错");
        assert!(
            err.to_string()
                .contains("Machine room is null for mq MessageQueue [topic=topic, brokerName=brokerName, queueId=0]"),
            "{err}"
        );
        let strategy = AllocateMachineRoomNearby::new(
            Arc::new(AllocateMessageQueueAveragely),
            Arc::new(BlankConsumerRoom),
        );
        let err = strategy
            .allocate("G", "CID-0", &mq_all, &cid_all)
            .expect_err("空机房必须报错");
        assert!(
            err.to_string().contains("Machine room is null for consumer id CID-0"),
            "{err}"
        );
        // 守卫仍然优先：current_cid 不在 cid_all 时先返回空，不碰 resolver
        let guard = AllocateMachineRoomNearby::new(
            Arc::new(AllocateMessageQueueAveragely),
            Arc::new(BlankBrokerRoom),
        );
        assert!(guard.allocate("G", "CID-0", &mq_all, &[]).expect("守卫不报错").is_empty());
    }

    /// Java `getName()` 的字面值（六个策略齐全）。
    #[test]
    fn all_six_java_strategy_names_match() {
        let strategies: Vec<Arc<dyn AllocateMessageQueueStrategy>> = vec![
            Arc::new(AllocateMessageQueueAveragely),
            Arc::new(AllocateMessageQueueAveragelyByCircle),
            Arc::new(AllocateMessageQueueByConfig::default()),
            Arc::new(AllocateMessageQueueConsistentHash::new()),
            Arc::new(AllocateMessageQueueByMachineRoom::default()),
            Arc::new(AllocateMachineRoomNearby::new(
                Arc::new(AllocateMessageQueueConsistentHash::new()),
                Arc::new(DashRoom),
            )),
        ];
        let names: Vec<&str> = strategies.iter().map(|s| s.get_name()).collect();
        assert_eq!(
            names,
            vec![
                "AVG",
                "AVG_BY_CIRCLE",
                "CONFIG",
                "CONSISTENT_HASH",
                "MACHINE_ROOM",
                "MACHINE_ROOM_NEARBY-CONSISTENT_HASH"
            ]
        );
    }

    /// 三个新策略在 push / lite 两个消费者上都能换上并读回（暴露面与旧策略一致）。
    #[test]
    fn new_strategies_plug_into_the_consumers() {
        use crate::client::consumer::DefaultMQPushConsumer;
        use crate::client::pull_consumer::DefaultLitePullConsumer;

        let mq_all = ch_queues(4);
        let cid_all = ch_cids(1);
        let push = DefaultMQPushConsumer::new("G_test").expect("ok");
        push.set_allocate_message_queue_strategy(Arc::new(
            AllocateMessageQueueConsistentHash::with_virtual_node_cnt(3).expect("ok"),
        ));
        let strategy = push.allocate_message_queue_strategy();
        assert_eq!(strategy.get_name(), "CONSISTENT_HASH");
        // 单消费者 + 哈希环：4 条队列全落在自己身上
        assert_eq!(queue_ids(&allocate(strategy.as_ref(), "g", "CID-0", &mq_all, &cid_all)), vec![0, 1, 2, 3]);

        let lite = DefaultLitePullConsumer::new("G_lite").expect("ok");
        lite.set_allocate_message_queue_strategy(Arc::new(
            AllocateMessageQueueByMachineRoom::new(["room1"]),
        ));
        let strategy = lite.allocate_message_queue_strategy();
        assert_eq!(strategy.get_name(), "MACHINE_ROOM");
        let room_queues: Vec<MessageQueue> = (0..3i32)
            .map(|i| MessageQueue::new("topic", "room1@broker-a", i))
            .collect();
        assert_eq!(
            queue_ids(&allocate(strategy.as_ref(), "g", "CID-0", &room_queues, &cid_all)),
            vec![0, 1, 2]
        );

        let nearby = AllocateMachineRoomNearby::new(
            Arc::new(AllocateMessageQueueAveragely),
            Arc::new(DashRoom),
        );
        lite.set_allocate_message_queue_strategy(Arc::new(nearby));
        assert_eq!(
            lite.allocate_message_queue_strategy().get_name(),
            "MACHINE_ROOM_NEARBY-AVG"
        );
    }
}
