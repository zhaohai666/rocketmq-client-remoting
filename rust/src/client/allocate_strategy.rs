//! 队列分配策略（rebalance 时把某个 topic 的队列分给同一消费组里的各个消费者）。
//!
//! 每种策略实现 `allocate`（算出本消费者的队列切片）与 `get_name`（策略名）。
//!
//! ## 策略集合
//!
//! 六个策略全部提供：`AVG` / `AVG_BY_CIRCLE` / `CONFIG` /
//! `CONSISTENT_HASH`（要 [`crate::common::consistent_hash`] 的一致性哈希环）/
//! `MACHINE_ROOM` / `MACHINE_ROOM_NEARBY-<内层>`（代理模式，见
//! [`AllocateMachineRoomNearby`）。
//!
//! ## 行为口径
//!
//! 1. **守卫不抛异常**：`current_cid` 为空串、`mq_all` 为空、`cid_all` 为空、
//!    `current_cid` 不在 `cid_all` 里这四种情况一律返回空结果，**不**返回 `Err`。
//!    理由是 rebalance 是后台周期任务，一条脏入参不该把消费者打挂。
//!    唯一的例外是 [`AllocateMachineRoomNearby`]：resolver 给出空机房时
//!    **照抛**（`Err`），因为静默返回空等于把整个 topic 的队列撤走，而 rebalance
//!    抓住异常反而能保住现有分配（算不动保留现有分配、不退化成全要）。
//! 2. **`AllocateMessageQueueByConfig` 的未配置态**：未配置即空列表，且 `allocate`
//!    返回**副本**而非共享可变引用。构造期缺参（Rust 由 `Arc<dyn ...>` 在类型上
//!    排除空值）没有对应的失败分支。
//! 3. **`allocate` 的 `Result`**：因为守卫不再报错，本模块的策略只在"配置本身不合法"
//!    （构造期 `virtual_node_cnt < 0`、resolver 给出空机房、内层策略报错）时返回 `Err`；
//!    `Result` 同时也是自定义策略（业务方自己实现 [`AllocateMessageQueueStrategy`]）的
//!    出错口子——策略报错时 rebalance 记 error 并保住现有分配。
//! 4. **`check_config` 的判定顺序**：先看 `current_cid`、再看 `mq_all`、最后看 `cid_all`，
//!    三处守卫都只影响结果、不影响彼此；`current_cid` 不在 `cid_all` 时打一条
//!    `[BUG] ... not in cidAll` 的 info 日志（日志不改变行为）。
//! 5. **broker 名的 `@` 切分**：约定为 `<机房>@<名字>`，切分时**丢掉末尾空段**
//!    （`"room1@"` → 1 段、`"room1@b@"` → 2 段），与 `str::split` 的默认行为不同。
//!    本模块用 [`split_dropping_trailing_empty`] 复刻这一口径，保证一条队列参不参与
//!    分配的判定稳定。

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::{Arc, RwLock};

use crate::common::consistent_hash::{ClientNode, ConsistentHashRouter, HashFunction, Node};
use crate::common::message::MessageQueue;
use crate::error::{Error, Result};
use crate::rmq_info;

/// 队列分配策略接口：`allocate` 算出本消费者的队列切片，`get_name` 给出策略名。
///
/// 刻意保持 **object-safe**（两个方法都只吃 `&self`，没有泛型参数、没有 `Self` 返回），
/// 这样调用方可以用 `Arc<dyn AllocateMessageQueueStrategy>` 在 rebalance 线程上共享它。
pub trait AllocateMessageQueueStrategy: Send + Sync {
    /// 把 `consumer_group` 的队列按本策略分给 `current_cid`。
    ///
    /// - `consumer_group`：当前消费组，只在「不在 `cid_all` 里」那条日志里出现；
    /// - `current_cid`：本客户端 id，它在 `cid_all` 中的下标决定分哪一段；
    /// - `mq_all`：该 topic 的全部队列。**调用方负责排序**：rebalance 对
    ///   `cid_all`/`mq_all` 都按（brokerName, queueId）排过序，否则分界不稳；
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

    /// 算法名（`AVG` / `AVG_BY_CIRCLE` / `CONFIG` / `CONSISTENT_HASH` /
    /// `MACHINE_ROOM` / `MACHINE_ROOM_NEARBY-<内层>`），用于日志与策略识别；
    /// 字面值已固定，改动会让运维脚本匹配不上。
    fn get_name(&self) -> &str;
}

/// 分配前的公共守卫：`current_cid` 空串、`mq_all` 空、`cid_all` 空、
/// `current_cid` 不在 `cid_all` 里时返回 `None`，由调用方给出空结果；
/// 通过时返回 `current_cid` 在 `cid_all` 中的下标。
///
/// 判空只看 `is_empty`：纯空白串算合法入参。
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
    // [BUG] 日志：本消费者不在组列表里，通常意味着 cidAll 视图滞后，只记日志不改行为。
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
/// 即前 `mod` 个各拿 `average_size + 1` 条，其余拿 `average_size` 条。策略名 `"AVG"`。
///
/// 算式已固定（10 队列 / 4 消费者 → `{3,3,2,2}`），闭式与切片式两种写法**逐值等价**：
/// - 闭式：`averageSize = mq<=cid ? 1 : (mod>0 && index<mod ? mq/cid+1 : mq/cid)`，
///   `startIndex = (mod>0 && index<mod) ? index*averageSize : index*averageSize+mod`，
///   `range = min(averageSize, mq - startIndex)`；
/// - 本实现（切片式）：`start/end` 双分支 + `mq_all[start:end]`。
///   `index*avg + mod` 与 `mod*(avg+1) + (index-mod)*avg` 展开后同为 `index*avg + mod`；
///   `min` 削零则等价于 `average_size == 0` 早返回分支
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
        let modulo = mq_size % cid_size; // 余数 mod
        let average_size = mq_size / cid_size; // 每人基准条数

        // 队列数少于消费者数时，只有下标 < modulo 的客户端拿到 1 条；
        // 靠 `average_size == 0` 早返回实现闭式的 `averageSize = 1` + `min` 削零。
        if average_size == 0 {
            return Ok(if index < mq_size {
                vec![mq_all[index].clone()]
            } else {
                Vec::new()
            });
        }

        // start/end 双分支：前 modulo 个消费者各多拿 1 条
        let (start_index, end_index) = if modulo > 0 && index < modulo {
            let start = index * (average_size + 1);
            (start, start + average_size + 1)
        } else {
            // 这里 index >= modulo（或 modulo == 0），减法不会下溢。
            let start = modulo * (average_size + 1) + (index - modulo) * average_size;
            (start, start + average_size)
        };
        // 闭式的 min(averageSize, mq - startIndex)：越界削成空/截断。
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
/// 即 `mq_all[index], mq_all[index + cid_size], ...`。策略名 `"AVG_BY_CIRCLE"`。
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
        // 「从自己的下标出发、步长 = 消费者数」的等差取数，等价于按下标逐条轮询命中。
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
/// 消费者（通常用于广播式手工绑队列）。策略名 `"CONFIG"`。
///
/// `allocate` **不调守卫**，所以空 group / 空 `cid_all` 也照样返回配置值。
#[derive(Debug, Default)]
pub struct AllocateMessageQueueByConfig {
    message_queue_list: RwLock<Vec<MessageQueue>>,
}

impl AllocateMessageQueueByConfig {
    /// 以给定队列列表构造（未配置即空列表）。
    pub fn new(message_queue_list: Vec<MessageQueue>) -> AllocateMessageQueueByConfig {
        AllocateMessageQueueByConfig {
            message_queue_list: RwLock::new(message_queue_list),
        }
    }

    /// 运行期替换队列列表。
    ///
    /// 取 `&self`（而不是 `&mut self`）：策略对象被 `Arc` 共享、注册到 rebalance 之后，
    /// 再改列表依然对所有后续分配生效；用 `&mut self` 就要求调用方独占这个对象，
    /// 与该用法不符（口径同 [`crate::client::latency::MQFaultStrategy::set_send_latency_fault_enable`]）。
    pub fn set_message_queue_list(&self, message_queue_list: Vec<MessageQueue>) {
        *self
            .message_queue_list
            .write()
            .unwrap_or_else(|e| e.into_inner()) = message_queue_list;
    }

    /// 读取当前配置的队列列表。返回副本（不交出共享可变引用）。
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
        // 未配置返回空列表；每次返回新副本，调用方改动不影响策略本身。
        Ok(self.get_message_queue_list())
    }

    fn get_name(&self) -> &str {
        "CONFIG"
    }
}

// --------------------------------------------------------- ConsistentHash（CONSISTENT_HASH）

/// 按 `sep` 切分 `text` 并**丢掉末尾的空段**。
///
/// 与 `str::split` 的唯一区别就在尾上空段：`"room1@"` 这里得 `["room1"]`（1 段），
/// `str::split` 得 `["room1", ""]`（2 段）。但**没命中分隔符时**整串原样给出
/// （哪怕它本身就是空串），所以不能无条件裁尾。
/// 固定样例（本实现的判定基准）：`""`→1 段、`"@"`→0 段、`"@@\n"`→3 段、`"room1@b@"`→2 段。
fn split_dropping_trailing_empty(text: &str, sep: char) -> Vec<&str> {
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
/// 再把每个队列（按 [`MessageQueue::to_java_string`] 的字符串形态作 key）路由到
/// 顺时针第一个节点所属的消费者。策略名 `"CONSISTENT_HASH"`。
///
/// 与 AVG / AVG_BY_CIRCLE 的差别不是"分得均不均"，而是**稳定性**：队列数或消费者数
/// 变化时，只有落在新增/移除节点之间弧段上的队列会换主，AVG 会把所有人的分界整体挪掉。
///
/// ⚠ 哈希的 key 必须是 [`MessageQueue::to_java_string`]：换一种写法就是另一个环，
/// 同组内 key 形态不一致时表现为全体队列换主（重复 / 漏消费），而不是"分得稍有不均"。
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
    /// 默认 10 个虚拟节点、默认 MD5Hash（等价于 `with_virtual_node_cnt(10)`）。
    fn default() -> AllocateMessageQueueConsistentHash {
        AllocateMessageQueueConsistentHash {
            virtual_node_cnt: 10,
            custom_hash_function: None,
        }
    }
}

impl AllocateMessageQueueConsistentHash {
    /// 默认参数构造：10 个虚拟节点、默认 MD5Hash。
    pub fn new() -> AllocateMessageQueueConsistentHash {
        Default::default()
    }

    /// 指定虚拟节点数构造。
    pub fn with_virtual_node_cnt(virtual_node_cnt: i32) -> Result<AllocateMessageQueueConsistentHash> {
        Self::with_hash_function(virtual_node_cnt, None)
    }

    /// 指定虚拟节点数与哈希函数构造。
    ///
    /// `virtual_node_cnt < 0` 非法，在构造期就报错（`Err`）：不在 rebalance 后台路径上，
    /// 模块头口径 1 的"守卫返回空"不适用。
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

    /// 虚拟节点数（只有 getter 语义，改它要重建策略）。
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
        // 把 cid_all 逐个包成 ClientNode 建环；未注入自定义哈希函数时用默认 MD5Hash。
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
            // 路由 key 用 MessageQueue 的字符串形态，再比节点 key 是否等于 current_cid。
            // 环空时 route_node 返回 None —— 这里 cid_all 非空 + 虚拟节点数可为 0，
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
/// 队列参与分配，然后在这批队列内部再切一次"平均"。策略名 `"MACHINE_ROOM"`。
///
/// 分片算式与 [`AllocateMessageQueueAveragely`] **看着像但不同**：这里是
/// `mod = premqAll.size() / cidAll.size()`、`rem = premqAll.size() % cidAll.size()`，
/// 余数队列按 `rem > currentIndex` 分给**前 rem 个**消费者（AVG 的余数是
/// `index < mod` 时给前 mod 个各多一条，切分区间也跟着挪），所以 10 队列 / 5 条属于
/// room1 / 2 消费者的结果是 `[0,1,4]` + `[2,3]`，而不是 AVG 的 `[0,1,2]` + `[3,4]`。
///
/// ⚠ broker 名用 [`split_dropping_trailing_empty`] 切，判据是"裁掉尾空段后正好 2 段"。
/// ⚠ `consumeridcs` 默认空集合，没设置就"一条都不分"（同模块头口径 1）。
#[derive(Debug, Default)]
pub struct AllocateMessageQueueByMachineRoom {
    consumeridcs: RwLock<HashSet<String>>,
}

impl AllocateMessageQueueByMachineRoom {
    /// 以机房白名单构造。
    pub fn new<I, S>(consumeridcs: I) -> AllocateMessageQueueByMachineRoom
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        AllocateMessageQueueByMachineRoom {
            consumeridcs: RwLock::new(consumeridcs.into_iter().map(Into::into).collect()),
        }
    }

    /// 运行期替换机房白名单。取 `&self` 的理由同
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

    /// 读取机房白名单：返回副本。
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
                // broker 名裁尾空段后正好 2 段、且首段在机房白名单里才参与分配
                let temp = split_dropping_trailing_empty(mq.get_broker_name(), '@');
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

/// 机房解析回调：告诉策略"谁在哪个机房"。
///
/// 两个方法**都不能返回空串**（空串即报错），否则该队列/消费者会被判定为"机房未知"。
pub trait MachineRoomResolver: Send + Sync {
    /// 给出队列所在机房（按 broker 名解析）。
    fn broker_deploy_in(&self, message_queue: &MessageQueue) -> String;

    /// 给出消费者所在机房（按 clientID 解析）。
    fn consumer_deploy_in(&self, client_id: &str) -> String;
}

/// 机房就近代理：把实际的分配算法包一层。策略名为
/// `"MACHINE_ROOM_NEARBY" + "-" + 内层策略.get_name()`。
///
/// 1. 与当前消费者**同机房**的队列只分给同机房的消费者（用内层策略）；
/// 2. **机房里没有任何活消费者**的那些队列，交给全部消费者按内层策略瓜分 ——
///    否则它们就没人消费了。
///
/// 机房按**字典序**遍历（`BTreeMap`），保证处理顺序稳定、结果可复现。
///
/// resolver 给出空机房：返回 `Err` 而不是空结果（模块头口径 1 的例外，
/// 理由：静默返回空等于把整个 topic 的队列撤走，保住现有分配更重要）。
/// 构造参数缺失在类型上已被 `Arc` 排除（模块头口径 2）。
pub struct AllocateMachineRoomNearby {
    allocate_message_queue_strategy: Arc<dyn AllocateMessageQueueStrategy>,
    machine_room_resolver: Arc<dyn MachineRoomResolver>,
    /// 策略名是拼出来的；`get_name(&self) -> &str` 要求有地方存，
    /// 所以在构造期算好。
    name: String,
}

impl AllocateMachineRoomNearby {
    /// 用内层策略与机房解析回调构造。
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

        // 按机房分组（队列、消费者各一张字典序表）。
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
        //    先把该机房从 mr_2_mq 移除，保证它不会重复出现在第 2 步的共享池里。
        let current_machine_room = self.machine_room_resolver.consumer_deploy_in(current_cid);
        let mq_in_this_machine_room = mr_2_mq.remove(&current_machine_room);
        // 同机房的 cid 一个都没有时按空列表处理 ⇒ 本机房队列改由第 2 步兜底共享。
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

    /// 测试夹具：`("topic", "brokerName", 0..size)` 的队列集合。
    fn create_message_queue_list(size: i32) -> Vec<MessageQueue> {
        (0..size)
            .map(|i| MessageQueue::new("topic", "brokerName", i))
            .collect()
    }

    /// 测试夹具：`"CID_PREFIX" + i` 的消费者 id 集合。
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
        // 固定用例：10 队列 / 4 消费者 → size {3,3,2,2}
        Case {
            mq_size: 10,
            cid_size: 4,
            expect: &[&[0, 1, 2], &[3, 4, 5], &[6, 7], &[8, 9]],
        },
        // 另一种不整除：8 / 3 → mod=2，前两个各 3 条
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
        // 队列比消费者少：只有前 2 个各 1 条，其余空（average_size == 0 早返回分支）
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
        // 固定用例：10 / 4 → {0,4,8} {1,5,9} {2,6} {3,7}
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

    /// 固定规模 10 队列 / 4 消费者：只断言各人拿到的 size = {3,3,2,2}。
    #[test]
    fn averagely_matches_canonical_allocation_sizes() {
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

    /// `currentCID` 不在 `cidAll` 里 → size 0（其余守卫场景见 [`guards_return_empty_result`]）。
    #[test]
    fn circle_matches_canonical_not_in_cid_all() {
        let cid_all = create_consumer_id_list(4);
        let mq_all = create_message_queue_list(10);
        let got =
            AllocateMessageQueueAveragelyByCircle.allocate("", "CID_PREFIX", &mq_all, &cid_all);
        assert_eq!(got.expect("内置策略不返回 Err").len(), 0);
    }

    /// 守卫：非法入参返回空结果，而不是报错。
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
                    "{} / {} 守卫应返回空结果而不是 Err: {:?}",
                    strategy_name,
                    name,
                    got
                );
            }
        }
    }

    /// `AllocateMessageQueueByConfig`：set 4 个队列后，2 个消费者都拿到 `[0,1,2,3]`。
    #[test]
    fn by_config_matches_canonical_allocations() {
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

    /// `AllocateMessageQueueByConfig#allocate` 不做守卫：
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

    /// 未配置 = 空列表，且 `allocate` 给的是副本、改配置不影响已拿到的结果。
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

    /// 策略被 `Arc` 共享后仍可重新配置（引用语义）。
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

    /// 策略名 `get_name()` 的字面值，同时验证 trait 是 object-safe 的。
    #[test]
    fn names_match_protocol_and_trait_is_object_safe() {
        let strategies: Vec<Arc<dyn AllocateMessageQueueStrategy>> = vec![
            Arc::new(AllocateMessageQueueAveragely),
            Arc::new(AllocateMessageQueueAveragelyByCircle),
            Arc::new(AllocateMessageQueueByConfig::default()),
        ];
        let names: Vec<&str> = strategies.iter().map(|s| s.get_name()).collect();
        assert_eq!(names, vec!["AVG", "AVG_BY_CIRCLE", "CONFIG"]);
    }

    /// `Default` 构造口与各策略默认名一致。
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

    /// 差分测试：独立转写一份纯算式实现，与本模块实现在 0..12 × 0..5 规模、
    /// 含探针 cid（组内 / 组外 / 空串）全组合下逐值一致。
    #[test]
    fn matches_golden_reference_arithmetic() {
        // 独立转写的算式（不共享本模块代码），仅用于差分测试。
        fn averagely_reference_allocation(mq_all: &[i32], cid_all: &[String], current_cid: &str) -> Vec<i32> {
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
            let (start_index, end_index) = if modulo > 0 && index < modulo {
                let start = index * (average_size + 1);
                (start, start + average_size + 1)
            } else {
                let start = modulo * (average_size + 1) + (index - modulo) * average_size;
                (start, start + average_size)
            };
            let end = end_index.min(mq_all.len());
            if start_index >= end {
                return vec![];
            }
            mq_all[start_index..end].to_vec()
        }

        fn circle_reference_allocation(mq_all: &[i32], cid_all: &[String], current_cid: &str) -> Vec<i32> {
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
                        averagely_reference_allocation(&ids, &cid_all, cid),
                        "差分算式不一致 AVG {}×{} cid={:?}",
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
                        circle_reference_allocation(&ids, &cid_all, cid),
                        "差分算式不一致 CIRCLE {}×{} cid={:?}",
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

    /// 一致性哈希夹具：消费者 id `CID-i`。
    /// 与 [`create_consumer_id_list`] 的 `CID_PREFIX i` **刻意不同名**——clientId 是要被
    /// 哈希的对象，改一个字符整张表就废了。
    fn ch_cids(size: usize) -> Vec<String> {
        (0..size).map(|i| format!("CID-{i}")).collect()
    }

    /// 一致性哈希夹具：`("topic", "brokerName", 0..N)` 的队列集合。
    /// 这张表里队列的字符串形态（`to_java_string`）同样参与哈希。
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
                    "{} mq={} cid={} index={} 与固定落点表不一致",
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
    /// 期望值是外部权威实现跑出的**固定向量**（MD5Hash + `to_java_string` key +
    /// 虚拟节点逐个铺环的定义不变，这些值就不变）：同组所有成员必须算出同一个环，
    /// 否则混跑时全体队列换主。
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

    /// 同一算法、默认 `virtualNodeCnt = 10`（无参构造的默认值）。
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
    fn consistent_hash_matches_the_canonical_ring_table() {
        let vc3 = AllocateMessageQueueConsistentHash::with_virtual_node_cnt(3).expect("vc>=0");
        run_consistent_hash_table(&vc3, CONSISTENT_HASH_CASES);
        run_consistent_hash_table(&AllocateMessageQueueConsistentHash::new(), CONSISTENT_HASH_DEFAULT_VC_CASES);
        assert_eq!(vc3.get_virtual_node_cnt(), 3);
        assert_eq!(AllocateMessageQueueConsistentHash::default().get_virtual_node_cnt(), 10);
    }

    /// 全覆盖：任意规模下每条队列恰好分给一个消费者（不重不漏）。
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

    /// 构造期必须拒绝负的虚拟节点数（错误串 `illegal virtualNodeCnt :` 已固定）；
    /// `0` 合法 ⇒ 环上没有虚拟节点 ⇒ 一条都分不到。
    #[test]
    fn consistent_hash_rejects_negative_virtual_node_cnt() {
        let err = AllocateMessageQueueConsistentHash::with_virtual_node_cnt(-1)
            .expect_err("必须拒绝");
        assert!(err.to_string().contains("illegal virtualNodeCnt :-1"), "{err}");
        let zero = AllocateMessageQueueConsistentHash::with_virtual_node_cnt(0).expect("0 合法");
        assert!(allocate(&zero, "g", "CID-0", &ch_queues(4), &ch_cids(2)).is_empty());
    }

    /// 自定义 `HashFunction` 必须真的参与建环（允许注入）。
    #[test]
    fn consistent_hash_uses_the_injected_hash_function() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        /// 探针哈希：取 key 的 UTF-16 首码元，兼计数调用次数。
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
        // 同一场景的固定真值：两个 cid 的虚拟节点 key（"CID-0-0"…）首字符都是 'C'
        // ⇒ 哈希相同 ⇒ 同址后者覆盖前者，环上只剩 CID-1 ⇒ 队列全归它。
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

    /// 固定用例：10 队列（0..4 在 room1）+ consumeridcs={room1} +
    /// 2 消费者 → `[0,1,4]` / `[2,3]`（按上述算式复核过的固定向量）。
    ///
    /// 余数队列给**前 rem 个**消费者（`rem > currentIndex`），与 AVG 的切法不同，
    /// 所以第 3 条落 CID_PREFIX0 而不是 CID_PREFIX1。
    #[test]
    fn by_machine_room_matches_canonical_allocations() {
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

    /// broker 名必须是 `机房@名字`（且切分按裁尾空段口径）。
    /// `parts` 列的是 [`split_dropping_trailing_empty`] 的固定段数基准。
    #[test]
    fn by_machine_room_uses_trailing_empty_split_on_the_broker_name() {
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
        // (broker 名, 裁尾后段数, 是否参与分配)
        let cases: &[(&str, usize, bool)] = &[
            ("room1@broker-a", 2, true),
            ("room1@", 1, false),          // 尾空段被裁掉
            ("room1@b@", 2, true),         // 裁尾后仍是 2 段
            ("@room1", 2, false),          // 2 段，但机房是空串、不在白名单
            ("room1@broker@a", 3, false),
            ("@", 0, false),
            ("broker-a", 1, false),
            ("", 1, false),
        ];
        for (broker, canonical_parts, allocated) in cases {
            assert_eq!(
                split_dropping_trailing_empty(broker, '@').len(),
                *canonical_parts,
                "split_dropping_trailing_empty({broker:?}) 段数与固定基准不一致"
            );
            assert_eq!(
                !allocate_one(broker).is_empty(),
                *allocated,
                "broker={broker:?} 的参与/剔除判定与固定基准不一致"
            );
        }
        // 没配机房 = 一条都不分（本模块"守卫返回空"口径）
        assert!(AllocateMessageQueueByMachineRoom::default()
            .allocate(
                "G",
                &cid_all[0],
                &[MessageQueue::new("topic", "room1@b", 0)],
                &cid_all
            )
            .expect("ok")
            .is_empty());
        // 白名单可替换（setter），换完立刻对同一条队列生效
        strategy.set_consumeridcs(["room2"]);
        assert!(strategy.get_consumeridcs().contains("room2"));
        assert!(allocate_one("room1@broker-a").is_empty(), "换白名单后 room1 不再参与");
    }

    /// 机房策略同样吃 `check_config` 守卫。
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

    /// 测试 resolver：broker `IDCx-brokerName` / 消费者 `IDCx-CID-i` 取 '-' 前段。
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

    /// 就近测试夹具：idc_size 个机房 × 每机房 queue_size 条队列。
    fn nearby_mq(idc_size: usize, queue_size: i32) -> Vec<MessageQueue> {
        let mut out = Vec::new();
        for i in 1..=idc_size {
            for q in 0..queue_size {
                out.push(MessageQueue::new("topic", &format!("IDC{i}-brokerName"), q));
            }
        }
        out
    }

    /// 就近测试夹具：idc_size 个机房 × 每机房 consumer_size 个消费者。
    fn nearby_cids(idc_size: usize, consumer_size: usize) -> Vec<String> {
        let mut out = Vec::new();
        for i in 1..=idc_size {
            for q in 0..consumer_size {
                out.push(format!("IDC{i}-CID-{q}"));
            }
        }
        out
    }

    /// 机房数相等时，每个消费者只拿到**同机房**的队列，
    /// 且全员并集恰好是全集（不重不漏）。四组规模：5 机房 × 20 队列 × 10/20/30/1 消费者。
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

    /// broker 机房多于消费者机房时，**没有活消费者**的
    /// 机房要交给全部消费者共享（否则没人消费），有消费者的机房仍然只给自己的消费者。
    #[test]
    fn nearby_shares_rooms_that_have_no_consumer() {
        let strategy = AllocateMachineRoomNearby::new(
            Arc::new(AllocateMessageQueueAveragely),
            Arc::new(DashRoom),
        );
        // 固定场景：mqs = IDC2×4 + IDC1×2，cids = IDC1 的两个消费者
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

    /// `getName()` = `MACHINE_ROOM_NEARBY-<内层策略名>`（固定值：
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

    /// resolver 给出空机房 ⇒ 返回 `Err` 而不是空结果：
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

    /// 策略名 `get_name()` 的字面值（六个策略齐全）。
    #[test]
    fn all_six_canonical_strategy_names_match() {
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
