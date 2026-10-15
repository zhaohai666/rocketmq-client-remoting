//! Queue allocation strategies (during rebalance, distribute a topic's queues to each consumer in the same consumer group).
//!
//! Every strategy implements `allocate` (compute this consumer's slice of queues) and `get_name` (the strategy name).
//!
//! ## Strategy set
//!
//! All six strategies are provided: `AVG` / `AVG_BY_CIRCLE` / `CONFIG` /
//! `CONSISTENT_HASH` (needs the consistent hash ring from [`crate::common::consistent_hash`]) /
//! `MACHINE_ROOM` / `MACHINE_ROOM_NEARBY-<inner>` (proxy pattern, see
//! [`AllocateMachineRoomNearby`]).
//!
//! ## Behaviour contract
//!
//! 1. **Guards never throw**: empty `current_cid` / `mq_all` / `cid_all`, or `current_cid` not in
//!    `cid_all`, all return an empty result, **never** `Err`: rebalance is a background periodic
//!    task and one dirty input must not take the consumer down. The single exception is
//!    [`AllocateMachineRoomNearby`]: an empty machine room from the resolver **still throws**
//!    (`Err`), because silently returning empty revokes every queue of the topic, while rebalance
//!    catching the error keeps the existing allocation (no recompute, no fallback of taking all).
//! 2. **Unconfigured state of `AllocateMessageQueueByConfig`**: unconfigured means an empty
//!    list, and `allocate` returns a **copy**, not a shared mutable reference. A missing constructor
//!    argument (Rust excludes null values on the type via `Arc<dyn ...>`) has no failure branch.
//! 3. **The `Result` of `allocate`**: since guards no longer report errors, the strategies here return `Err`
//!    only when the configuration itself is illegal (constructor-time `virtual_node_cnt < 0`, the resolver
//!    yielding an empty machine room, the inner strategy erroring); `Result` is also the error channel for
//!    custom strategies (business code implementing [`AllocateMessageQueueStrategy`] itself) -- on their error rebalance logs and keeps the existing allocation.
//! 4. **Decision order of `check_config`**: `current_cid` first, then `mq_all`, then `cid_all` last;
//!    the three guards only affect the result, never each other; when `current_cid` is not in
//!    `cid_all`, emit one `[BUG] ... not in cidAll` info log line (logging does not change behaviour).
//! 5. **`@` split of broker names**: the convention is `<machineRoom>@<name>`; splitting **drops
//!    trailing empty segments** (`"room1@"` -> 1 part, `"room1@b@"` -> 2 parts), unlike the default
//!    behaviour of `str::split`. This module replicates that contract with
//!    [`split_dropping_trailing_empty`] so the decision of whether a queue takes part in allocation stays stable.

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::{Arc, RwLock};

use crate::common::consistent_hash::{ClientNode, ConsistentHashRouter, HashFunction, Node};
use crate::common::message::MessageQueue;
use crate::error::{Error, Result};
use crate::rmq_info;

/// Queue allocation strategy interface: `allocate` computes this consumer's slice of queues, `get_name` gives the strategy name.
///
/// Deliberately kept **object-safe** (both methods take only `&self`, no generic parameters, no `Self` returns),
/// so callers can share it across rebalance threads via `Arc<dyn AllocateMessageQueueStrategy>`.
pub trait AllocateMessageQueueStrategy: Send + Sync {
    /// Distribute `consumer_group`'s queues to `current_cid` according to this strategy.
    ///
    /// - `consumer_group`: the current consumer group; only appears in the "not in `cid_all`" log line;
    /// - `current_cid`: this client's id; its index in `cid_all` decides which slice to take;
    /// - `mq_all`: all queues of the topic. **The caller must sort**: rebalance sorts both
    ///   `cid_all` and `mq_all` by (brokerName, queueId), otherwise the boundaries are unstable;
    /// - `cid_all`: all client ids of the consumer group, also required sorted (decides each client's slice).
    ///
    /// The strategy itself neither sorts nor dedups; it only guarantees "the output is an ordered subsequence of the input", so the result is deterministic and reproducible.
    fn allocate(
        &self,
        consumer_group: &str,
        current_cid: &str,
        mq_all: &[MessageQueue],
        cid_all: &[String],
    ) -> Result<Vec<MessageQueue>>;

    /// Algorithm name (`AVG` / `AVG_BY_CIRCLE` / `CONFIG` / `CONSISTENT_HASH` /
    /// `MACHINE_ROOM` / `MACHINE_ROOM_NEARBY-<inner>`), used for logging and strategy identification;
    /// the literal values are fixed -- changing them would break matching by operational scripts.
    fn get_name(&self) -> &str;
}

/// Common guard before allocation: returns `None` when `current_cid` is an empty string,
/// `mq_all` is empty, `cid_all` is empty, or `current_cid` is not in `cid_all`, leaving the
/// caller to produce an empty result; when it passes, returns `current_cid`'s index in `cid_all`.
///
/// Emptiness only checks `is_empty`: a whitespace-only string is a legal input.
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
    // [BUG] log: this consumer is not in the group list, which usually means a lagging cidAll view; log only, behaviour unchanged.
    rmq_info!(
        "[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {:?}",
        consumer_group,
        current_cid,
        cid_all
    );
    None
}

// --------------------------------------------------------- Averagely (AVG)

/// Average allocation: cut `mq_all` into **contiguous ranges**; the remainder `mod` is handed out in order to the first `mod` consumers,
/// i.e. those take `average_size + 1` queues each and the rest take `average_size`. Strategy name `"AVG"`.
///
/// The formula is fixed (10 queues / 4 consumers -> `{3,3,2,2}`); the closed form and the slicing form are **value-for-value equivalent**:
/// - closed form: `averageSize = mq<=cid ? 1 : (mod>0 && index<mod ? mq/cid+1 : mq/cid)`,
///   `startIndex = (mod>0 && index<mod) ? index*averageSize : index*averageSize+mod`,
///   `range = min(averageSize, mq - startIndex)`;
/// - this implementation (slicing form): `start/end` two branches + `mq_all[start:end]`.
///   `index*avg + mod` and `mod*(avg+1) + (index-mod)*avg` both expand to `index*avg + mod`;
///   the `min` trim is equivalent to the `average_size == 0` early return
///   (when queues <= consumers only clients with `index < mod` get 1 queue, the rest get none).
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
        let modulo = mq_size % cid_size; // remainder mod
        let average_size = mq_size / cid_size; // baseline queues per consumer

        // With fewer queues than consumers, only clients with index < modulo get 1 queue;
        // the `average_size == 0` early return implements the closed form's `averageSize = 1` + `min` trim.
        if average_size == 0 {
            return Ok(if index < mq_size {
                vec![mq_all[index].clone()]
            } else {
                Vec::new()
            });
        }

        // start/end two branches: the first `modulo` consumers each take one extra queue
        let (start_index, end_index) = if modulo > 0 && index < modulo {
            let start = index * (average_size + 1);
            (start, start + average_size + 1)
        } else {
            // Here index >= modulo (or modulo == 0), so the subtraction cannot underflow.
            let start = modulo * (average_size + 1) + (index - modulo) * average_size;
            (start, start + average_size)
        };
        // The closed form's min(averageSize, mq - startIndex): out-of-range becomes empty or is truncated.
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

// ------------------------------------------------- AveragelyByCircle (AVG_BY_CIRCLE)

/// Circular average allocation: consumer number `index` takes the queues whose index satisfies
/// `≡ index (mod cid_size)`, i.e. `mq_all[index], mq_all[index + cid_size], ...`. Name `"AVG_BY_CIRCLE"`.
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
        // Arithmetic take-up "start at own index, stride = consumer count" equals round-robin hitting by index.
        // `check_config` guarantees `cid_all` is non-empty, so the stride is always >= 1 and the `step_by(0)` panic cannot happen.
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

// ------------------------------------------------------------- ByConfig (CONFIG)

/// Allocation by explicit configuration: ignores `mq_all` / `cid_all` entirely and hands the
/// configured queue list verbatim to **every** consumer (typically manual broadcast binding); name `"CONFIG"`.
///
/// `allocate` **does not call the guard**, so an empty group / empty `cid_all` still returns the configured list.
#[derive(Debug, Default)]
pub struct AllocateMessageQueueByConfig {
    message_queue_list: RwLock<Vec<MessageQueue>>,
}

impl AllocateMessageQueueByConfig {
    /// Construct with the given queue list (unconfigured means an empty list).
    pub fn new(message_queue_list: Vec<MessageQueue>) -> AllocateMessageQueueByConfig {
        AllocateMessageQueueByConfig {
            message_queue_list: RwLock::new(message_queue_list),
        }
    }

    /// Replace the queue list at runtime.
    ///
    /// Takes `&self` (not `&mut self`): the strategy object is shared via `Arc` and registered with rebalance;
    /// replacing the list afterwards still affects all later allocations. `&mut self` would demand the caller
    /// to own the object exclusively, contradicting that usage (same contract as [`crate::client::latency::MQFaultStrategy::set_send_latency_fault_enable`]).
    pub fn set_message_queue_list(&self, message_queue_list: Vec<MessageQueue>) {
        *self
            .message_queue_list
            .write()
            .unwrap_or_else(|e| e.into_inner()) = message_queue_list;
    }

    /// Read the currently configured queue list. Returns a copy (no shared mutable reference is handed out).
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
        // Unconfigured returns an empty list; each call returns a fresh copy so caller edits cannot affect the strategy.
        Ok(self.get_message_queue_list())
    }

    fn get_name(&self) -> &str {
        "CONFIG"
    }
}

// --------------------------------------------------------- ConsistentHash (CONSISTENT_HASH)

/// Split `text` on `sep` and **drop trailing empty segments**.
///
/// The only difference from `str::split` is the trailing empty segment: `"room1@"` yields `["room1"]` (1 part) here,
/// while `str::split` yields `["room1", ""]` (2 parts). But **when the separator is not found** the whole string is
/// returned verbatim (even if it is itself empty), so the tail cannot be trimmed unconditionally.
/// Fixed samples (this implementation's decision baseline): `""`->1 part, `"@"`->0 parts, `"@@\n"`->3 parts, `"room1@b@"`->2 parts.
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

/// Consistent-hash allocation: hash each clientId into `virtual_node_cnt` virtual nodes placed on the ring,
/// then route every queue (keyed by its string form from [`MessageQueue::to_java_string`]) to the
/// consumer owning the first node found clockwise. Strategy name `"CONSISTENT_HASH"`.
///
/// What differs from AVG / AVG_BY_CIRCLE is not "evenness" but **stability**: when the queue or consumer count changes, only queues on the
/// arcs adjacent to the added/removed node change owners, while AVG moves everyone's boundaries wholesale.
///
/// ⚠ The hash key must be [`MessageQueue::to_java_string`]: any other form is a different ring, and mixed key
/// forms within one group show up as every queue changing owner (duplicate/skipped consumption), not "slightly uneven".
pub struct AllocateMessageQueueConsistentHash {
    virtual_node_cnt: i32,
    custom_hash_function: Option<Arc<dyn HashFunction>>,
}

/// Hand-written `Debug`: `Arc<dyn HashFunction>` has no `Debug`, so deriving would fail on it.
impl fmt::Debug for AllocateMessageQueueConsistentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AllocateMessageQueueConsistentHash")
            .field("virtual_node_cnt", &self.virtual_node_cnt)
            .field("custom_hash_function", &self.custom_hash_function.is_some())
            .finish()
    }
}

impl Default for AllocateMessageQueueConsistentHash {
    /// Default: 10 virtual nodes with the default MD5Hash (equivalent to `with_virtual_node_cnt(10)`).
    fn default() -> AllocateMessageQueueConsistentHash {
        AllocateMessageQueueConsistentHash {
            virtual_node_cnt: 10,
            custom_hash_function: None,
        }
    }
}

impl AllocateMessageQueueConsistentHash {
    /// Construct with default parameters: 10 virtual nodes, default MD5Hash.
    pub fn new() -> AllocateMessageQueueConsistentHash {
        Default::default()
    }

    /// Construct with a given virtual node count.
    pub fn with_virtual_node_cnt(virtual_node_cnt: i32) -> Result<AllocateMessageQueueConsistentHash> {
        Self::with_hash_function(virtual_node_cnt, None)
    }

    /// Construct with a given virtual node count and hash function.
    ///
    /// `virtual_node_cnt < 0` is illegal and rejected at construction (`Err`): this is not the rebalance
    /// background path, so contract 1's "guards return empty" in the module header does not apply here.
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

    /// Virtual node count (getter semantics only; changing it requires rebuilding the strategy).
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
        // Wrap each entry of cid_all into a ClientNode to build the ring; with no injected hash function, use default MD5Hash.
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
            // Route on the MessageQueue string form as key, then compare the node's key against current_cid.
            // route_node returns None on an empty ring -- here cid_all is non-empty but the virtual node count may be 0,
            // so the `None` branch really is reachable (vc=0 ⇒ empty ring ⇒ nobody gets any queue).
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

// ------------------------------------------------------- ByMachineRoom (MACHINE_ROOM)

/// Machine-room allocation: broker names follow the convention `<machineRoom>@<brokerName>`; only queues whose prefix is in the
/// `consumeridcs` whitelist take part, then one "average" cut happens inside that filtered set. Strategy name `"MACHINE_ROOM"`.
///
/// The partition formula **looks like but differs from** [`AllocateMessageQueueAveragely`]: here
/// `mod = premqAll.size() / cidAll.size()` and `rem = premqAll.size() % cidAll.size()`, and the
/// remainder queues go to the **first rem** consumers via `rem > currentIndex` (AVG instead gives the
/// first mod consumers one extra each when `index < mod`, shifting the ranges too), so 10 queues /
/// 5 in room1 / 2 consumers yields `[0,1,4]` + `[2,3]`, not AVG's `[0,1,2]` + `[3,4]`.
///
/// ⚠ Broker names are split with [`split_dropping_trailing_empty`]; the test is "exactly 2 parts after dropping the empty tail".
/// ⚠ `consumeridcs` defaults to the empty set, so with nothing configured "not one queue is allocated" (module-header contract 1).
#[derive(Debug, Default)]
pub struct AllocateMessageQueueByMachineRoom {
    consumeridcs: RwLock<HashSet<String>>,
}

impl AllocateMessageQueueByMachineRoom {
    /// Construct from the machine-room whitelist.
    pub fn new<I, S>(consumeridcs: I) -> AllocateMessageQueueByMachineRoom
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        AllocateMessageQueueByMachineRoom {
            consumeridcs: RwLock::new(consumeridcs.into_iter().map(Into::into).collect()),
        }
    }

    /// Replace the machine-room whitelist at runtime; the reason for taking `&self` is the same as
    /// [`AllocateMessageQueueByConfig::set_message_queue_list`].
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

    /// Read the machine-room whitelist: returns a copy.
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
                // A broker name joins allocation only if, after dropping the empty tail, it has exactly 2 parts and the first part is in the whitelist
                let temp = split_dropping_trailing_empty(mq.get_broker_name(), '@');
                temp.len() == 2 && rooms.contains(temp[0])
            })
            .cloned()
            .collect();

        let cid_size = cid_all.len(); // check_config already guarantees >= 1
        let modulo = premq_all.len() / cid_size;
        let rem = premq_all.len() % cid_size;
        let start_index = modulo * current_index;
        let end_index = start_index + modulo;
        let mut result: Vec<MessageQueue> = premq_all[start_index..end_index].to_vec();
        if rem > current_index {
            // Out-of-range is impossible: `current_index < rem` and `rem < cid_size`
            // ⇒ `current_index + modulo * cid_size < rem + modulo * cid_size == premq_all.len()`.
            result.push(premq_all[current_index + modulo * cid_size].clone());
        }
        Ok(result)
    }

    fn get_name(&self) -> &str {
        "MACHINE_ROOM"
    }
}

// ------------------------------------------------------------- Machine room nearby (MACHINE_ROOM_NEARBY)

/// Machine room resolver callback: tells the strategy "who is in which machine room".
///
/// Neither method **may return an empty string** (empty is an error); otherwise that queue/consumer is judged as "machine room unknown".
pub trait MachineRoomResolver: Send + Sync {
    /// Give the machine room where the queue is deployed (resolved from the broker name).
    fn broker_deploy_in(&self, message_queue: &MessageQueue) -> String;

    /// Give the machine room where the consumer is deployed (resolved from the clientID).
    fn consumer_deploy_in(&self, client_id: &str) -> String;
}

/// Machine-room-nearby proxy: wraps the real allocation algorithm one layer in. The strategy name is
/// `"MACHINE_ROOM_NEARBY" + "-" + inner strategy.get_name()`.
///
/// 1. Queues in the **same machine room** as the current consumer go only to that room's consumers (via the inner strategy);
/// 2. Queues whose machine room has **no live consumer at all** are shared by all consumers via the inner
///    strategy -- otherwise nobody would consume them.
///
/// Machine rooms are walked in **lexicographic order** (`BTreeMap`), keeping the processing order stable and the result reproducible.
///
/// Resolver yielding an empty machine room: return `Err`, not an empty result (the exception to module-header
/// contract 1; silently returning empty equals revoking the whole topic's queues, and keeping the existing allocation matters more).
/// Missing constructor arguments are already excluded by the `Arc` types (module-header contract 2).
pub struct AllocateMachineRoomNearby {
    allocate_message_queue_strategy: Arc<dyn AllocateMessageQueueStrategy>,
    machine_room_resolver: Arc<dyn MachineRoomResolver>,
    /// The strategy name is composed by concatenation; `get_name(&self) -> &str` needs somewhere to store it,
    /// so it is computed at construction time.
    name: String,
}

impl AllocateMachineRoomNearby {
    /// Construct from the inner strategy and the machine room resolver callback.
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

        // Group by machine room (one lexicographically-ordered table each for queues and for consumers).
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

        // 1. Queues in the current consumer's machine room: split only among same-room consumers.
        //    Remove that room from mr_2_mq first, so it cannot reappear in step 2's shared pool.
        let current_machine_room = self.machine_room_resolver.consumer_deploy_in(current_cid);
        let mq_in_this_machine_room = mr_2_mq.remove(&current_machine_room);
        // With zero same-room cids, treat as an empty list ⇒ this room's queues fall back to step 2's shared pool.
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

        // 2. Rooms with no live consumer: queues cannot go unconsumed, so hand them to all consumers.
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

    /// Test fixture: queues `("topic", "brokerName", 0..size)`.
    fn create_message_queue_list(size: i32) -> Vec<MessageQueue> {
        (0..size)
            .map(|i| MessageQueue::new("topic", "brokerName", i))
            .collect()
    }

    /// Test fixture: consumer ids `"CID_PREFIX" + i`.
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
            .expect("built-in strategies never return Err");
        queue_ids(&got)
    }

    /// One golden-comparison case: queue count / consumer count / per-consumer expected queue ids (in `CID_PREFIX0..N` order).
    #[derive(Debug)]
    struct Case {
        mq_size: i32,
        cid_size: usize,
        expect: &'static [&'static [i32]],
    }

    const AVERAGELY_CASES: &[Case] = &[
        // Fixed case: 10 queues / 4 consumers -> sizes {3,3,2,2}
        Case {
            mq_size: 10,
            cid_size: 4,
            expect: &[&[0, 1, 2], &[3, 4, 5], &[6, 7], &[8, 9]],
        },
        // Another non-divisible case: 8 / 3 -> mod=2, the first two take 3 each
        Case {
            mq_size: 8,
            cid_size: 3,
            expect: &[&[0, 1, 2], &[3, 4, 5], &[6, 7]],
        },
        // Exactly divisible
        Case {
            mq_size: 9,
            cid_size: 3,
            expect: &[&[0, 1, 2], &[3, 4, 5], &[6, 7, 8]],
        },
        // Queue count == consumer count
        Case {
            mq_size: 4,
            cid_size: 4,
            expect: &[&[0], &[1], &[2], &[3]],
        },
        // Fewer queues than consumers: only the first 2 take 1 each, the rest empty (average_size == 0 early-return branch)
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
        // Single consumer takes everything
        Case {
            mq_size: 3,
            cid_size: 1,
            expect: &[&[0, 1, 2]],
        },
        // Empty mq_all (guard)
        Case {
            mq_size: 0,
            cid_size: 2,
            expect: &[&[], &[]],
        },
    ];

    const CIRCLE_CASES: &[Case] = &[
        // Fixed case: 10 / 4 -> {0,4,8} {1,5,9} {2,6} {3,7}
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
                "case cid count disagrees with expected count: {:?}",
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

    /// Fixed size 10 queues / 4 consumers: only assert each consumer's got size = {3,3,2,2}.
    #[test]
    fn averagely_matches_canonical_allocation_sizes() {
        let cid_all = create_consumer_id_list(4);
        let mq_all = create_message_queue_list(10);
        let sizes: Vec<usize> = cid_all
            .iter()
            .map(|cid| {
                AllocateMessageQueueAveragely
                    .allocate("", cid, &mq_all, &cid_all)
                    .expect("built-in strategies never return Err")
                    .len()
            })
            .collect();
        assert_eq!(sizes, vec![3, 3, 2, 2]);
    }

    /// `currentCID` not in `cidAll` -> size 0 (other guard scenarios are in [`guards_return_empty_result`]).
    #[test]
    fn circle_matches_canonical_not_in_cid_all() {
        let cid_all = create_consumer_id_list(4);
        let mq_all = create_message_queue_list(10);
        let got =
            AllocateMessageQueueAveragelyByCircle.allocate("", "CID_PREFIX", &mq_all, &cid_all);
        assert_eq!(got.expect("built-in strategies never return Err").len(), 0);
    }

    /// Guards: illegal inputs return an empty result instead of an error.
    #[test]
    fn guards_return_empty_result() {
        // (case name, current_cid, mq_all count, cid_all count) -- each row triggers exactly one guard
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
                    "{} / {} guard should return an empty result, not Err: {:?}",
                    strategy_name,
                    name,
                    got
                );
            }
        }
    }

    /// `AllocateMessageQueueByConfig`: after setting 4 queues, both of 2 consumers get `[0,1,2,3]`.
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

    /// `AllocateMessageQueueByConfig#allocate` skips the guards:
    /// guard scenarios (empty group / empty cid_all / current_cid not in the list) still return the configured value.
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

    /// Unconfigured = empty list; `allocate` hands out a copy, so later config changes do not affect already-taken results.
    #[test]
    fn by_config_defaults_empty_and_returns_copy() {
        let strategy = AllocateMessageQueueByConfig::default();
        assert!(strategy.get_message_queue_list().is_empty());
        let before = strategy
            .allocate("", "CID0", &[], &[])
            .expect("built-in strategies never return Err");
        strategy.set_message_queue_list(create_message_queue_list(3));
        assert!(before.is_empty(), "the returned value must be a copy");
        assert_eq!(queue_ids(&before), Vec::<i32>::new());
        assert_eq!(
            queue_ids(
                &strategy
                    .allocate("", "CID0", &[], &[])
                    .expect("built-in strategies never return Err")
            ),
            vec![0, 1, 2]
        );
    }

    /// The strategy stays reconfigurable through a shared `Arc` (reference semantics).
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

    /// Literal values of strategy names `get_name()`; also proves the trait is object-safe.
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

    /// The `Default` constructors agree with each strategy's default name.
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

    /// Full coverage + no overlap: for any (queue count, consumer count) combination, the union over all consumers is exactly `mq_all`.
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
                        "{} allocation overlap",
                        strategy.get_name()
                    );
                }
            }
        }
    }

    /// Stability: repeated allocation on the same input gives bit-for-bit identical results, and the output preserves the input's relative order.
    #[test]
    fn allocation_is_deterministic_and_order_preserving() {
        // Deliberately shuffled (descending + mixed broker names); the strategy slices by index, not by sorting by value.
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
                assert_eq!(once, twice, "{} result is unstable", strategy.get_name());
                // The output must be an ordered subsequence of the input (relative order unchanged)
                let mut rest = mq_all.as_slice();
                for mq in &once {
                    let pos = rest
                        .iter()
                        .position(|x| x == mq)
                        .expect("output must be an element of the input");
                    rest = &rest[pos + 1..];
                }
                let ids = queue_ids(&once);
                let positions: Vec<usize> = ids
                    .iter()
                    .map(|id| {
                        mq_all
                            .iter()
                            .position(|mq| mq.get_queue_id() == *id)
                            .expect("must exist")
                    })
                    .collect();
                assert!(
                    positions.windows(2).all(|w| w[0] < w[1]),
                    "output order is shuffled: {:?}",
                    ids
                );
            }
        }
    }

    /// Differential test: an independently transcribed pure-arithmetic implementation must match this
    /// module's implementation value-for-value across 0..12 × 0..5 sizes and every probe cid (in-group / out-of-group / empty string).
    #[test]
    fn matches_golden_reference_arithmetic() {
        // Independently transcribed arithmetic (shares no code with this module), used only for the differential test.
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
                        "differential arithmetic mismatch AVG {}×{} cid={:?}",
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
                        "differential arithmetic mismatch CIRCLE {}×{} cid={:?}",
                        mq_size,
                        cid_size,
                        cid
                    );
                }
            }
        }
    }

    /// Real queues (one topic mounted on multiple brokerNames) are also always sliced by index, regardless of broker name / queueId.
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

    // ------------------------------------------------ Consistent hash (CONSISTENT_HASH)

    /// Consistent-hash fixture: consumer ids `CID-i`.
    /// Deliberately named differently from [`create_consumer_id_list`]'s `CID_PREFIX i` -- the clientId is
    /// what gets hashed; changing one character invalidates the whole table.
    fn ch_cids(size: usize) -> Vec<String> {
        (0..size).map(|i| format!("CID-{i}")).collect()
    }

    /// Consistent-hash fixture: the queue set `("topic", "brokerName", 0..N)`.
    /// The string form of the queues in this table (`to_java_string`) also takes part in the hash.
    fn ch_queues(size: i32) -> Vec<MessageQueue> {
        (0..size)
            .map(|i| MessageQueue::new("topic", "brokerName", i))
            .collect()
    }

    fn run_consistent_hash_table(strategy: &dyn AllocateMessageQueueStrategy, cases: &[Case]) {
        for case in cases {
            let mq_all = ch_queues(case.mq_size);
            let cid_all = ch_cids(case.cid_size);
            assert_eq!(case.expect.len(), case.cid_size, "case cid count mismatch: {case:?}");
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
                    "{} mq={} cid={} index={} disagrees with the fixed landing table",
                    strategy.get_name(),
                    case.mq_size,
                    case.cid_size,
                    index
                );
            }
        }
    }

    /// Landing table (`virtualNodeCnt = 3`).
    ///
    /// The expected values are the **fixed vector** of the canonical algorithm definition (MD5Hash + `to_java_string` key +
    /// virtual nodes laid on the ring one by one; the definition is unchanged, so these values are unchanged): all members of one group must compute the same ring,
    /// otherwise every queue switches owner in a mixed run.
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

    /// Same algorithm, default `virtualNodeCnt = 10` (the no-argument constructor's default).
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

    /// Full coverage: at any size, every queue is assigned to exactly one consumer (no duplicates, no omissions).
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

    /// The whole point of consistent hashing: a membership change only moves the arc involved; other consumers' queues do not change owner.
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
            map.into_iter().map(|o| o.expect("every queue has an owner")).collect()
        };
        let before = owner_of(&cid_all);

        // Remove CID-0: its former queues get taken over by others (that must happen),
        // but queues **already held by others** must stay with those holders.
        let remaining: Vec<String> = cid_all[1..].to_vec();
        let after_remove = owner_of(&remaining);
        for (qid, owner) in before.iter().enumerate() {
            if owner != "CID-0" {
                assert_eq!(&after_remove[qid], owner, "qid={qid} must not change owner on removal");
            }
        }

        // Add a new consumer: likewise, only the queues assigned to CID-NEW are new.
        let mut joined = remaining.clone();
        joined.push("CID-NEW".to_string());
        let after_add = owner_of(&joined);
        for (qid, owner) in before.iter().enumerate() {
            if after_add[qid] != "CID-NEW" {
                assert_eq!(&after_add[qid], owner, "qid={qid} must not change owner on join");
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
        strategy.allocate(group, cid, mq_all, cid_all).expect("built-in strategies never return Err")
    }

    /// Construction must reject a negative virtual node count (the error string `illegal virtualNodeCnt :` is fixed);
    /// `0` is legal ⇒ no virtual nodes on the ring ⇒ not a single queue is allocated.
    #[test]
    fn consistent_hash_rejects_negative_virtual_node_cnt() {
        let err = AllocateMessageQueueConsistentHash::with_virtual_node_cnt(-1)
            .expect_err("must be rejected");
        assert!(err.to_string().contains("illegal virtualNodeCnt :-1"), "{err}");
        let zero = AllocateMessageQueueConsistentHash::with_virtual_node_cnt(0).expect("0 is legal");
        assert!(allocate(&zero, "g", "CID-0", &ch_queues(4), &ch_cids(2)).is_empty());
    }

    /// A custom `HashFunction` must really take part in building the ring (injection is allowed).
    #[test]
    fn consistent_hash_uses_the_injected_hash_function() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        /// Probe hash: takes the first UTF-16 code unit of the key and also counts invocations.
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
        // Fixed truth for this scenario: both cids' virtual node keys ("CID-0-0"...) start with 'C'
        // ⇒ same hash ⇒ the later one overwrites the earlier one at the same address; only CID-1 remains on the ring ⇒ all queues go to it.
        assert_eq!(allocate_ids(&strategy, "g", "CID-0", &mq_all, &cid_all), Vec::<i32>::new());
        assert_eq!(allocate_ids(&strategy, "g", "CID-1", &mq_all, &cid_all), vec![0, 1, 2, 3]);
        assert!(counter.0.load(Ordering::Relaxed) > 0, "custom hash was never called");
    }

    /// Guard semantics are the same as the other strategies.
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

    // ----------------------------------------------------- Machine-room strategy (MACHINE_ROOM)

    /// Fixed case: 10 queues (0..4 in room1) + consumeridcs={room1} +
    /// 2 consumers → `[0,1,4]` / `[2,3]` (a fixed vector re-checked against the formula above).
    ///
    /// The remainder queues go to the **first rem** consumers (`rem > currentIndex`), unlike AVG's cut,
    /// so the 3rd queue lands on CID_PREFIX0 instead of CID_PREFIX1.
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

    /// A broker name must be `room@name` (and splitting follows the trailing-empty-dropping convention).
    /// `parts` lists the fixed segment-count baseline of [`split_dropping_trailing_empty`].
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
        // (broker name, segment count after dropping the tail, whether it joins allocation)
        let cases: &[(&str, usize, bool)] = &[
            ("room1@broker-a", 2, true),
            ("room1@", 1, false),          // trailing empty segment dropped
            ("room1@b@", 2, true),         // still 2 segments after dropping the tail
            ("@room1", 2, false),          // 2 segments, but the room is an empty string, not in the whitelist
            ("room1@broker@a", 3, false),
            ("@", 0, false),
            ("broker-a", 1, false),
            ("", 1, false),
        ];
        for (broker, canonical_parts, allocated) in cases {
            assert_eq!(
                split_dropping_trailing_empty(broker, '@').len(),
                *canonical_parts,
                "split_dropping_trailing_empty({broker:?}) segment count disagrees with the fixed baseline"
            );
            assert_eq!(
                !allocate_one(broker).is_empty(),
                *allocated,
                "broker={broker:?} include/exclude decision disagrees with the fixed baseline"
            );
        }
        // Nothing configured = not a single queue is allocated (this module's "guards return empty" convention)
        assert!(AllocateMessageQueueByMachineRoom::default()
            .allocate(
                "G",
                &cid_all[0],
                &[MessageQueue::new("topic", "room1@b", 0)],
                &cid_all
            )
            .expect("ok")
            .is_empty());
        // The whitelist is replaceable (setter); the change takes effect on the same queue immediately
        strategy.set_consumeridcs(["room2"]);
        assert!(strategy.get_consumeridcs().contains("room2"));
        assert!(allocate_one("room1@broker-a").is_empty(), "room1 no longer joins after the whitelist is replaced");
    }

    /// The machine-room strategy goes through the `check_config` guard as well.
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

    // --------------------------------------------- Machine-room-nearby proxy (MACHINE_ROOM_NEARBY)

    /// Test resolver: broker `IDCx-brokerName` / consumer `IDCx-CID-i` takes the '-' prefix segment.
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

    /// Nearby test fixture: idc_size machine rooms × queue_size queues per room.
    fn nearby_mq(idc_size: usize, queue_size: i32) -> Vec<MessageQueue> {
        let mut out = Vec::new();
        for i in 1..=idc_size {
            for q in 0..queue_size {
                out.push(MessageQueue::new("topic", &format!("IDC{i}-brokerName"), q));
            }
        }
        out
    }

    /// Nearby test fixture: idc_size machine rooms × consumer_size consumers per room.
    fn nearby_cids(idc_size: usize, consumer_size: usize) -> Vec<String> {
        let mut out = Vec::new();
        for i in 1..=idc_size {
            for q in 0..consumer_size {
                out.push(format!("IDC{i}-CID-{q}"));
            }
        }
        out
    }

    /// With equal room counts, every consumer only gets queues from **its own room**,
    /// and the union over all consumers is exactly the full set (no duplicates, no omissions). Four sizes: 5 rooms × 20 queues × 10/20/30/1 consumers.
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
                        "{} got a queue from another room {:?}",
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
            assert_eq!(flat.len(), expected.len(), "{idc_size}×{queue_size}×{consumer_size} missing or duplicated");
            flat.sort();
            expected.sort();
            assert_eq!(flat, expected, "{idc_size}×{queue_size}×{consumer_size} union does not equal the full set");
        }
    }

    /// When broker rooms outnumber consumer rooms, rooms **without any live consumer** are
    /// shared by all consumers (otherwise nobody consumes them); rooms with consumers still go only to their own consumers.
    #[test]
    fn nearby_shares_rooms_that_have_no_consumer() {
        let strategy = AllocateMachineRoomNearby::new(
            Arc::new(AllocateMessageQueueAveragely),
            Arc::new(DashRoom),
        );
        // Fixed scenario: mqs = IDC2×4 + IDC1×2, cids = two consumers of IDC1
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
            "IDC1's own 2 + IDC2's 4 (empty room) split half by AVG"
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
            "same-room queues must be collected first, then topped up with the empty room's shared queues"
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

        // 5 rooms, only the first 2 have consumers: every queue must get a consumer; healthy rooms do not leak queues.
        let mq_all = nearby_mq(5, 4);
        let cid_all = nearby_cids(2, 3);
        let mut claimed = 0;
        for cid in &cid_all {
            for mq in allocate(&strategy, "Test-C-G", cid, &mq_all, &cid_all) {
                let room = DashRoom.broker_deploy_in(&mq);
                if room == "IDC1" || room == "IDC2" {
                    assert_eq!(room, DashRoom.consumer_deploy_in(cid), "{room}'s queues leaked out");
                }
                claimed += 1;
            }
        }
        assert_eq!(claimed, mq_all.len(), "no duplicates or omissions in the empty-room scenario");
    }

    /// `get_name()` = `MACHINE_ROOM_NEARBY-<inner strategy name>` (fixed values:
    /// `MACHINE_ROOM_NEARBY-AVG` / `MACHINE_ROOM_NEARBY-AVG_BY_CIRCLE`).
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
        // The same composition applies when the inner strategy is CONFIG
        let by_config = AllocateMachineRoomNearby::new(
            Arc::new(AllocateMessageQueueByConfig::new(create_message_queue_list(1))),
            Arc::new(DashRoom),
        );
        assert_eq!(by_config.get_name(), "MACHINE_ROOM_NEARBY-CONFIG");
    }

    /// A resolver yielding an empty room ⇒ return `Err` instead of an empty result:
    /// silently returning empty equals revoking the whole topic's queues, while rebalance catching the error keeps the existing allocation.
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
            .expect_err("an empty room must raise an error");
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
            .expect_err("an empty room must raise an error");
        assert!(
            err.to_string().contains("Machine room is null for consumer id CID-0"),
            "{err}"
        );
        // The guard still takes priority: when current_cid is not in cid_all, return empty first without touching the resolver
        let guard = AllocateMachineRoomNearby::new(
            Arc::new(AllocateMessageQueueAveragely),
            Arc::new(BlankBrokerRoom),
        );
        assert!(guard.allocate("G", "CID-0", &mq_all, &[]).expect("the guard does not error").is_empty());
    }

    /// Literal values of the `get_name()` strategy names (all six strategies present).
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

    /// The three new strategies can be plugged into and read back from both the push / lite consumers (exposure matches the older strategies).
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
        // Single consumer + hash ring: all 4 queues land on this consumer alone
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
