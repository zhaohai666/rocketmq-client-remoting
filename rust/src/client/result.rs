//! 发送 / 拉取 / 消费结果类型（对应 Java
//! `org.apache.rocketmq.client.producer.{SendResult,SendStatus}`、
//! `org.apache.rocketmq.client.consumer.*`、`...consumer.listener.*`，
//! 参考实现 `python/rocketmq/client/send_result.py` 与 `consumer_result.py`）。
//!
//! 两个容易踩的点：
//! - `from_code` 对**未知 code 一律回落默认值**（`SendStatus::SEND_OK` /
//!   `PullStatus::FOUND`），这是 Java `RemotingHelper` 与 Python 一致的行为，
//!   不是「懒得报错」：broker 加新状态时老客户端要能继续工作。
//! - [`ConsumeReturnType`] 的顺序**就是** Java 枚举的 ordinal，轨迹 SubAfter 的
//!   `contextCode` 直接用它，改动顺序会让控制台显示错乱。

use std::fmt;

use crate::common::message::{MessageExt, MessageQueue};

// ---------------------------------------------------------------- 发送

/// 对应 Java `SendStatus`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SendStatus {
    #[default]
    SendOk = 0,
    FlushDiskTimeout = 1,
    FlushSlaveTimeout = 2,
    SlaveNotAvailable = 3,
}

impl SendStatus {
    /// 对应 Python `SendStatus.from_code`：未知 code 回落 `SEND_OK`。
    pub fn from_code(code: i32) -> SendStatus {
        match code {
            1 => SendStatus::FlushDiskTimeout,
            2 => SendStatus::FlushSlaveTimeout,
            3 => SendStatus::SlaveNotAvailable,
            _ => SendStatus::SendOk,
        }
    }

    pub fn code(self) -> i32 {
        self as i32
    }
}

impl fmt::Display for SendStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            SendStatus::SendOk => "SEND_OK",
            SendStatus::FlushDiskTimeout => "FLUSH_DISK_TIMEOUT",
            SendStatus::FlushSlaveTimeout => "FLUSH_SLAVE_TIMEOUT",
            SendStatus::SlaveNotAvailable => "SLAVE_NOT_AVAILABLE",
        };
        f.write_str(name)
    }
}

/// 对应 Java `SendResult` / Python `SendResult`。
#[derive(Debug, Clone, PartialEq)]
pub struct SendResult {
    pub status: SendStatus,
    pub msg_id: Option<String>,
    pub message_queue: Option<MessageQueue>,
    pub queue_offset: i64,
    pub transaction_id: Option<String>,
    pub offset_msg_id: Option<String>,
    pub region_id: Option<String>,
    /// 定时/延迟消息的撤回句柄（Java `SendResult.recallHandle`，来自 SEND 响应头的
    /// `recallHandle`）。普通消息恒为 `None`。
    pub recall_handle: Option<String>,
    /// 轨迹开关：来自 SEND 响应头的 `TRACE_ON`。Java 的判据是
    /// `!"false".equals(extFields.get("TRACE_ON"))`，所以默认为 true。
    pub trace_on: bool,
}

impl Default for SendResult {
    fn default() -> SendResult {
        SendResult {
            status: SendStatus::SendOk,
            msg_id: None,
            message_queue: None,
            queue_offset: 0,
            transaction_id: None,
            offset_msg_id: None,
            region_id: None,
            recall_handle: None,
            trace_on: true,
        }
    }
}

impl SendResult {
    pub fn get_send_status(&self) -> SendStatus {
        self.status
    }

    pub fn get_msg_id(&self) -> Option<&str> {
        self.msg_id.as_deref()
    }

    pub fn get_offset_msg_id(&self) -> Option<&str> {
        self.offset_msg_id.as_deref()
    }

    pub fn get_message_queue(&self) -> Option<&MessageQueue> {
        self.message_queue.as_ref()
    }

    pub fn get_queue_offset(&self) -> i64 {
        self.queue_offset
    }

    pub fn get_transaction_id(&self) -> Option<&str> {
        self.transaction_id.as_deref()
    }

    pub fn set_transaction_id(&mut self, transaction_id: Option<&str>) {
        self.transaction_id = transaction_id.map(|s| s.to_string());
    }

    pub fn is_trace_on(&self) -> bool {
        self.trace_on
    }

    pub fn set_trace_on(&mut self, trace_on: bool) {
        self.trace_on = trace_on;
    }

    pub fn get_region_id(&self) -> Option<&str> {
        self.region_id.as_deref()
    }

    pub fn set_region_id(&mut self, region_id: Option<&str>) {
        self.region_id = region_id.map(|s| s.to_string());
    }
}

impl fmt::Display for SendResult {
    /// 逐字段对齐 Python `SendResult.__repr__`，日志/自检查里会比对这串文本。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SendResult [sendStatus={}, msgId={}, offsetMsgId={}, messageQueue={}, queueOffset={}, transactionId={}]",
            self.status,
            opt_str(&self.msg_id),
            opt_str(&self.offset_msg_id),
            match &self.message_queue {
                Some(mq) => mq.to_string(),
                None => "None".to_string(),
            },
            self.queue_offset,
            opt_str(&self.transaction_id),
        )
    }
}

/// 可选字段的显示口径：`None` 就写 `None`（Python `Optional` 字段的 repr）。
pub(crate) fn opt_str(value: &Option<String>) -> String {
    match value {
        Some(s) => s.clone(),
        None => "None".to_string(),
    }
}

/// 对应 Java `TransactionSendResult`（比 `SendResult` 多一个本地事务状态）。
#[derive(Debug, Clone, PartialEq)]
pub struct TransactionSendResult {
    pub local_transaction_state: Option<LocalTransactionState>,
    pub inner: SendResult,
}

/// 对应 Java `LocalTransactionState`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LocalTransactionState {
    CommitMessage = 0,
    RollbackMessage = 1,
    Unknow = 2,
}

impl LocalTransactionState {
    /// 对应 Python `TransactionSendResult` 的 code 解析：未知值按 `UNKNOW`。
    pub fn from_code(code: i32) -> LocalTransactionState {
        match code {
            0 => LocalTransactionState::CommitMessage,
            1 => LocalTransactionState::RollbackMessage,
            _ => LocalTransactionState::Unknow,
        }
    }

    pub fn code(self) -> i32 {
        self as i32
    }
}

impl fmt::Display for LocalTransactionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            LocalTransactionState::CommitMessage => "COMMIT_MESSAGE",
            LocalTransactionState::RollbackMessage => "ROLLBACK_MESSAGE",
            LocalTransactionState::Unknow => "UNKNOW",
        };
        f.write_str(name)
    }
}

// ---------------------------------------------------------------- 拉取

/// 对应 Java `PullStatus`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum PullStatus {
    #[default]
    Found = 0,
    NoNewMsg = 1,
    NoMatchedMsg = 2,
    OffsetIllegal = 3,
}

impl PullStatus {
    /// 对应 Java `PullStatus.valueOf(String)` + Python `from_code`：
    /// 未知值（含 code 越界、名字不认识的字符串）一律 `FOUND`。
    pub fn from_code(code: i32) -> PullStatus {
        match code {
            1 => PullStatus::NoNewMsg,
            2 => PullStatus::NoMatchedMsg,
            3 => PullStatus::OffsetIllegal,
            _ => PullStatus::Found,
        }
    }

    pub fn from_name(name: &str) -> PullStatus {
        match name {
            "NO_NEW_MSG" => PullStatus::NoNewMsg,
            "NO_MATCHED_MSG" => PullStatus::NoMatchedMsg,
            "OFFSET_ILLEGAL" => PullStatus::OffsetIllegal,
            _ => PullStatus::Found,
        }
    }

    pub fn code(self) -> i32 {
        self as i32
    }

    pub fn name(self) -> &'static str {
        match self {
            PullStatus::Found => "FOUND",
            PullStatus::NoNewMsg => "NO_NEW_MSG",
            PullStatus::NoMatchedMsg => "NO_MATCHED_MSG",
            PullStatus::OffsetIllegal => "OFFSET_ILLEGAL",
        }
    }
}

impl fmt::Display for PullStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// 对应 Java `PullResult` / Python `PullResult`。
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PullResult {
    pub status: PullStatus,
    pub next_begin_offset: i64,
    pub min_offset: i64,
    pub max_offset: i64,
    pub msg_found_list: Vec<MessageExt>,
}

impl PullResult {
    pub fn new(status: PullStatus) -> PullResult {
        PullResult { status, ..Default::default() }
    }

    pub fn get_status(&self) -> PullStatus {
        self.status
    }

    pub fn get_msg_found_list(&self) -> &[MessageExt] {
        &self.msg_found_list
    }

    pub fn get_next_begin_offset(&self) -> i64 {
        self.next_begin_offset
    }
}

impl fmt::Display for PullResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PullResult [status={}, nextBeginOffset={}, minOffset={}, maxOffset={}, msgFoundList.size={}]",
            self.status,
            self.next_begin_offset,
            self.min_offset,
            self.max_offset,
            self.msg_found_list.len(),
        )
    }
}

/// 对应 Java `PopStatus`。⚠ 与 `PullStatus` 的 code 排布不同，别混用。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum PopStatus {
    #[default]
    Found = 0,
    NoNewMsg = 1,
    PollingFull = 2,
    PollingNotFound = 3,
}

impl PopStatus {
    /// 对应 Python 的解析：broker 给的是名字串，未知值按 `FOUND` 处理。
    pub fn from_name(name: &str) -> PopStatus {
        match name {
            "NO_NEW_MSG" => PopStatus::NoNewMsg,
            "POLLING_FULL" => PopStatus::PollingFull,
            "POLLING_NOT_FOUND" => PopStatus::PollingNotFound,
            _ => PopStatus::Found,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            PopStatus::Found => "FOUND",
            PopStatus::NoNewMsg => "NO_NEW_MSG",
            PopStatus::PollingFull => "POLLING_FULL",
            PopStatus::PollingNotFound => "POLLING_NOT_FOUND",
        }
    }
}

impl fmt::Display for PopStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// 对应 Java `PopResult` / Python `PopResult`。
///
/// `start_offset_info` / `msg_offset_info` / `order_count_info` 保留 broker 原样字符串，
/// 解析交给 [`crate::remoting::protocol::extra_info`]。
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PopResult {
    pub status: PopStatus,
    pub msg_found_list: Vec<MessageExt>,
    pub rest_num: i64,
    pub pop_time: i64,
    pub invisible_time: i64,
    pub revive_qid: i32,
    pub start_offset_info: Option<String>,
    pub msg_offset_info: Option<String>,
    pub order_count_info: Option<String>,
}

impl fmt::Display for PopResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PopResult [status={}, restNum={}, popTime={}, invisibleTime={}, reviveQid={}, msgFoundList.size={}]",
            self.status,
            self.rest_num,
            self.pop_time,
            self.invisible_time,
            self.revive_qid,
            self.msg_found_list.len(),
        )
    }
}

/// 对应 `change_invisible_time` 的结果（Python `ChangeInvisibleTimeResult`）。
///
/// `extra_info` 是用响应里**新的** popTime/invisibleTime/reviveQid 重建的 CK 串，
/// 后续 ACK 必须用它，而不是请求时传进去的旧串。
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChangeInvisibleTimeResult {
    pub response_code: i32,
    pub pop_time: i64,
    pub invisible_time: i64,
    pub revive_qid: i32,
    pub extra_info: Option<String>,
    pub success: bool,
}

impl ChangeInvisibleTimeResult {
    pub fn new(response_code: i32) -> ChangeInvisibleTimeResult {
        ChangeInvisibleTimeResult {
            response_code,
            pop_time: 0,
            invisible_time: 0,
            revive_qid: 0,
            extra_info: None,
            success: response_code == 0,
        }
    }
}

impl fmt::Display for ChangeInvisibleTimeResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ChangeInvisibleTimeResult [responseCode={}, popTime={}, invisibleTime={}, reviveQid={}]",
            self.response_code, self.pop_time, self.invisible_time, self.revive_qid,
        )
    }
}

// ---------------------------------------------------------------- 消费

/// 对应 Java `ConsumeReturnType`（轨迹 `contextCode` 用它）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConsumeReturnType {
    Success = 0,
    TimeOut = 1,
    Exception = 2,
    ReturnNull = 3,
    Failed = 4,
}

impl ConsumeReturnType {
    pub fn code(self) -> i32 {
        self as i32
    }

    /// Java `ConsumeReturnType#name()`：轨迹 `ConsumeContextType` 属性存的是这个名字，
    /// 消费端按名反查 ordinal（见 `trace_hook` 里的 `ConsumeReturnType.valueOf`）。
    pub fn name(self) -> &'static str {
        match self {
            ConsumeReturnType::Success => "SUCCESS",
            ConsumeReturnType::TimeOut => "TIME_OUT",
            ConsumeReturnType::Exception => "EXCEPTION",
            ConsumeReturnType::ReturnNull => "RETURNNULL",
            ConsumeReturnType::Failed => "FAILED",
        }
    }
}

/// 对应 Java `ConsumeConcurrentlyStatus`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ConsumeConcurrentlyStatus {
    #[default]
    ConsumeSuccess = 0,
    ReconsumeLater = 1,
}

impl ConsumeConcurrentlyStatus {
    pub fn name(self) -> &'static str {
        match self {
            ConsumeConcurrentlyStatus::ConsumeSuccess => "CONSUME_SUCCESS",
            ConsumeConcurrentlyStatus::ReconsumeLater => "RECONSUME_LATER",
        }
    }
}

impl fmt::Display for ConsumeConcurrentlyStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// 对应 Java `ConsumeOrderlyStatus`。
///
/// 变体顺序与判别值逐字对齐 Java 枚举（`SUCCESS, ROLLBACK, COMMIT, SUSPEND_...`）：
/// `ROLLBACK` / `COMMIT` 在 Java 侧标着 `@Deprecated` + "only for binlog consumption"，
/// 用法见 `consumer.rs` 的 `auto_commit` 两条分支。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ConsumeOrderlyStatus {
    #[default]
    Success = 0,
    Rollback = 1,
    Commit = 2,
    SuspendCurrentQueueAMoment = 3,
}

impl ConsumeOrderlyStatus {
    pub fn name(self) -> &'static str {
        match self {
            ConsumeOrderlyStatus::Success => "SUCCESS",
            ConsumeOrderlyStatus::Rollback => "ROLLBACK",
            ConsumeOrderlyStatus::Commit => "COMMIT",
            ConsumeOrderlyStatus::SuspendCurrentQueueAMoment => {
                "SUSPEND_CURRENT_QUEUE_A_MOMENT"
            }
        }
    }
}

impl fmt::Display for ConsumeOrderlyStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// 对应 Java `ConsumeConcurrentlyContext`。
#[derive(Debug, Clone)]
pub struct ConsumeConcurrentlyContext {
    pub message_queue: Option<MessageQueue>,
    /// 缺省 0；为 0 时由投递线程改写成 `3 + reconsumeTimes`
    /// （见 `consumer.rs` 的重投逻辑，对齐 Python `_send_back_batch`）。
    pub delay_level_when_next_consume: i32,
    /// 对应 Java `ConsumeConcurrentlyContext.ackIndex`（默认 `Integer.MAX_VALUE`）：
    /// listener 用它表达「这批只认可到第几条」（含自身），其后的条目按状态回投/丢弃。
    /// 默认即「整批认可」，只有 listener 主动调小才会部分 ack。
    pub ack_index: i32,
}

impl ConsumeConcurrentlyContext {
    pub fn new(message_queue: Option<MessageQueue>) -> ConsumeConcurrentlyContext {
        ConsumeConcurrentlyContext {
            message_queue,
            delay_level_when_next_consume: 0,
            ack_index: i32::MAX,
        }
    }
}

/// 对应 Java `ConsumeOrderlyContext`。
#[derive(Debug, Clone)]
pub struct ConsumeOrderlyContext {
    pub message_queue: Option<MessageQueue>,
    /// 对应 Java `ConsumeOrderlyContext.autoCommit`（默认 true）。置 false 后 SUCCESS 只记
    /// TPS 不提交、COMMIT/ROLLBACK 才生效（只给 binlog 消费用）。
    pub auto_commit: bool,
    /// 对应 Java `ConsumeOrderlyContext.suspendCurrentQueueTimeMillis`，**默认 -1**
    /// （Java 就是这个默认值）：-1 表示"没指定"，挂起时长回落到消费者配置的
    /// `suspend_current_queue_time_millis`；解析出来的值再由投递侧钳到 [10, 30000]
    /// （Java `ConsumeMessageOrderlyService#submitConsumeRequestLater:211-234`）。
    pub suspend_current_queue_time_millis: i64,
}

impl ConsumeOrderlyContext {
    pub fn new(message_queue: Option<MessageQueue>) -> ConsumeOrderlyContext {
        ConsumeOrderlyContext {
            message_queue,
            auto_commit: true,
            suspend_current_queue_time_millis: -1,
        }
    }
}

/// 对应 Java `MessageListenerConcurrently`。
pub trait MessageListenerConcurrently: Send + Sync {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus;
}

/// 对应 Java `MessageListenerOrderly`。
pub trait MessageListenerOrderly: Send + Sync {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        context: &mut ConsumeOrderlyContext,
    ) -> ConsumeOrderlyStatus;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_status_code_mapping_and_unknown_falls_back() {
        assert_eq!(SendStatus::from_code(0), SendStatus::SendOk);
        assert_eq!(SendStatus::from_code(1), SendStatus::FlushDiskTimeout);
        assert_eq!(SendStatus::from_code(2), SendStatus::FlushSlaveTimeout);
        assert_eq!(SendStatus::from_code(3), SendStatus::SlaveNotAvailable);
        // 未知 code 回落 SEND_OK（与 Python/Java 一致，别改成报错）
        assert_eq!(SendStatus::from_code(42), SendStatus::SendOk);
        assert_eq!(SendStatus::from_code(-1), SendStatus::SendOk);
        assert_eq!(SendStatus::FlushSlaveTimeout.code(), 2);
        assert_eq!(SendStatus::SlaveNotAvailable.to_string(), "SLAVE_NOT_AVAILABLE");
    }

    #[test]
    fn send_result_defaults_and_repr() {
        let result = SendResult::default();
        assert_eq!(result.status, SendStatus::SendOk);
        assert_eq!(result.queue_offset, 0);
        // broker 默认回 TRACE_ON=true，缺省也要是 true
        assert!(result.trace_on);
        assert_eq!(
            result.to_string(),
            "SendResult [sendStatus=SEND_OK, msgId=None, offsetMsgId=None, \
             messageQueue=None, queueOffset=0, transactionId=None]"
        );

        let mut with_queue = SendResult {
            msg_id: Some("AC10".into()),
            message_queue: Some(MessageQueue::new("TopicTest", "broker-a", 3)),
            queue_offset: 88,
            ..Default::default()
        };
        assert_eq!(
            with_queue.to_string(),
            "SendResult [sendStatus=SEND_OK, msgId=AC10, offsetMsgId=None, \
             messageQueue=TopicTest broker-a 3, queueOffset=88, transactionId=None]"
        );
        with_queue.set_transaction_id(Some("tx-1"));
        assert_eq!(with_queue.get_transaction_id(), Some("tx-1"));
    }

    #[test]
    fn local_transaction_state_uses_java_ordinal() {
        assert_eq!(LocalTransactionState::CommitMessage.code(), 0);
        assert_eq!(LocalTransactionState::RollbackMessage.code(), 1);
        assert_eq!(LocalTransactionState::Unknow.code(), 2);
        assert_eq!(LocalTransactionState::from_code(9), LocalTransactionState::Unknow);
        assert_eq!(LocalTransactionState::Unknow.to_string(), "UNKNOW");
    }

    #[test]
    fn pull_status_parsing_matches_reference() {
        assert_eq!(PullStatus::from_code(0), PullStatus::Found);
        assert_eq!(PullStatus::from_code(1), PullStatus::NoNewMsg);
        assert_eq!(PullStatus::from_code(3), PullStatus::OffsetIllegal);
        assert_eq!(PullStatus::from_code(7), PullStatus::Found);
        assert_eq!(PullStatus::from_name("NO_MATCHED_MSG"), PullStatus::NoMatchedMsg);
        assert_eq!(PullStatus::from_name("weird"), PullStatus::Found);
        assert_eq!(PullStatus::OffsetIllegal.code(), 3);
        assert_eq!(PullStatus::NoNewMsg.name(), "NO_NEW_MSG");
    }

    #[test]
    fn pop_status_codes_differ_from_pull_status() {
        // POLLING_FULL 是 2，而 PullStatus 的 2 是 NO_MATCHED_MSG —— 两套不能混用
        assert_eq!(PopStatus::PollingFull as i32, 2);
        assert_eq!(PullStatus::NoMatchedMsg as i32, 2);
        assert_eq!(PopStatus::from_name("POLLING_NOT_FOUND"), PopStatus::PollingNotFound);
        assert_eq!(PopStatus::from_name(""), PopStatus::Found);
    }

    #[test]
    fn pull_and_pop_result_repr() {
        let pull = PullResult {
            status: PullStatus::Found,
            next_begin_offset: 5,
            min_offset: 1,
            max_offset: 6,
            msg_found_list: vec![MessageExt::new(), MessageExt::new()],
        };
        assert_eq!(
            pull.to_string(),
            "PullResult [status=FOUND, nextBeginOffset=5, minOffset=1, maxOffset=6, msgFoundList.size=2]"
        );
        assert_eq!(
            PopResult::default().to_string(),
            "PopResult [status=FOUND, restNum=0, popTime=0, invisibleTime=0, reviveQid=0, msgFoundList.size=0]"
        );
    }

    #[test]
    fn change_invisible_time_success_follows_code() {
        assert!(ChangeInvisibleTimeResult::new(0).success);
        let failed = ChangeInvisibleTimeResult::new(1);
        assert!(!failed.success);
        assert_eq!(
            failed.to_string(),
            "ChangeInvisibleTimeResult [responseCode=1, popTime=0, invisibleTime=0, reviveQid=0]"
        );
    }

    #[test]
    fn consume_contexts_default_like_java() {
        let ctx = ConsumeConcurrentlyContext::new(None);
        assert_eq!(ctx.delay_level_when_next_consume, 0);
        // Java ConsumeConcurrentlyContext:33 —— 默认「整批认可」，不是「一条都不认可」
        assert_eq!(ctx.ack_index, i32::MAX);
        let orderly = ConsumeOrderlyContext::new(Some(MessageQueue::new("T", "b", 0)));
        assert!(orderly.auto_commit);
        // Java ConsumeOrderlyContext:27 —— 默认 -1 = 「没指定」，回落到消费者配置
        assert_eq!(orderly.suspend_current_queue_time_millis, -1);
        assert_eq!(ConsumeConcurrentlyStatus::ReconsumeLater.to_string(), "RECONSUME_LATER");
        assert_eq!(
            ConsumeOrderlyStatus::SuspendCurrentQueueAMoment.name(),
            "SUSPEND_CURRENT_QUEUE_A_MOMENT"
        );
        // 声明顺序（= Java 枚举序号）也要对得上：COMMIT/ROLLBACK 只给 binlog 消费用
        assert_eq!(ConsumeOrderlyStatus::Success as i32, 0);
        assert_eq!(ConsumeOrderlyStatus::Rollback as i32, 1);
        assert_eq!(ConsumeOrderlyStatus::Commit as i32, 2);
        assert_eq!(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment as i32, 3);
        assert_eq!(ConsumeOrderlyStatus::Commit.name(), "COMMIT");
    }

    #[test]
    fn consume_return_type_ordinals_are_stable() {
        // 轨迹 contextCode 直接写 ordinal，顺序变了控制台就对不上
        assert_eq!(ConsumeReturnType::Success.code(), 0);
        assert_eq!(ConsumeReturnType::TimeOut.code(), 1);
        assert_eq!(ConsumeReturnType::Exception.code(), 2);
        assert_eq!(ConsumeReturnType::ReturnNull.code(), 3);
        assert_eq!(ConsumeReturnType::Failed.code(), 4);
    }
}
