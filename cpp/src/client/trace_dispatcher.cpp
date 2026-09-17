// 异步轨迹分发器实现（对应 org.apache.rocketmq.client.trace.AsyncTraceDispatcher）。
//
// 职责：钩子把 TraceContext 丢进内存队列（``append``），后台线程按
// 「攒够 batchNum 条 或 距上次发送超过 5s」两个条件触发刷写，再把编码后的文本
// 用**独立的内部生产者**发到轨迹 topic（默认 RMQ_SYS_TRACE_TOPIC）。
//
// 与 Java 逐字节语义一致：队列 2048、batchNum=min(n,20)、maxMsgSize=128000、5s 刷新、
// 按 (业务 topic, 轨迹 topic) 分组、累积 keys、按 maxMsgSize 切块异步发送、
// 空 region / 空 bean 跳过、CLOUD 时轨迹 topic 用 "rmq_sys_TRACE_DATA_" + regionId。
//
// ⚠ 防递归：内部生产者自身的 trace 必须关闭（见 start()/构造），且 SendMessageTraceHook
//   会跳过 topic 以轨迹 topic 开头的消息 —— 两道保险都要有，否则轨迹会自我复制到无限。
#include "rocketmq/client/trace_dispatcher.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <map>
#include <mutex>
#include <sstream>
#include <thread>

#include "rocketmq/client/producer.h"
#include "rocketmq/client/trace.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/namespace_util.h"
#include "rocketmq/common/util_all.h"

namespace rocketmq {

// 进程级递增计数器（用于内部生产者组名后缀 / 实例编号）
std::atomic<int32_t> AsyncTraceDispatcher::instanceCounter_{0};
std::atomic<int32_t> AsyncTraceDispatcher::groupCounter_{0};

namespace {

int32_t nextInstanceNum() {
    static std::atomic<int32_t> counter{0};
    return counter.fetch_add(1, std::memory_order_relaxed);
}

std::string typeName(TraceDispatcherType t) {
    return t == TraceDispatcherType::PRODUCE ? "PRODUCE" : "CONSUME";
}

}  // namespace

AsyncTraceDispatcher::AsyncTraceDispatcher(const std::string& group, TraceDispatcherType type,
                                           int32_t batchNum, const std::string& traceTopicName,
                                           std::shared_ptr<void> rpcHook)
    : traceInstanceId_(nextInstanceNum()),
      group_(group),
      type_(type),
      batchNum_(std::min(batchNum, MAX_BATCH_NUM)),
      traceTopicName_(traceTopicName.empty() ? std::string(MixAll::TRACE_TOPIC) : traceTopicName),
      rpcHook_(std::move(rpcHook)) {
    // 内部生产者：独立组名，关闭自身轨迹，避免自我复制。
    // ⚠ DefaultMQProducer 没有 enableTrace 开关（C++ 侧由钩子侧 Anti-recursion 兜底），
    //   这里只需保证它不注册任何 trace 钩子即可。
    traceProducer_ = std::make_unique<DefaultMQProducer>(genGroupNameForTrace());
    traceProducer_->setSendMsgTimeout(5000);
    traceProducer_->setMaxMessageSize(maxMsgSize_);
}

AsyncTraceDispatcher::~AsyncTraceDispatcher() {
    try {
        shutdown();
    } catch (...) {
        // 析构不抛
    }
}

std::string AsyncTraceDispatcher::genGroupNameForTrace() const {
    std::ostringstream oss;
    oss << TraceConstants::GROUP_NAME_PREFIX << "-" << group_ << "-" << typeName(type_) << "-"
        << groupCounter_.fetch_add(1, std::memory_order_relaxed);
    return oss.str();
}

void AsyncTraceDispatcher::start(const std::string& nameSrvAddr, AccessChannel accessChannel) {
    {
        std::lock_guard<std::mutex> lk(queueMutex_);
        if (!started_.load()) {
            traceProducer_->setNamesrvAddr(nameSrvAddr);
            traceProducer_->setInstanceName(
                std::string(TraceConstants::TRACE_INSTANCE_NAME) + "_" + nameSrvAddr);
            traceProducer_->start();
            started_.store(true);
        }
    }
    accessChannel_ = accessChannel;
    if (!worker_.joinable()) {
        stopped_.store(false);
        worker_ = std::thread(&AsyncTraceDispatcher::asyncRun, this);
    }
}

void AsyncTraceDispatcher::shutdown() {
    stopped_.store(true);
    cv_.notify_all();
    flush();
    if (worker_.joinable()) {
        worker_.join();
    }
    if (started_.load()) {
        try {
            traceProducer_->shutdown();
        } catch (const std::exception& e) {
            logger_debug("trace producer shutdown failed: " + std::string(e.what()));
        }
        started_.store(false);
    }
}

bool AsyncTraceDispatcher::append(const std::shared_ptr<TraceContext>& ctx) {
    if (ctx == nullptr) return false;
    {
        std::lock_guard<std::mutex> lk(queueMutex_);
        if (static_cast<int32_t>(traceContextQueue_.size()) >= QUEUE_CAPACITY) {
            discardCount_.fetch_add(1, std::memory_order_relaxed);
            logger_info("trace buffer full, context discarded, total discarded="
                        + std::to_string(discardCount_.load()));
            return false;
        }
        traceContextQueue_.push_back(ctx);
    }
    cv_.notify_one();
    return true;
}

void AsyncTraceDispatcher::flush() {
    while (true) {
        std::vector<std::shared_ptr<TraceContext>> batch;
        {
            std::lock_guard<std::mutex> lk(queueMutex_);
            if (traceContextQueue_.empty()) break;
            while (!traceContextQueue_.empty() && static_cast<int32_t>(batch.size()) < batchNum_) {
                batch.push_back(traceContextQueue_.front());
                traceContextQueue_.pop_front();
            }
        }
        try {
            asyncSendTraceMessage(batch);
        } catch (const std::exception& e) {
            logger_error("trace flush failed: " + std::string(e.what()));
        }
    }
}

void AsyncTraceDispatcher::asyncRun() {
    setThreadName("MQ-AsyncArrayDispatcher-Thread" + std::to_string(traceInstanceId_));
    while (!stopped_.load()) {
        try {
            flushTraceContext(false);
        } catch (const std::exception& e) {
            logger_error("trace flushTraceContext error: " + std::string(e.what()));
        }
        // 防止忙等（对齐 Java 的 Thread.sleep(5)）
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
}

void AsyncTraceDispatcher::flushTraceContext(bool forceFlush) {
    std::vector<std::shared_ptr<TraceContext>> contextList;
    {
        std::lock_guard<std::mutex> lk(queueMutex_);
        int64_t size = static_cast<int64_t>(traceContextQueue_.size());
        if (size != 0) {
            int64_t now = UtilAll::currentTimeMillis();
            if (forceFlush || size >= batchNum_
                || (now - lastFlushTime_.load()) > FLUSH_TRACE_INTERVAL) {
                int32_t n = std::min<int64_t>(batchNum_, size);
                for (int32_t i = 0; i < n; ++i) {
                    contextList.push_back(traceContextQueue_.front());
                    traceContextQueue_.pop_front();
                }
            }
        }
    }
    if (!contextList.empty()) {
        asyncSendTraceMessage(contextList);
    }
}

void AsyncTraceDispatcher::asyncSendTraceMessage(
    const std::vector<std::shared_ptr<TraceContext>>& contextList) {
    if (contextList.empty()) return;
    lastFlushTime_.store(UtilAll::currentTimeMillis());
    sendTraceData(contextList);
}

void AsyncTraceDispatcher::sendTraceData(
    const std::vector<std::shared_ptr<TraceContext>>& contextList) {
    // 按 (业务 topic, 轨迹 topic) 分组（对齐 Java sendTraceData）。
    std::map<std::string, std::vector<TraceTransferBean>> beanMap;
    for (const auto& ctx : contextList) {
        if (ctx == nullptr) continue;
        AccessChannel ch = ctx->accessChannel.value_or(accessChannel_);
        const std::string& regionId = ctx->regionId;
        if (regionId.empty() || ctx->traceBeans.empty()) {
            continue;  // 空 region / 空 bean 跳过（对齐 Java）
        }
        std::string traceTopic;
        if (ch == AccessChannel::CLOUD) {
            traceTopic = std::string(TraceConstants::TRACE_TOPIC_PREFIX) + regionId;
        } else {
            traceTopic = traceTopicName_;
        }
        const std::string& topic = ctx->traceBeans[0].topic;
        const std::string key = topic + TraceConstants::CONTENT_SPLITOR + traceTopic;
        auto tb = TraceDataEncoder::encoderFromContextBean(ctx.get());
        if (!tb.has_value()) continue;
        beanMap[key].push_back(std::move(*tb));
    }
    for (auto& kv : beanMap) {
        const std::string& key = kv.first;
        // key = topic + CONTENT_SPLITOR + traceTopic
        size_t pos = key.find(TraceConstants::CONTENT_SPLITOR);
        std::string topic = key.substr(0, pos);
        std::string traceTopic = key.substr(pos + std::string(TraceConstants::CONTENT_SPLITOR).size());
        flushData(kv.second, topic, traceTopic);
    }
}

void AsyncTraceDispatcher::flushData(const std::vector<TraceTransferBean>& transBeanList,
                                    const std::string& topic, const std::string& traceTopic) {
    // Java 签名里也带 topic（private void flushData(List, String topic, String traceTopic)），
    // 但实现只用 traceTopic —— 保留参数以对齐签名，显式标注未使用。
    (void)topic;
    if (transBeanList.empty()) return;
    std::string buffer;
    std::set<std::string> keySet;
    int32_t count = 0;
    for (const TraceTransferBean& bean : transBeanList) {
        for (const std::string& k : bean.transKey) {
            if (!k.empty()) keySet.insert(k);
        }
        buffer += bean.transData;
        ++count;
        if (static_cast<int32_t>(buffer.size()) >= maxMsgSize_) {
            // 拼装 keys：原始消息 msgId + 业务 keys，控制台靠它反查轨迹
            std::string keys;
            for (auto it = keySet.begin(); it != keySet.end(); ++it) {
                if (!keys.empty()) keys += TraceConstants::KEY_SEPARATOR;
                keys += *it;
            }
            sendTraceMessage(traceTopic, buffer, keys);
            buffer.clear();
            keySet.clear();
            count = 0;
        }
    }
    if (count > 0) {
        std::string keys;
        for (auto it = keySet.begin(); it != keySet.end(); ++it) {
            if (!keys.empty()) keys += TraceConstants::KEY_SEPARATOR;
            keys += *it;
        }
        sendTraceMessage(traceTopic, buffer, keys);
    }
}

void AsyncTraceDispatcher::sendTraceMessage(const std::string& traceTopic, const std::string& body,
                                           const std::string& keys) {
    if (!started_.load() || traceProducer_ == nullptr) return;
    try {
        Message msg(traceTopic, body);
        if (!keys.empty()) {
            msg.putProperty(MessageConst::PROPERTY_KEYS, keys);
        }
        traceProducer_->send(msg, 5000);
    } catch (const std::exception& e) {
        logger_error("send trace data failed: " + std::string(e.what()));
    }
}

}  // namespace rocketmq
