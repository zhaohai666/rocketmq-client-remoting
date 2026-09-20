#include "rocketmq/client/lite_pull_consumer.h"

#include <algorithm>
#include <ctime>
#include <optional>
#include <utility>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/validators.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"

namespace rocketmq {

namespace {

// 与 pull_consumer.cpp 同款：按 ';' 切分 namesrv 地址。
std::vector<std::string> splitSemicolon(const std::string& addr) {
    std::vector<std::string> out;
    size_t start = 0;
    while (start <= addr.size()) {
        size_t pos = addr.find(';', start);
        std::string piece = addr.substr(start, pos == std::string::npos ? std::string::npos : pos - start);
        // trim
        size_t b = piece.find_first_not_of(" \t\r\n");
        if (b != std::string::npos) {
            size_t e = piece.find_last_not_of(" \t\r\n");
            std::string t = piece.substr(b, e - b + 1);
            if (!t.empty()) out.push_back(t);
        }
        if (pos == std::string::npos) break;
        start = pos + 1;
    }
    return out;
}

int64_t nowMillis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// 对应 Java UtilAll.parseDate(ts, UtilAll.YYYYMMDDHHMMSS)：该字段**只**是 14 位本地墙钟日期。
// 旧实现把任何纯数字串当 epoch 处理，"20230101000000" 会被解释成公元 2611 年，起点彻底错位；
// 而它真正想做日期解析的分支用了 strptime/timegm——POSIX 专有，MSVC 下既不可达也编不过。
int64_t parseConsumeTimestamp(const std::string& ts) {
    auto invalid = [&ts] {
        return MQClientException(
            "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received " + ts);
    };
    if (ts.size() != 14) throw invalid();

    int field[6] = {0, 0, 0, 0, 0, 0};
    size_t pos = 0;
    for (int i = 0; i < 6; ++i) {
        int width = i == 0 ? 4 : 2;
        for (int k = 0; k < width; ++k) {
            char c = ts[pos++];
            if (c < '0' || c > '9') throw invalid();
            field[i] = field[i] * 10 + (c - '0');
        }
    }

    std::tm tmv {};
    tmv.tm_year = field[0] - 1900;
    tmv.tm_mon = field[1] - 1;
    tmv.tm_mday = field[2];
    tmv.tm_hour = field[3];
    tmv.tm_min = field[4];
    tmv.tm_sec = field[5];
    tmv.tm_isdst = -1;
    std::time_t sec = std::mktime(&tmv);
    if (sec == static_cast<std::time_t>(-1)) throw invalid();
    return static_cast<int64_t>(sec) * 1000;
}

}  // namespace

DefaultLitePullConsumer::DefaultLitePullConsumer(const std::string& consumerGroup) {
    // 与 push consumer 同款首道守卫：只挡空白组名（⚠ 旧实现用 empty()，纯空白漏过，
    // 改为 UtilAll::isBlank）；完整校验在 start() 里走 Validators::checkGroup。
    if (UtilAll::isBlank(consumerGroup)) {
        throw MQClientException("consumerGroup is empty");
    }
    consumerGroup_ = consumerGroup;
    // 对应 Java DefaultLitePullConsumer.consumeTimestamp 的字段初值：now - 30 分钟。
    // 留空会让 CONSUME_FROM_TIMESTAMP 退化成「从当前时刻起消费」。
    consumeTimestamp_ = UtilAll::timeMillisToHumanString3(nowMillis() - 30 * 60 * 1000);
}

DefaultLitePullConsumer::~DefaultLitePullConsumer() {
    try {
        shutdown();
    } catch (...) {
    }
}

// ---------------------------------------------------------------- 配置
void DefaultLitePullConsumer::setNamesrvAddr(const std::string& addr) {
    nameServerAddrs_ = splitSemicolon(addr);
}

void DefaultLitePullConsumer::setNameServerAddresses(const std::vector<std::string>& addrs) {
    nameServerAddrs_ = addrs;
}

// ---------------------------------------------------------------- 订阅 / 分配
void DefaultLitePullConsumer::subscribe(const std::string& topic, const std::string& subExpression) {
    assignMode_ = false;
    std::string ns = withNamespace(topic);
    subscription_[ns] = subExpression;
    try {
        subscriptionData_[ns] = FilterAPI::buildSubscriptionData(ns, subExpression);
    } catch (...) {
        subscriptionData_.erase(ns);
    }
}

void DefaultLitePullConsumer::setSubExpressionForAssign(const std::string& topic,
                                                       const std::string& subExpression) {
    std::string ns = withNamespace(topic);
    assignSubExpr_[ns] = subExpression;
    try {
        subscriptionData_[ns] = FilterAPI::buildSubscriptionData(ns, subExpression);
    } catch (...) {
        subscriptionData_.erase(ns);
    }
}

void DefaultLitePullConsumer::assign(const std::vector<MessageQueue>& messageQueues) {
    assignMode_ = true;
    assigned_ = std::set<MessageQueue>(messageQueues.begin(), messageQueues.end());
    for (const MessageQueue& mq : assigned_) {
        if (nextOffset_.find(mq) == nextOffset_.end()) {
            try {
                nextOffset_[mq] = resolveInitialOffset(mq);
        } catch (...) {
            logger_debug("lite assign: resolve initial offset failed for " + mq.toString());
        }
    }
}
}

// ---------------------------------------------------------------- 生命周期
void DefaultLitePullConsumer::start() {
    if (started_) return;
    if (!namespace_.empty()) {
        consumerGroup_ = NamespaceUtil::wrapNamespace(namespace_, consumerGroup_);
    }
    // 对应 Java DefaultLitePullConsumerImpl.checkConfig(:415) / Python：组名合法性 +
    // 挡掉 DEFAULT_CONSUMER。纯本地校验，排在地址/订阅检查之前，失败不碰网络。
    Validators::checkGroup(consumerGroup_);
    if (consumerGroup_ == MixAll::DEFAULT_CONSUMER_GROUP) {
        throw MQClientException(
            "consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.");
    }
    if (nameServerAddrs_.empty()) {
        throw MQClientException("name server address is not set");
    }
    if (subscription_.empty() && !assignMode_) {
        throw MQClientException("subscription is not set, call subscribe() or assign() first");
    }
    // 对应 Java DefaultMQPushConsumerImpl.checkConfig（:1058）：启动即无条件校验，
    // 而不是等到算起点时抛出、被下面的 catch(...) 吞掉后静默退化成从 max offset 消费。
    parseConsumeTimestamp(consumeTimestamp_);
    if (clientId_.empty()) {
        clientId_ = buildClientId(instanceName_);
    }
    mqClient_.reset(new MQClientInstance(clientId_, nameServerAddrs_,
                                        /*connectTimeoutMillis=*/3000,
                                        /*invokeTimeoutMillis=*/10000));
    mqClient_->start();
    // 登记在用 topic，让路由周期刷新（订阅 topic 与已 assign 队列所在 topic）。
    for (const auto& kv : subscription_) {
        mqClient_->registerTopicInUse(kv.first);
    }
    for (const MessageQueue& mq : assigned_) {
        mqClient_->registerTopicInUse(mq.topic);
    }
    if (rpcHook_ && !mqClient_->registerRPCHook(rpcHook_)) {
        logger_warn("lite pull consumer rpc hook ignored: MQClientInstance already has one (clientId="
                    + clientId_ + ")");
    }
    if (assignMode_) {
        for (const MessageQueue& mq : assigned_) {
            if (nextOffset_.find(mq) == nextOffset_.end()) {
                try {
                    nextOffset_[mq] = resolveInitialOffset(mq);
            } catch (...) {
                logger_debug("lite start: resolve initial offset failed for " + mq.toString());
            }
            }
        }
    }
    // 先把 tag 订阅注册给 broker（心跳），再启动后台拉取，避免首轮拉取因 broker 不认订阅而丢消息。
    sendHeartbeatToAllBroker();
    heartbeatThread_ = std::thread(&DefaultLitePullConsumer::heartbeatLoop, this);
    running_ = true;
    started_ = true;
    pullThread_ = std::thread(&DefaultLitePullConsumer::pullServiceLoop, this);
}

void DefaultLitePullConsumer::shutdown() {
    if (!started_) return;
    started_ = false;
    running_ = false;
    if (autoCommit_) {
        try {
            commit();
        } catch (...) {
            logger_debug("lite shutdown commit failed");
        }
    }
    {
        std::lock_guard<std::mutex> lk(bufferMutex_);
        bufferCv_.notify_all();
    }
    if (pullThread_.joinable()) pullThread_.join();
    if (heartbeatThread_.joinable()) heartbeatThread_.join();
    if (mqClient_ != nullptr) {
        mqClient_->shutdown();
        mqClient_.reset();
    }
}

// ---------------------------------------------------------------- 拉取服务
void DefaultLitePullConsumer::pullServiceLoop() {
    while (running_) {
        bool gotAny = false;
        try {
            auto now = std::chrono::steady_clock::now();
            if (!assignMode_ && (lastRebalanceTs_ == std::chrono::steady_clock::time_point() ||
                                 std::chrono::duration_cast<std::chrono::milliseconds>(
                                     now - lastRebalanceTs_)
                                         .count() > 1000)) {
                rebalance();
                lastRebalanceTs_ = now;
            }
            for (const MessageQueue& mq : assigned_) {
                if (!running_) break;
                if (paused_.find(mq) != paused_.end()) continue;
                if (pullOne(mq)) gotAny = true;
            }
        } catch (...) {
            logger_debug("lite pull service error");
        }
        // 退避：轮询空时放慢，避免打爆 broker；有消息则尽快回填缓冲。
        auto backoff = gotAny ? std::chrono::milliseconds(5)
                              : std::chrono::milliseconds(pullIntervalMillis_);
        std::this_thread::sleep_for(backoff);
    }
}

std::string DefaultLitePullConsumer::subscriptionFor(const std::string& topic) const {
    auto it = subscription_.find(topic);
    if (it != subscription_.end()) return it->second;
    auto ait = assignSubExpr_.find(topic);
    if (ait != assignSubExpr_.end()) return ait->second;
    return "*";
}

bool DefaultLitePullConsumer::pullOne(const MessageQueue& mq) {
    auto it = nextOffset_.find(mq);
    int64_t offset = (it != nextOffset_.end()) ? it->second : resolveInitialOffset(mq);
    nextOffset_[mq] = offset;
    std::string sub = subscriptionFor(mq.topic);
    // 短轮询（suspend=false），位点由 auto-commit 单独提交（与 Java LitePull 一致）。
    const int32_t sysFlag = PullSysFlag::buildSysFlag(/*commitOffset=*/false,
                                                      /*suspend=*/false,
                                                      /*subscription=*/true,
                                                      /*classFilter=*/false);
    PullResult result;
    try {
        result = mqClient_->pullMessage(consumerGroup_, mq, offset, pullBatchSize_, sysFlag, 0, sub,
                                        /*subVersion=*/0, ExpressionType::TAG,
                                        /*timeoutMillis=*/30000, /*maxMsgBytes=*/-1,
                                        /*suspendTimeoutMillis=*/15000);
    } catch (...) {
        logger_debug("lite pull_one failed for " + mq.topic + "@" + std::to_string(mq.queueId));
        return false;
    }
    if (result.status == PullStatus::FOUND && !result.msgFoundList.empty()) {
        std::vector<MessageExt> msgs = std::move(result.msgFoundList);
        filterTags(mq.topic, msgs, sub);
        if (!msgs.empty()) {
            enqueue(msgs);
            nextOffset_[mq] = msgs.back().queueOffset + 1;
            if (autoCommit_) maybeCommit(mq);
            return true;
        }
    }
    return false;
}

bool DefaultLitePullConsumer::filterTags(const std::string& topic, std::vector<MessageExt>& msgs,
                                        const std::string& sub) {
    if (sub.empty() || sub == "*") return true;
    SubscriptionData subData;
    try {
        subData = FilterAPI::buildSubscriptionData(topic, sub);
    } catch (...) {
        return true;
    }
    if (subData.tagsSet.empty()) return true;
    size_t w = 0;
    for (size_t r = 0; r < msgs.size(); ++r) {
        const std::string tag = msgs[r].getTags();
        if (subData.tagsSet.find(tag) != subData.tagsSet.end()) {
            if (w != r) msgs[w] = std::move(msgs[r]);
            ++w;
        }
    }
    msgs.resize(w);
    return true;
}

void DefaultLitePullConsumer::enqueue(const std::vector<MessageExt>& msgs) {
    std::lock_guard<std::mutex> lk(bufferMutex_);
    for (const MessageExt& m : msgs) {
        localBuffer_.push_back(m);
    }
    bufferCv_.notify_all();
}

void DefaultLitePullConsumer::maybeCommit(const MessageQueue& mq) {
    int64_t now = nowMillis();
    auto lit = lastCommit_.find(mq);
    if (lit != lastCommit_.end()) {
        if (now - lit->second < autoCommitIntervalMillis_) return;
    }
    try {
        mqClient_->updateConsumerOffset(consumerGroup_, mq, nextOffset_[mq]);
        lastCommit_[mq] = now;
    } catch (...) {
        logger_debug("lite auto-commit failed for " + mq.toString());
    }
}

int64_t DefaultLitePullConsumer::resolveInitialOffset(const MessageQueue& mq) {
    // 未启动时 mqClient_ 为空：Java 允许 start 前 assign（位点推迟到 start 时再解析），
    // 这里必须显式抛错而不是解引用空指针（调用方 assign/rebalance 均已兜住）。
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    auto sit = seekOffset_.find(mq);
    if (sit != seekOffset_.end()) return sit->second;
    // 与 Java RebalanceLitePullImpl.computePullFromWhereWithException 同序：先读已提交位点，
    // 只有真正 QUERY_NOT_FOUND 时才按 consumeFromWhere 计算。broker 在 setZeroIfNotFound
    // 未设置且队首仍在 commitlog 内时会直接回 0，此时 consumeFromWhere 不参与。
    try {
        int64_t off = 0;
        if (mqClient_->queryConsumerOffset(consumerGroup_, mq, off)) {
            return off;
        }
    } catch (...) {
    }
    if (consumeFromWhere_ == ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET) {
        return mqClient_->getMinOffset(mq);
    }
    if (consumeFromWhere_ == ConsumeFromWhere::CONSUME_FROM_TIMESTAMP) {
        return mqClient_->searchOffsetByTimestamp(mq, parseConsumeTimestamp(consumeTimestamp_));
    }
    return mqClient_->getMaxOffset(mq);
}

// ---------------------------------------------------------------- rebalance
std::vector<MessageQueue> DefaultLitePullConsumer::allocateMessageQueueAveragely(
    const std::string& /*consumerGroup*/, const std::string& currentCid,
    const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll) {
    if (mqAll.empty()) return {};
    if (cidAll.empty() || std::find(cidAll.begin(), cidAll.end(), currentCid) == cidAll.end()) {
        return {};
    }
    int index = static_cast<int>(std::find(cidAll.begin(), cidAll.end(), currentCid) - cidAll.begin());
    int mod = static_cast<int>(mqAll.size() % cidAll.size());
    int avg = mqAll.size() <= cidAll.size()
                  ? 1
                  : (mod > 0 && index < mod ? static_cast<int>(mqAll.size()) / static_cast<int>(cidAll.size()) + 1
                                            : static_cast<int>(mqAll.size()) / static_cast<int>(cidAll.size()));
    int startIndex =
        (mod > 0 && index < mod) ? index * avg : index * avg + mod;
    int range = std::min(avg, static_cast<int>(mqAll.size()) - startIndex);
    if (range <= 0) return {};
    return std::vector<MessageQueue>(mqAll.begin() + startIndex, mqAll.begin() + startIndex + range);
}

void DefaultLitePullConsumer::rebalance() {
    std::set<MessageQueue> newSet;
    for (const auto& kv : subscription_) {
        std::vector<MessageQueue> mqAll;
        try {
            std::shared_ptr<TopicPublishInfo> info = mqClient_->getTopicPublishInfo(kv.first);
            mqAll = info->msgQueueList;
        } catch (...) {
            mqAll.clear();
        }
        std::vector<std::string> cidAll =
            mqClient_->getConsumerIdListByGroup(kv.first, consumerGroup_);
        if (std::find(cidAll.begin(), cidAll.end(), clientId_) == cidAll.end()) {
            cidAll.push_back(clientId_);
        }
        std::vector<MessageQueue> allocated =
            allocateMessageQueueAveragely(consumerGroup_, clientId_, mqAll, cidAll);
        newSet.insert(allocated.begin(), allocated.end());
    }
    if (newSet != assigned_) {
        std::set<MessageQueue> old = assigned_;
        assigned_ = newSet;
        for (const MessageQueue& mq : newSet) {
            if (nextOffset_.find(mq) == nextOffset_.end()) {
                try {
                    nextOffset_[mq] = resolveInitialOffset(mq);
                } catch (...) {
                    logger_debug("lite rebalance: resolve offset failed for " + mq.toString());
                }
            }
        }
        for (const MessageQueue& mq : old) {
            if (newSet.find(mq) == newSet.end()) {
                nextOffset_.erase(mq);
                lastCommit_.erase(mq);
            }
        }
        if (messageQueueListener_ != nullptr) {
            try {
                messageQueueListener_->messageQueueChanged(mqAllOfSubscription(), newSetAsVector(newSet));
            } catch (...) {
            }
        }
    }
}

// 收集订阅的全部队列（listener 回调用）
std::vector<MessageQueue> DefaultLitePullConsumer::mqAllOfSubscription() const {
    std::vector<MessageQueue> all;
    for (const auto& kv : subscription_) {
        try {
            std::shared_ptr<TopicPublishInfo> info = mqClient_->getTopicPublishInfo(kv.first);
            for (const MessageQueue& mq : info->msgQueueList) all.push_back(mq);
        } catch (...) {
        }
    }
    return all;
}

std::vector<MessageQueue> DefaultLitePullConsumer::newSetAsVector(const std::set<MessageQueue>& s) const {
    return std::vector<MessageQueue>(s.begin(), s.end());
}

// ---------------------------------------------------------------- poll / 位点
std::vector<MessageExt> DefaultLitePullConsumer::poll(int32_t timeoutMillis) {
    int32_t timeout = timeoutMillis > 0 ? timeoutMillis : pollTimeoutMillis_;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout);
    std::unique_lock<std::mutex> lk(bufferMutex_);
    while (localBuffer_.empty()) {
        auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now())
                             .count();
        if (remaining <= 0) return {};
        bufferCv_.wait_for(lk, std::chrono::milliseconds(remaining));
    }
    std::vector<MessageExt> out;
    while (!localBuffer_.empty() && out.size() < 1024) {
        out.push_back(std::move(localBuffer_.front()));
        localBuffer_.pop_front();
    }
    return out;
}

void DefaultLitePullConsumer::seek(const MessageQueue& mq, int64_t offset) {
    seekOffset_[mq] = offset;
    nextOffset_[mq] = offset;
    std::lock_guard<std::mutex> lk(bufferMutex_);
    std::deque<MessageExt> kept;
    for (MessageExt& m : localBuffer_) {
        bool same = (m.topic == mq.topic && m.brokerName == mq.brokerName &&
                     m.queueId == mq.queueId && m.queueOffset < offset);
        if (!same) kept.push_back(std::move(m));
    }
    localBuffer_.swap(kept);
}

void DefaultLitePullConsumer::seekToBegin(const MessageQueue& mq) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    seek(mq, mqClient_->getMinOffset(mq));
}

void DefaultLitePullConsumer::seekToEnd(const MessageQueue& mq) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    seek(mq, mqClient_->getMaxOffset(mq));
}

int64_t DefaultLitePullConsumer::committed(const MessageQueue& mq) {
    if (mqClient_ == nullptr) return -1;
    try {
        int64_t off = 0;
        if (mqClient_->queryConsumerOffset(consumerGroup_, mq, off)) return off;
    } catch (...) {
    }
    return -1;
}

void DefaultLitePullConsumer::commit() {
    if (mqClient_ == nullptr) return;  // 未启动 / 已 shutdown：无可提交位点
    int64_t now = nowMillis();
    for (const auto& kv : nextOffset_) {
        try {
            mqClient_->updateConsumerOffset(consumerGroup_, kv.first, kv.second);
            lastCommit_[kv.first] = now;
        } catch (...) {
            logger_debug("lite commit failed for " + kv.first.toString());
        }
    }
}

int64_t DefaultLitePullConsumer::offsetForTimestamp(const MessageQueue& mq, int64_t timestamp) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    return mqClient_->searchOffsetByTimestamp(mq, timestamp);
}

// ---------------------------------------------------------------- 队列查询 / 控制
std::vector<MessageQueue> DefaultLitePullConsumer::fetchMessageQueues(const std::string& topic) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    std::shared_ptr<TopicPublishInfo> info = mqClient_->getTopicPublishInfo(withNamespace(topic));
    return info->msgQueueList;
}

std::vector<MessageQueue> DefaultLitePullConsumer::assignment() {
    return std::vector<MessageQueue>(assigned_.begin(), assigned_.end());
}

void DefaultLitePullConsumer::pause(const std::vector<MessageQueue>& messageQueues) {
    paused_.insert(messageQueues.begin(), messageQueues.end());
}

void DefaultLitePullConsumer::resume(const std::vector<MessageQueue>& messageQueues) {
    for (const MessageQueue& mq : messageQueues) {
        paused_.erase(mq);
    }
}

// ---------------------------------------------------------------- 心跳
HeartbeatData DefaultLitePullConsumer::buildHeartbeat() const {
    HeartbeatData hb(clientId_);
    ConsumerData cd(consumerGroup_, ConsumeType::CONSUME_PASSIVELY, messageModel_,
                    consumeFromWhere_);
    for (const auto& kv : subscriptionData_) {
        cd.subscriptionDataSet.push_back(kv.second);
    }
    hb.consumerDataSet.push_back(cd);
    return hb;
}

int32_t DefaultLitePullConsumer::sendHeartbeatToAllBroker() {
    if (mqClient_ == nullptr) return 0;
    HeartbeatData hb = buildHeartbeat();
    int32_t ok = 0;
    for (const std::string& addr : mqClient_->getRouteOfAllBrokers()) {
        try {
            mqClient_->sendHeartbeat(addr, hb, 5000);
            ++ok;
        } catch (...) {
            logger_debug("lite heartbeat to " + addr + " failed");
        }
    }
    return ok;
}

void DefaultLitePullConsumer::heartbeatLoop() {
    while (running_) {
        try {
            sendHeartbeatToAllBroker();
        } catch (...) {
            logger_debug("lite heartbeat loop error");
        }
        // 5s 心跳间隔（与 push 消费者一致）
        for (int i = 0; i < 50; ++i) {
            if (!running_) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
}

}  // namespace rocketmq
