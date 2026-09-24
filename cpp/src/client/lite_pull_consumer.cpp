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
    // 对应 Java DefaultLitePullConsumer 的字段初值 new AllocateMessageQueueAveragely()。
    allocateStrategy_ = std::make_shared<AllocateMessageQueueAveragely>();
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

void DefaultLitePullConsumer::setAllocateMessageQueueStrategy(
    std::shared_ptr<AllocateMessageQueueStrategy> strategy) {
    // 与 Java 一致：setter 不校验，null 由 start() 的 checkConfig 拒绝。
    allocateStrategy_ = std::move(strategy);
}

std::shared_ptr<AllocateMessageQueueStrategy>
DefaultLitePullConsumer::allocateMessageQueueStrategy() const {
    return allocateStrategy_;
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
    std::set<MessageQueue> queues(messageQueues.begin(), messageQueues.end());
    std::vector<MessageQueue> toResolve;
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        assignMode_ = true;
        // Java assignedMessageQueue.updateAssignedMessageQueue：撤掉的队列连着整份
        // MessageQueueState 丢掉（拉取游标与已消费游标一起消失）。**不**动 offsetTable_——
        // 那份清理挂在 rebalance 的 removeUnnecessaryMessageQueue 上，assign 模式不走
        // rebalance，所以 persist 也发生在这里之外（Java 同）。
        for (const MessageQueue& mq : assigned_) {
            if (queues.find(mq) == queues.end()) {
                nextOffset_.erase(mq);
                consumeOffset_.erase(mq);
            }
        }
        assigned_ = queues;
        for (const MessageQueue& mq : assigned_) {
            if (nextOffset_.find(mq) == nextOffset_.end()) toResolve.push_back(mq);
        }
    }
    // resolveInitialOffset 要发 RPC，绝不能抱着 stateMutex_ 做。
    for (const MessageQueue& mq : toResolve) {
        int64_t offset = 0;
        try {
            offset = resolveInitialOffset(mq);
        } catch (...) {
            logger_debug("lite assign: resolve initial offset failed for " + mq.toString());
            continue;
        }
        std::lock_guard<std::mutex> lk(stateMutex_);
        // 期间可能被 rebalance/assign 改过：只给当下还持有的队列写回游标。
        if (assigned_.find(mq) != assigned_.end() && nextOffset_.find(mq) == nextOffset_.end()) {
            nextOffset_[mq] = offset;
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
    // 对应 Java DefaultLitePullConsumerImpl.checkConfig(:435)：策略为 null 直接拒绝启动，
    // 而不是等 rebalance 解引用空指针（UB）。
    if (allocateStrategy_ == nullptr) {
        throw MQClientException("allocateMessageQueueStrategy is null");
    }
    if (subscription_.empty() && !assignMode_) {
        throw MQClientException("subscription is not set, call subscribe() or assign() first");
    }
    // 对应 Java DefaultMQPushConsumerImpl.checkConfig（:1058）：启动即无条件校验，
    // 而不是等到算起点时抛出、被下面的 catch(...) 吞掉后静默退化成从 max offset 消费。
    parseConsumeTimestamp(consumeTimestamp_);
    // Java `DefaultLitePullConsumerImpl#start`:287-289：CLUSTERING 才改写 instanceName，
    // clientId 口径是 `ClientConfig#buildMQClientId` 的 `<本机 IP>@<instanceName>`。
    if (messageModel_ == MessageModel::CLUSTERING) {
        instanceName_ = changeInstanceNameToPID(instanceName_);
    }
    if (clientId_.empty()) {
        clientId_ = buildClientId(instanceName_, unitName_, enableStreamRequestType_);
    }
    // 轻量消费者默认开着 stream（Java `DefaultLitePullConsumer:213/228` 构造函数里置真），
    // unitName 只影响动态取址 URL。
    mqClient_.reset(new MQClientInstance(clientId_, nameServerAddrs_,
                                        /*connectTimeoutMillis=*/3000,
                                        /*invokeTimeoutMillis=*/10000,
                                        MQClientInstance::tlsEnabledFromEnv(), unitName_,
                                        pollNameServerIntervalMillis_));
    // 请求钩子（ACL 签名 / stream 的 `ReqT`）：lite 消费者在 Java 里**默认**开 stream
    //（DefaultLitePullConsumer:213/228），所以这里必须走 composeRequestHooks 把
    // StreamTypeRPCHook 排在用户钩子之前 —— 直接注册 rpcHook_ 会让 ReqT 漏发，
    // 而且开 ACL 时签的内容与上线的字段不一致。
    // 绑定位置也关键：Java 的 rpcHook 在 MQClientAPIImpl 构造时就传进去了，实例发出的
    // 第一笔报文（下面的 start() 动态取址、路由刷新）就带着它；放到 start() 之后，
    // 首包就是裸的。
    const std::shared_ptr<RPCHook> requestHook =
        composeRequestHooks(enableStreamRequestType_, rpcHook_);
    if (requestHook && !mqClient_->registerRPCHook(requestHook)) {
        logger_warn("lite pull consumer rpc hook ignored: MQClientInstance already has one (clientId="
                    + clientId_ + ")");
    }
    mqClient_->start();
    // 登记在用 topic，并**同步**把路由拉进来：心跳只发给「实例路由表里已知的 broker」，
    // 自建实例此刻路由表还是空的，那一轮会发 0 份 → 订阅要等 5s 心跳循环第一轮才注册上
    // broker，同组多实例时首轮 rebalance 会各自独占全部队列。
    // （对应 Python `_refresh_route_for_heartbeat` / Java 注册心跳前的 updateTopicRouteInfo）
    std::set<std::string> routeTopics;
    for (const auto& kv : subscription_) {
        routeTopics.insert(kv.first);
    }
    for (const MessageQueue& mq : assigned_) {
        routeTopics.insert(mq.topic);
    }
    for (const std::string& t : routeTopics) {
        mqClient_->registerTopicInUse(t);
        try {
            mqClient_->getTopicPublishInfo(t);
        } catch (const std::exception& e) {
            logger_debug("lite start: refresh route for " + t + " failed: " + e.what());
        }
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
    // running_ 必须在心跳线程起来**之前**置位：heartbeatLoop 的循环条件是它，
    // 线程先跑起来会看到 false 直接退出，本实例就再也不会周期心跳（broker 120s 后踢掉）。
    running_ = true;
    heartbeatThread_ = std::thread(&DefaultLitePullConsumer::heartbeatLoop, this);
    started_ = true;
    pullThread_ = std::thread(&DefaultLitePullConsumer::pullServiceLoop, this);
}

void DefaultLitePullConsumer::shutdown() {
    if (!started_) return;
    started_ = false;
    running_ = false;
    // Java 的 shutdown 走 persistConsumerOffset()：把内存位点表按当下持有的队列刷一遍，
    // 与 autoCommit 无关（手动模式用 persist=false 攒下的值同样要落盘）。
    // 自动提交模式再多走一步 commit()：本端口没有 Java 那份 5s 定时器，
    // "poll 交出去但还没到截止时刻"的位点得在这里补上，否则重启后从上一格重投。
    try {
        if (autoCommit_) {
            commit();
        } else {
            std::set<MessageQueue> scope;
            {
                std::lock_guard<std::mutex> lk(stateMutex_);
                scope = assigned_;
            }
            persistOffsetTable(scope);
        }
    } catch (...) {
        logger_debug("lite shutdown commit failed");
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
            // assigned_ / paused_ 会被调用方线程的 assign()、pause() 改，先快照再遍历。
            std::set<MessageQueue> snapshot;
            std::set<MessageQueue> paused;
            {
                std::lock_guard<std::mutex> lk(stateMutex_);
                snapshot = assigned_;
                paused = paused_;
            }
            for (const MessageQueue& mq : snapshot) {
                if (!running_) break;
                if (paused.find(mq) != paused.end()) continue;
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
    int64_t offset = 0;
    bool known = false;
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        auto it = nextOffset_.find(mq);
        if (it != nextOffset_.end()) {
            offset = it->second;
            known = true;
        }
    }
    if (!known) {
        // 首次拉取要问 broker 起点，这一步是 RPC：绝不能抱着 stateMutex_ 做。
        int64_t resolved = 0;
        try {
            resolved = resolveInitialOffset(mq);
        } catch (...) {
            return false;
        }
        std::lock_guard<std::mutex> lk(stateMutex_);
        auto it = nextOffset_.find(mq);
        if (it == nextOffset_.end()) {
            nextOffset_[mq] = resolved;
            offset = resolved;
        } else {
            offset = it->second;
        }
    }
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
            {
                // 只推进拉取游标。「已消费游标」是 poll() 交付时才写的，两者不是一条线：
                // 缓冲里压着没交出去的消息不能算已消费（Java processQueue.removeMessage 同口径）。
                std::lock_guard<std::mutex> lk(stateMutex_);
                nextOffset_[mq] = msgs.back().queueOffset + 1;
            }
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

void DefaultLitePullConsumer::maybeAutoCommit() {
    // 对位 Java DefaultLitePullConsumerImpl#maybeAutoCommit：只有一道全局截止时刻，
    // 到点提交**全部**已分配队列（Java 的 commitAll 也是遍历 assignedMessageQueue），
    // 然后把截止时刻推到 now + autoCommitIntervalMillis_。初值 -1 ⇒ 第一次检查就提交一次，
    // 那次手上还没有任何交付记录（游标是 -1），所发出去的仍是空操作。
    //
    // 调用点只有两个，和 Java 一致：poll() 开头，以及 shutdown()。Java 里空闲消费者
    // 靠 MQClientInstance 每 5s 的 persistConsumerOffset 定时器兜底，本端口没挂那个定时器
    // （见 commitOffsets 的偏离①），所以停 poll 之后到 shutdown 之前不会自动落位点。
    // 拉取循环里**不**查这道闸：那会让本端口在没人 poll 时也往前提交，语义比 Java 激进。
    const int64_t now = nowMillis();
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        if (now < nextAutoCommitDeadline_) return;
        nextAutoCommitDeadline_ = now + autoCommitIntervalMillis_;
    }
    commit();
}

int64_t DefaultLitePullConsumer::resolveInitialOffset(const MessageQueue& mq) {
    // 未启动时 mqClient_ 为空：Java 允许 start 前 assign（位点推迟到 start 时再解析），
    // 这里必须显式抛错而不是解引用空指针（调用方 assign/rebalance 均已兜住）。
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        auto sit = seekOffset_.find(mq);
        if (sit != seekOffset_.end()) return sit->second;
    }
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
        // Java RebalanceImpl.rebalanceByTopic 在分配前 Collections.sort(mqAll) + sort(cidAll)：
        // 顺序不一致会让同组不同实例算出冲突的分配（同一队列被两个实例同时消费）。
        std::sort(mqAll.begin(), mqAll.end());
        std::vector<std::string> cidAll =
            mqClient_->getConsumerIdListByGroup(kv.first, consumerGroup_);
        if (std::find(cidAll.begin(), cidAll.end(), clientId_) == cidAll.end()) {
            cidAll.push_back(clientId_);
        }
        std::sort(cidAll.begin(), cidAll.end());
        std::vector<MessageQueue> allocated;
        // Java RebalanceImpl#rebalanceByTopic 的 catch (Throwable) 直接 return，位置在
        // updateProcessQueueTableInRebalance **之前** → 一次分配异常不该把队列撤走。
        // 所以这里回退成「沿用本 topic 当前的分配」（Python / Rust 同口径）。
        try {
            allocated = allocateStrategy_->allocate(consumerGroup_, clientId_, mqAll, cidAll);
        } catch (const std::exception& e) {
            logger_error("allocate message queue exception. strategy name: "
                         + allocateStrategy_->getName() + ", ex: " + e.what());
            std::lock_guard<std::mutex> lk(stateMutex_);
            for (const MessageQueue& mq : assigned_) {
                if (mq.topic == kv.first) allocated.push_back(mq);
            }
        }
        newSet.insert(allocated.begin(), allocated.end());
    }
    std::set<MessageQueue> old;
    std::vector<MessageQueue> toResolve;
    std::vector<std::pair<MessageQueue, int64_t>> revoked;
    bool changed = false;
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        changed = (newSet != assigned_);
        if (changed) {
            old = assigned_;
            assigned_ = newSet;
            for (const MessageQueue& mq : newSet) {
                if (nextOffset_.find(mq) == nextOffset_.end()) toResolve.push_back(mq);
            }
            for (const MessageQueue& mq : old) {
                if (newSet.find(mq) == newSet.end()) {
                    // Java RebalanceLitePullImpl#removeUnnecessaryMessageQueue：先 persist(mq)
                    // 再 removeOffset(mq)。persist 是 RPC，所以这里只把要补发的队列记下来，
                    // 抱着锁做网络会把整条 poll/commit 路径卡住（下面统一发）。
                    auto ot = offsetTable_.find(mq);
                    if (ot != offsetTable_.end()) revoked.push_back(*ot);
                    nextOffset_.erase(mq);
                    // AssignedMessageQueue 的条目（连着 consumeOffset）一起丢掉：
                    // 留着就是一个再没人提交的陈旧值。
                    consumeOffset_.erase(mq);
                    offsetTable_.erase(mq);
                    seekOffset_.erase(mq);
                }
            }
        }
    }
    if (!changed) return;
            // 撤手之前把最后那次提交补发出去，别让新持有者从上一个窗口起重新投一遍。
    for (const auto& kv : revoked) {
        try {
            mqClient_->updateConsumerOffset(consumerGroup_, kv.first, kv.second);
        } catch (...) {
            logger_debug("lite persist on revoke failed for " + kv.first.toString());
        }
    }
    for (const MessageQueue& mq : toResolve) {
        int64_t offset = 0;
        try {
            offset = resolveInitialOffset(mq);
        } catch (...) {
            logger_debug("lite rebalance: resolve offset failed for " + mq.toString());
            continue;
        }
        std::lock_guard<std::mutex> lk(stateMutex_);
        if (assigned_.find(mq) != assigned_.end() && nextOffset_.find(mq) == nextOffset_.end()) {
            nextOffset_[mq] = offset;
        }
    }
    if (messageQueueListener_ != nullptr) {
        try {
            messageQueueListener_->messageQueueChanged(mqAllOfSubscription(), newSetAsVector(newSet));
        } catch (...) {
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
    // Java poll() 进来先按截止时刻试一次自动提交：拿缓冲锁之前做，提交要发 RPC，
    // 抱着 bufferMutex_ 等网络会把 enqueue() 一起卡住。
    if (autoCommit_) maybeAutoCommit();
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout);
    std::vector<MessageExt> out;
    {
        std::unique_lock<std::mutex> lk(bufferMutex_);
        while (localBuffer_.empty()) {
            auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 deadline - std::chrono::steady_clock::now())
                                 .count();
            if (remaining <= 0) return {};
            bufferCv_.wait_for(lk, std::chrono::milliseconds(remaining));
        }
        while (!localBuffer_.empty() && out.size() < 1024) {
            out.push_back(std::move(localBuffer_.front()));
            localBuffer_.pop_front();
        }
    }
    advanceConsumeOffset(out);
    return out;
}

void DefaultLitePullConsumer::advanceConsumeOffset(const std::vector<MessageExt>& msgs) {
    if (msgs.empty()) return;
    std::lock_guard<std::mutex> lk(stateMutex_);
    // 对位 Java poll()：消息交到调用方手上才推进「已消费游标」
    // （assignedMessageQueue.updateConsumeOffset(mq, processQueue.removeMessage(msgs))）。
    // 只认当下还持有（有拉取游标）的队列——别的实例刚被分走的队列不归我们记账；
    // 同一次交付里按 offset 最大的那条定游标（缓冲可能交错混着几条队列）。
    for (const MessageExt& m : msgs) {
        const MessageQueue mq(m.topic, m.brokerName, m.queueId);
        if (nextOffset_.find(mq) == nextOffset_.end()) continue;
        const int64_t next = m.queueOffset + 1;
        auto it = consumeOffset_.find(mq);
        if (it == consumeOffset_.end() || next > it->second) consumeOffset_[mq] = next;
    }
}

void DefaultLitePullConsumer::seek(const MessageQueue& mq, int64_t offset) {
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        seekOffset_[mq] = offset;
        nextOffset_[mq] = offset;
        // Java nextPullOffset()：吃掉 seekOffset 时连同 consumeOffset 一起改写，
        // 否则重放的那一段会被上一格的已提交位点盖过去（"跳回去"意味着"那里之前都还没消费"）。
        consumeOffset_[mq] = offset;
    }
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
    // Java committed() 走 offsetStore.readOffset(mq, MEMORY_FIRST_THEN_STORE)：
    // 先看内存位点表（persist=false 刚提交、还没发给 broker 的值也算数），
    // 表里没有再问 broker，并把 broker 的值回填进表里（Java 同一处也回填）。
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        auto it = offsetTable_.find(mq);
        if (it != offsetTable_.end()) return it->second;
    }
    if (mqClient_ == nullptr) return -1;
    try {
        int64_t off = 0;
        if (mqClient_->queryConsumerOffset(consumerGroup_, mq, off)) {
            std::lock_guard<std::mutex> lk(stateMutex_);
            offsetTable_[mq] = off;
            return off;
        }
    } catch (...) {
    }
    return -1;
}

void DefaultLitePullConsumer::persistOffset(const MessageQueue& mq) {
    // Java OffsetStore#persist(mq)：只把这一条队列的内存位点发给 broker，不做清理。
    int64_t offset = 0;
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        auto it = offsetTable_.find(mq);
        if (it == offsetTable_.end()) return;
        offset = it->second;
    }
    if (mqClient_ == nullptr) return;
    try {
        mqClient_->updateConsumerOffset(consumerGroup_, mq, offset);
    } catch (const std::exception& e) {
        logger_debug("lite persist failed for " + mq.toString() + ": " + e.what());
    } catch (...) {
        logger_debug("lite persist failed for " + mq.toString());
    }
}

void DefaultLitePullConsumer::persistOffsetTable(const std::set<MessageQueue>& mqs) {
    // Java RemoteBrokerOffsetStore#persistAll(Set)：内存位点表里落在 mqs 上的那部分写给
    // broker，**不在**其中的条目顺手从表里删掉（Java 日志里那句 remove unused mq）。
    // 后半句是 Java 的真实行为：这张表只服务于当下持有的队列，撤走的队列留在表里没人再
    // 提交，persistAll 一路扫过去就清掉。代价是 commit(部分队列, persist=true) 会把其余
    // 队列**尚未落盘**的内存值一起丢掉——要提交谁就一次给全。
    if (mqs.empty()) return;
    std::vector<std::pair<MessageQueue, int64_t>> toSend;
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        for (auto it = offsetTable_.begin(); it != offsetTable_.end();) {
            if (mqs.find(it->first) == mqs.end()) {
                it = offsetTable_.erase(it);
                continue;
            }
            toSend.push_back(*it);
            ++it;
        }
    }
    if (mqClient_ == nullptr) return;
    for (const auto& kv : toSend) {
        try {
            mqClient_->updateConsumerOffset(consumerGroup_, kv.first, kv.second);
        } catch (...) {
            logger_debug("lite persist failed for " + kv.first.toString());
        }
    }
}

void DefaultLitePullConsumer::commit() {
    // Java commit() → commitAll()：按「已消费游标」提交所有已分配队列。
    std::map<MessageQueue, int64_t> targets;
    std::set<MessageQueue> scope;
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        scope = assigned_;
        for (const MessageQueue& mq : assigned_) {
            auto it = consumeOffset_.find(mq);
            targets[mq] = (it == consumeOffset_.end()) ? -1 : it->second;
        }
    }
    commitOffsets(targets, scope, true);
}

void DefaultLitePullConsumer::commit(const std::map<MessageQueue, int64_t>& offsets,
                                    bool persist) {
    if (offsets.empty()) {
        // Java commit(Map) 原文：空集合只记一条 warn 就 return，
        // **不**碰 offsetStore，所以上一轮 persist=false 攒下的内存值也原样保留。
        logger_warn("MessageQueues is empty, Ignore this commit ");
        return;
    }
    std::set<MessageQueue> scope;
    for (const auto& kv : offsets) scope.insert(kv.first);
    commitOffsets(offsets, scope, persist);
}

void DefaultLitePullConsumer::commit(const std::set<MessageQueue>& messageQueues, bool persist) {
    // Java commit(Set, persist)：集合为空直接 return（连 warn 都没有）。
    if (messageQueues.empty()) return;
    std::map<MessageQueue, int64_t> targets;
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        for (const MessageQueue& mq : messageQueues) {
            auto it = consumeOffset_.find(mq);
            targets[mq] = (it == consumeOffset_.end()) ? -1 : it->second;
        }
    }
    commitOffsets(targets, messageQueues, persist);
}

void DefaultLitePullConsumer::commitOffsets(const std::map<MessageQueue, int64_t>& targets,
                                           const std::set<MessageQueue>& scope, bool persist) {
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        for (const auto& kv : targets) {
            if (kv.second == -1) {
                // Java commitAll 原文：这条队列还没消费过，记 error 并跳过，
                // 绝不能把 -1 写进 broker（位点变 -1 会让下次消费从队首重投全量）。
                logger_error("consumerOffset is -1 in messageQueue [" + kv.first.toString() + "].");
                continue;
            }
            if (assigned_.find(kv.first) == assigned_.end()) {
                // Java 的 processQueue != nullptr && !isDropped() 守卫：不是本实例持有的
                // 队列一律不替它提交，静默跳过（Java 原文这里连日志都没有）。
                continue;
            }
            offsetTable_[kv.first] = kv.second;
        }
    }
    // persistAll 只认 scope 里的队列：表里其余条目会被清掉（Java 同一处）。
    // 两处与 Java 的偏离：① Java 的 commitAll 只写内存表，真正发给 broker 靠 MQClientInstance
    // 每 persistConsumerOffsetInterval(5s) 一次的定时器，本端口的 lite 消费者没挂那个定时器，
    // 所以 persist=true（默认）就地发出去；② Java 的 persistAll 用 oneway、异常只记日志，
    // 这里发同步带应答，坏位点当场就能从日志看到。
    if (persist) persistOffsetTable(scope);
}

int64_t DefaultLitePullConsumer::offsetForTimestamp(const MessageQueue& mq, int64_t timestamp) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    return mqClient_->searchOffsetByTimestamp(mq, timestamp);
}

int64_t DefaultLitePullConsumer::pullCursorOf(const MessageQueue& mq) {
    std::lock_guard<std::mutex> lk(stateMutex_);
    auto it = nextOffset_.find(mq);
    return it == nextOffset_.end() ? -1 : it->second;
}

int64_t DefaultLitePullConsumer::consumeCursorOf(const MessageQueue& mq) {
    std::lock_guard<std::mutex> lk(stateMutex_);
    auto it = consumeOffset_.find(mq);
    return it == consumeOffset_.end() ? -1 : it->second;
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
    std::lock_guard<std::mutex> lk(stateMutex_);
    return std::vector<MessageQueue>(assigned_.begin(), assigned_.end());
}

void DefaultLitePullConsumer::pause(const std::vector<MessageQueue>& messageQueues) {
    std::lock_guard<std::mutex> lk(stateMutex_);
    paused_.insert(messageQueues.begin(), messageQueues.end());
}

void DefaultLitePullConsumer::resume(const std::vector<MessageQueue>& messageQueues) {
    std::lock_guard<std::mutex> lk(stateMutex_);
    for (const MessageQueue& mq : messageQueues) {
        paused_.erase(mq);
    }
}

// ---------------------------------------------------------------- 心跳
HeartbeatData DefaultLitePullConsumer::buildHeartbeat() const {
    HeartbeatData hb(clientId_);
    ConsumerData cd(consumerGroup_, ConsumeType::CONSUME_PASSIVELY, messageModel_,
                    consumeFromWhere_);
    // Java `MQClientInstance:1039`：心跳里的 ConsumerData.unitMode 决定 broker 建
    // %RETRY% topic 时打不打 UNIT_SUB 位
    cd.unitMode = unitMode_;
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
