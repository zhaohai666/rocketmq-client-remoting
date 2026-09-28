// 轻量拉取消费者的**拉取游标**（#105）离线单测 —— 假 namesrv + 假 broker（真 socket），
// 不碰真集群。
//
// 对齐基准（Java 5.5.1）：DefaultLitePullConsumerImpl#PullTaskImpl.run:982-998 —— 一轮
// 拉取**成功返回**之后，无论 FOUND / NO_NEW_MSG / NO_MATCHED_MSG / OFFSET_ILLEGAL，都把
// 拉取游标推进到 pullResult.getNextBeginOffset()（:998 updatePullOffset）；唯一的刹车是
// 「这轮里刚 seek 过」（:808 的 getSeekOffset(mq) == -1 检查）与「队列已被撤走」
// （AssignedMessageQueue.updatePullOffset:82-91 的 processQueue 身份比对），FOUND 分支的
// 入缓冲（:986）挂的是同一只刹车。
//
// 为什么必须离线锁死：旧实现只在 FOUND 时用 `msgs.back().queueOffset + 1` 推进游标，于是
// broker 回 NO_MATCHED_MSG（本轮扫过的整段都不匹配）时游标原地不动 —— 下一轮从同一位点
// 把同一段重扫一遍，永远打转；OFFSET_ILLEGAL 的纠正值也吃不到，越界不自愈。真机窗口里
// 两者都表现为「消费者活着但永远收不到消息」，很难归因（真机另有一条链路：
// examples/live_lite_pull.cpp 的 S4/S5/S6 腿）。
//
// 判据取自**线上报文**：假 broker 按脚本回包，并从 socket 上取回每笔 PULL_MESSAGE 的
// extFields —— 「游标真的推到 5 了」看的是下一笔请求是不是从 5 起，而不是只看内存表。
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/protocol/route.h"

using namespace rocketmq;

namespace {

using netcompat::kInvalidSocket;
using netcompat::socket_t;
using netcompat::socklen_type;

#ifdef _WIN32
constexpr int kShutdownHow = SD_BOTH;
#else
constexpr int kShutdownHow = SHUT_RDWR;
#endif

int fails = 0;
int checks = 0;

void expect(bool ok, const std::string& name, const std::string& detail = "") {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("FAIL %s %s\n", name.c_str(), detail.c_str());
    }
}

void expectInt(long long actual, long long expected, const std::string& name) {
    ++checks;
    if (actual != expected) {
        std::printf("FAIL %s (actual=%lld expected=%lld)\n", name.c_str(), actual, expected);
    }
}

// ---------------------------------------------------------------- socket 工具

bool readN(socket_t s, char* buf, size_t n) {
    size_t got = 0;
    while (got < n) {
        int r = static_cast<int>(::recv(s, buf + got, static_cast<int>(n - got), 0));
        if (r <= 0) return false;
        got += static_cast<size_t>(r);
    }
    return true;
}

bool writeAll(socket_t s, const Bytes& data) {
    size_t sent = 0;
    while (sent < data.size()) {
        int n = static_cast<int>(::send(s, reinterpret_cast<const char*>(data.data() + sent),
                                        static_cast<int>(data.size() - sent),
                                        netcompat::sendFlags()));
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

bool readFrame(socket_t s, const std::atomic<bool>& alive, Bytes& out) {
    for (;;) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(s, &rfds);
        timeval tv;
        tv.tv_sec = 0;
        tv.tv_usec = 100 * 1000;
        int ready = ::select(netcompat::selectNfds(s), &rfds, nullptr, nullptr, &tv);
        if (!alive.load()) return false;
        if (ready == 0) continue;
        if (ready < 0) return false;
        break;
    }
    char lenBuf[4];
    if (!readN(s, lenBuf, 4)) return false;
    Bytes lenStr(lenBuf, 4);
    const int32_t totalLen = ByteReader::getInt32At(lenStr, 0);
    if (totalLen <= 0 || totalLen > 8 * 1024 * 1024) return false;
    Bytes body(static_cast<size_t>(totalLen), '\0');
    if (!readN(s, reinterpret_cast<char*>(body.data()), static_cast<size_t>(totalLen))) {
        return false;
    }
    out = lenStr + body;
    return true;
}

// ---------------------------------------------------------------- 假端点

// 一笔脚本化的 PULL_MESSAGE 应答。
struct PullReply {
    int32_t code = ResponseCode::PULL_NOT_FOUND;
    PropertyMap ext;
    Bytes body;
};

// 同时扮演 name server（回路由）与 broker（回 QUERY_CONSUMER_OFFSET / PULL_MESSAGE）。
class MockEndpoint {
public:
    MockEndpoint() {
        netcompat::ensureInitialized();
        listen_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(listen_, SOL_SOCKET, SO_REUSEADDR,
                     reinterpret_cast<const char*>(&one), static_cast<int>(sizeof(one)));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        ::bind(listen_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ::listen(listen_, 16);
        socklen_type len = sizeof(addr);
        ::getsockname(listen_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        running_.store(true);
        acceptor_ = std::thread([this]() { acceptLoop(); });
    }

    ~MockEndpoint() {
        running_.store(false);
        {
            // 闸没放行的话先放掉，免得工作线程卡在 wait 上拖住析构。
            std::lock_guard<std::mutex> lk(gateMutex_);
            gateOpen_ = true;
            gateCv_.notify_all();
        }
        if (acceptor_.joinable()) acceptor_.join();
        for (;;) {
            std::vector<std::thread> pending;
            {
                std::lock_guard<std::mutex> lk(workersM_);
                pending.swap(workers_);
            }
            if (pending.empty()) break;
            for (auto& t : pending) {
                if (t.joinable()) t.join();
            }
        }
        netcompat::closeSocket(listen_);
    }

    MockEndpoint(const MockEndpoint&) = delete;
    MockEndpoint& operator=(const MockEndpoint&) = delete;

    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }

    void addRoute(const std::string& topic, const TopicRouteData& route) {
        std::lock_guard<std::mutex> lk(state_);
        routes_[topic] = route.encode();
    }

    /// 排一笔 PULL_MESSAGE 应答（FIFO 命中，每笔只回一次）。
    void scriptPull(int32_t code, const PropertyMap& ext, const Bytes& body = Bytes()) {
        std::lock_guard<std::mutex> lk(state_);
        PullReply reply;
        reply.code = code;
        reply.ext = ext;
        reply.body = body;
        pullScripts_.push_back(reply);
    }

    /// 给下一笔 PULL_MESSAGE 挂发车闸：应答先等 releasePullGate()。用来把「应答还在路上、
    /// 客户端先 seek 了」这段在途窗口拉成确定性的。
    void gateNextPull() {
        std::lock_guard<std::mutex> lk(gateMutex_);
        gateArmed_ = true;
        gateOpen_ = false;
    }

    void releasePullGate() {
        std::lock_guard<std::mutex> lk(gateMutex_);
        gateOpen_ = true;
        gateCv_.notify_all();
    }

    size_t pullCount() {
        std::lock_guard<std::mutex> lk(state_);
        return pulls_.size();
    }

    PropertyMap pullExt(size_t i) {
        std::lock_guard<std::mutex> lk(state_);
        if (i >= pulls_.size()) return PropertyMap();
        return pulls_[i];
    }

    int32_t pullCode(size_t i) {
        std::lock_guard<std::mutex> lk(state_);
        if (i >= pullCodes_.size()) return -1;
        return pullCodes_[i];
    }

    /// 有没有任何一笔 PULL_MESSAGE 是从 `offset` 起的。
    bool pullHasOffset(int64_t offset) {
        std::lock_guard<std::mutex> lk(state_);
        for (const PropertyMap& ext : pulls_) {
            auto it = ext.find("queueOffset");
            if (it != ext.end() && extInt(ext, "queueOffset", -1) == offset) return true;
        }
        return false;
    }

    /// 收到过该请求码吗（GET_MIN_OFFSET 那类「不该发的报文」断言靠它）。
    bool sawRequest(int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        for (int32_t c : codes_) {
            if (c == code) return true;
        }
        return false;
    }

    static int64_t extInt(const PropertyMap& ext, const std::string& key, int64_t fallback) {
        auto it = ext.find(key);
        if (it == ext.end()) return fallback;
        try {
            return std::stoll(it->second);
        } catch (...) {
            return fallback;
        }
    }

private:
    void acceptLoop() {
        while (running_.load()) {
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(listen_, &rfds);
            timeval tv;
            tv.tv_sec = 0;
            tv.tv_usec = 100 * 1000;
            int ready = ::select(netcompat::selectNfds(listen_), &rfds, nullptr, nullptr, &tv);
            if (ready == 0) continue;
            if (ready < 0) break;
            socket_t sock = ::accept(listen_, nullptr, nullptr);
            if (sock == kInvalidSocket) break;
            std::lock_guard<std::mutex> lk(workersM_);
            workers_.emplace_back([this, sock]() { serveConn(sock); });
        }
    }

    void serveConn(socket_t sock) {
        Bytes frame;
        while (running_.load() && readFrame(sock, running_, frame)) {
            RemotingCommand req;
            if (!RemotingCommand::tryDecode(frame, req, nullptr)) break;
            RemotingCommand resp;
            resp.opaque = req.opaque;
            resp.markResponseType();
            if (req.code == RequestCode::GET_ROUTEINFO_BY_TOPIC) {
                const std::string topic =
                    req.extFields.count("topic") ? req.extFields.at("topic") : std::string();
                std::lock_guard<std::mutex> lk(state_);
                codes_.push_back(req.code);
                auto it = routes_.find(topic);
                if (it != routes_.end()) {
                    resp.code = ResponseCode::SUCCESS;
                    resp.body = it->second;
                    resp.hasBody = true;
                } else {
                    resp.code = ResponseCode::TOPIC_NOT_EXIST;
                }
            } else if (req.code == RequestCode::PULL_MESSAGE) {
                answerPull(req, resp);
            } else if (req.code == RequestCode::QUERY_CONSUMER_OFFSET) {
                // 组从未提交过位点：真实 broker 回 QUERY_NOT_FOUND（21/22 号里的 22），
                // 客户端据此走 consumeFromWhere 分支算起点。
                std::lock_guard<std::mutex> lk(state_);
                codes_.push_back(req.code);
                resp.code = ResponseCode::QUERY_NOT_FOUND;
            } else {
                std::lock_guard<std::mutex> lk(state_);
                codes_.push_back(req.code);
                resp.code = ResponseCode::SUCCESS;
            }
            Bytes out = resp.encode();
            if (!writeAll(sock, out)) break;
        }
        ::shutdown(sock, kShutdownHow);
        netcompat::closeSocket(sock);
    }

    /// PULL_MESSAGE 的应答：排了脚本就按脚本回（挂了闸先等闸），否则按**空 broker** 的
    /// 忠实形态回 PULL_NOT_FOUND + nextBeginOffset = 请求 queueOffset（Java
    /// PullMessageProcessor#composeResponseHeader 对 NO_NEW_MSG 同样回填 nextBeginOffset；
    /// min/max 为 0）。客户端每轮都信 nextBeginOffset，这里不忠实就会把游标推到 0。
    ///
    /// 请求**先于**发车闸记账：闸把「应答还在路上」这段窗口拉长时，pullCount() 必须立刻
    /// 可见，否则「等第一笔到达」的用例会在闸前空等。pullCodes_ 记的是**应答**码。
    void answerPull(const RemotingCommand& req, RemotingCommand& resp) {
        {
            std::lock_guard<std::mutex> lk(state_);
            codes_.push_back(req.code);
            pulls_.push_back(req.extFields);
        }
        bool gated = false;
        {
            std::lock_guard<std::mutex> lk(gateMutex_);
            if (gateArmed_) {
                gateArmed_ = false;
                gated = true;
            }
        }
        if (gated) {
            std::unique_lock<std::mutex> lk(gateMutex_);
            // 5s 兜底：用例失败时也不能把工作线程永久卡住（析构会等它）。
            gateCv_.wait_for(lk, std::chrono::seconds(5), [this] { return gateOpen_; });
        }
        PullReply reply;
        bool hasScript = false;
        {
            std::lock_guard<std::mutex> lk(state_);
            if (!pullScripts_.empty()) {
                reply = pullScripts_.front();
                pullScripts_.pop_front();
                hasScript = true;
            }
        }
        if (!hasScript) {
            reply.code = ResponseCode::PULL_NOT_FOUND;
            reply.ext["nextBeginOffset"] =
                std::to_string(extInt(req.extFields, "queueOffset", 0));
            reply.ext["minOffset"] = "0";
            reply.ext["maxOffset"] = "0";
        }
        {
            std::lock_guard<std::mutex> lk(state_);
            pullCodes_.push_back(reply.code);
        }
        resp.code = reply.code;
        resp.extFields = reply.ext;
        if (!reply.body.empty()) {
            resp.body = reply.body;
            resp.hasBody = true;
        }
    }

    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::mutex state_;
    std::map<std::string, Bytes> routes_;
    std::vector<PropertyMap> pulls_;
    std::vector<int32_t> pullCodes_;
    std::vector<int32_t> codes_;
    std::deque<PullReply> pullScripts_;
    std::mutex gateMutex_;
    std::condition_variable gateCv_;
    bool gateArmed_ = false;
    bool gateOpen_ = false;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- 脚手架

const char* kTopic = "LiteCursorTopic";
const char* kBroker = "broker-a";

MessageQueue queue0() { return MessageQueue(kTopic, kBroker, 0); }

TopicRouteData routeMaster(const std::string& addr) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[MixAll::MASTER_ID] = addr;
    route.brokerDatas.emplace_back("DefaultCluster", kBroker, addrs);
    route.queueDatas.emplace_back(kBroker, 1, 1, 6, 0);
    return route;
}

// 应答头：nextBeginOffset / minOffset / maxOffset
PropertyMap pullResp(int64_t next, int64_t min = 0, int64_t max = 9) {
    PropertyMap ext;
    ext["nextBeginOffset"] = std::to_string(next);
    ext["minOffset"] = std::to_string(min);
    ext["maxOffset"] = std::to_string(max);
    return ext;
}

// 等条件成立（默认 5s 上限），返回是否等到
template <typename Fn>
bool waitFor(Fn fn, int timeoutMs = 5000) {
    const int64_t deadline = UtilAll::currentTimeMillis() + timeoutMs;
    while (UtilAll::currentTimeMillis() < deadline) {
        if (fn()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return fn();
}

// 起一个指向假端点的 lite-pull（assign + FIRST_OFFSET：起点是字面量 0，不做起点位点查询
// 之外的 RPC，第一笔 PULL_MESSAGE 确定落在 queueOffset=0）。
std::unique_ptr<DefaultLitePullConsumer> startedLite(const std::string& group,
                                                     MockEndpoint& cluster) {
    auto c = std::make_unique<DefaultLitePullConsumer>(group);
    c->setNamesrvAddr(cluster.address());
    c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    c->assign({queue0()});
    c->start();
    return c;
}

// ---------------------------------------------------------------- 1. NO_MATCHED_MSG

// Java :982-998：NO_MATCHED_MSG 的 nextBeginOffset 已越过本轮扫过的整段不匹配区间，拉取
// 游标必须跟过去 —— 旧实现只在 FOUND 时用 `msgs.back().queueOffset + 1` 推进，游标会永远
// 卡在 0，每轮把同一段不匹配区间重扫一遍。
void testCursorFollowsNextBeginOffsetOnNoMatchedMsg() {
    MockEndpoint cluster;
    cluster.addRoute(kTopic, routeMaster(cluster.address()));
    cluster.scriptPull(ResponseCode::PULL_RETRY_IMMEDIATELY, pullResp(/*next=*/5));
    auto c = startedLite("LitePG_CursorNoMatch", cluster);

    const MessageQueue mq = queue0();
    expect(waitFor([&] { return c->pullCursorOf(mq) == 5; }),
           "NO_MATCHED_MSG 后拉取游标跟到 nextBeginOffset=5");
    // 不是内存表里改了个数：下一笔 PULL_MESSAGE 真的从 5 起
    expect(waitFor([&] { return cluster.pullHasOffset(5); }),
           "下一笔 PULL_MESSAGE 从 5 开始（游标真的上了线）");
    // #106：FIRST_OFFSET 的起点是字面量 0（Java RebalanceLitePullImpl 不发 minOffset 查询）
    expect(!cluster.sawRequest(RequestCode::GET_MIN_OFFSET),
           "FIRST_OFFSET 起点是字面量 0：一次 minOffset 都不该发");
    expect(waitFor([&] { return cluster.pullCode(0) == ResponseCode::PULL_RETRY_IMMEDIATELY; }),
           "前置：第一笔 pull 命中的就是脚本里的 NO_MATCHED_MSG");
    expect(c->poll(50).empty(), "NO_MATCHED_MSG 没有可交付的消息");
    c->shutdown();
}

// ---------------------------------------------------------------- 2. OFFSET_ILLEGAL

// OFFSET_ILLEGAL 的 nextBeginOffset 是 broker 对越界位点的纠正值：跟过去才算「越界自愈」，
// 停在旧位点会每轮收到同一个纠正、原地打转。
void testCursorAdoptsTheBrokersOffsetCorrection() {
    MockEndpoint cluster;
    cluster.addRoute(kTopic, routeMaster(cluster.address()));
    cluster.scriptPull(ResponseCode::PULL_OFFSET_MOVED, pullResp(/*next=*/42, /*min=*/40));
    auto c = startedLite("LitePG_CursorIllegal", cluster);

    const MessageQueue mq = queue0();
    expect(waitFor([&] { return c->pullCursorOf(mq) == 42; }),
           "OFFSET_ILLEGAL 后拉取游标采纳 broker 纠正");
    expect(waitFor([&] { return cluster.pullHasOffset(42); }),
           "下一笔 PULL_MESSAGE 从 42 开始");
    c->shutdown();
}

// ---------------------------------------------------------------- 3. 在途 seek

// 唯一一只刹车（Java :808 的 seekOffset == -1 + :979 的 isDropped）：在途应答回来时，这轮里
// 刚 seek 过的位点不许被盖掉，也不许把应答里的消息塞进缓冲（seek 的语义就是「游标钉在这里、
// 旧位点的消息全丢」）。发车闸把在途窗口拉成确定性的：请求已到 broker → seek → 放闸。
// 旧实现没有刹车：FOUND + 一条 offset=2 的消息会把游标改成 3（last+1）并把消息入缓冲。
void testInFlightSeekWinsOverThePullResult() {
    MockEndpoint cluster;
    cluster.addRoute(kTopic, routeMaster(cluster.address()));
    MessageExt late;
    late.topic = kTopic;
    late.brokerName = kBroker;
    late.queueId = 0;
    late.queueOffset = 2;
    late.body = "late";
    cluster.scriptPull(ResponseCode::SUCCESS, pullResp(/*next=*/7),
                       encodeMessageExt(late, false));
    cluster.gateNextPull();
    auto c = startedLite("LitePG_CursorSeekRace", cluster);

    expect(waitFor([&] { return cluster.pullCount() >= 1; }), "第一笔拉取到达 broker");
    const MessageQueue mq = queue0();
    c->seek(mq, 99);
    cluster.releasePullGate();

    expect(waitFor([&] { return cluster.pullHasOffset(99); }),
           "seek 之后下一笔 PULL_MESSAGE 从 99 起");
    expectInt(c->pullCursorOf(mq), 99, "在途应答不得盖掉 seek 写下的位点");
    expect(c->poll(50).empty(), "被刹车的一轮不许入缓冲");
    c->shutdown();
}

}  // namespace

int main() {
    testCursorFollowsNextBeginOffsetOnNoMatchedMsg();
    testCursorAdoptsTheBrokersOffsetCorrection();
    testInFlightSeekWinsOverThePullResult();
    std::printf("lite_pull_cursor: %d checks, %d failed\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
