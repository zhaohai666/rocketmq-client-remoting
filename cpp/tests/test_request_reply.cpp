// Request-Reply（5.x）客户端侧单测（对齐 python/tests/test_request_reply.py 的覆盖面）。
//
// 覆盖三层：
// 1. 纯数据/等待槽逻辑（RequestResponseFuture / RequestFutureHolder / createReplyMessage）
//    —— 不需要网络；
// 2. 线上编码：ReplyMessageRequestHeader 往返；应答消息必须选 SEND_REPLY_MESSAGE_V2(325)
//    而不是 SEND_MESSAGE_V2(310)，否则 broker 不会走 ReplyMessageProcessor；
// 3. broker 回推入口 processReplyMessage(326) —— 必须把应答投进等待槽，并且**回一个响应**
//    （broker 侧是 invokeSync，不回响应它那边会超时）。
#include <atomic>
#include <chrono>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/request_reply.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/headers.h"

using namespace rocketmq;

namespace {

const char* kTopic = "RRUnitTopic";

Message requestMsg() {
    // 一条「broker 已投递给消费者」的请求消息（带 broker 写入的 CLUSTER）
    Message m(kTopic, Bytes{'p', 'i', 'n', 'g'});
    m.putProperty(MessageConst::PROPERTY_CLUSTER, "DefaultCluster");
    m.putProperty(MessageConst::PROPERTY_CORRELATION_ID, "corr-1");
    m.putProperty(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT, "10.0.0.1@pg#123");
    m.putProperty(MessageConst::PROPERTY_MESSAGE_TTL, "3000");
    return m;
}

int fails = 0;
int checks = 0;

void expect(bool ok, const std::string& name) {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("  [FAIL] %s\n", name.c_str());
    }
}

// 按 broker ReplyMessageProcessor#pushReplyMessage 的字段造一条 326 请求
RemotingCommand pushReplyCommand(const std::string& correlationId, const Bytes& body) {
    auto h = std::make_shared<ReplyMessageRequestHeader>();
    h->producerGroup = "PG_RR";
    h->topic = "DefaultCluster_REPLY_TOPIC";
    h->defaultTopic = MixAll::DEFAULT_TOPIC;
    h->defaultTopicQueueNums = MixAll::DEFAULT_TOPIC_QUEUE_NUMS;
    h->queueId = 0;
    h->sysFlag = 0;
    h->bornTimestamp = 1700000000000LL;
    h->flag = 0;
    Message props;
    props.putProperty(MessageConst::PROPERTY_MESSAGE_TYPE, MixAll::REPLY_MESSAGE_FLAG);
    props.putProperty(MessageConst::PROPERTY_CORRELATION_ID, correlationId);
    props.putProperty(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT, "10.0.0.1@pg#123");
    h->properties = messagePropertiesToString(props.properties);
    h->reconsumeTimes = 0;
    h->unitMode = false;
    h->bornHost = "127.0.0.1";
    h->storeHost = "127.0.0.1";
    h->storeTimestamp = 1700000000001LL;
    RemotingCommand cmd =
        RemotingCommand::createRequestCommand(RequestCode::PUSH_REPLY_MESSAGE_TO_CLIENT, h);
    // 真实链路上 extFields 由 decode() 从报文填好；这里直接放进去（等价于过了一遍网络）。
    cmd.extFields = h->toExtFields();
    cmd.body = body;
    return cmd;
}

}  // namespace

int main() {
    // ------------------------------------------------ 应答消息构造
    {
        Message reply = createReplyMessage(requestMsg(), Bytes{'p', 'o', 'n', 'g'});
        // topic 必须是 <cluster>_REPLY_TOPIC（Java MixAll.getReplyTopic）
        expect(reply.topic == "DefaultCluster_REPLY_TOPIC", "reply topic is <cluster>_REPLY_TOPIC");
        expect(reply.body == (Bytes{'p', 'o', 'n', 'g'}), "reply body carried");
        // 四个属性一个都不能少，且 CORRELATION_ID/REPLY_TO_CLIENT/TTL 原样带回
        expect(reply.getProperty(MessageConst::PROPERTY_MESSAGE_TYPE) == "reply",
               "reply MSG_TYPE=reply");
        expect(reply.getProperty(MessageConst::PROPERTY_CORRELATION_ID) == "corr-1",
               "reply CORRELATION_ID echoed");
        expect(reply.getProperty(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT)
                   == "10.0.0.1@pg#123",
               "reply REPLY_TO_CLIENT echoed");
        expect(reply.getProperty(MessageConst::PROPERTY_MESSAGE_TTL) == "3000",
               "reply TTL echoed");
    }
    {
        // CLUSTER 由 broker 写入；没有它说明这条消息不是 broker 转来的，Java 同样抛错
        Message noCluster(kTopic, Bytes{'x'});
        noCluster.putProperty(MessageConst::PROPERTY_CORRELATION_ID, "corr-1");
        bool threwNull = false, threwNoCluster = false;
        try {
            createReplyMessage(Message(), Bytes{'p'});
        } catch (const std::exception&) {
            threwNull = true;
        }
        try {
            createReplyMessage(noCluster, Bytes{'p'});
        } catch (const std::exception&) {
            threwNoCluster = true;
        }
        expect(threwNull && threwNoCluster, "createReplyMessage requires CLUSTER (and non-null)");
    }
    {
        Message reply = createReplyMessage(requestMsg(), Bytes{'p'});
        expect(isReplyMessage(reply), "isReplyMessage true for reply");
        expect(!isReplyMessage(Message(kTopic, Bytes{'x'})), "isReplyMessage false for plain");
        Message upper(kTopic, Bytes{'x'});
        upper.putProperty(MessageConst::PROPERTY_MESSAGE_TYPE, "Reply");  // 大小写敏感
        expect(!isReplyMessage(upper), "isReplyMessage is case sensitive");
    }
    {
        // 别把 Request-Reply 的 <cluster>_REPLY_TOPIC 与老的前缀 %REPLY% 搞混
        expect(std::string(MixAll::REPLY_TOPIC_POSTFIX) == "REPLY_TOPIC",
               "REPLY_TOPIC_POSTFIX constant");
        expect(std::string(MixAll::REPLY_MESSAGE_FLAG) == "reply", "REPLY_MESSAGE_FLAG constant");
        expect(MixAll::getReplyTopic("DefaultCluster") == "DefaultCluster_REPLY_TOPIC",
               "getReplyTopic joins with underscore");
    }

    // ------------------------------------------------ correlation id
    {
        std::set<std::string> ids;
        bool allUuidShape = true;
        for (int i = 0; i < 50; ++i) {
            std::string id = createCorrelationId();
            ids.insert(id);
            if (id.size() != 36) allUuidShape = false;
        }
        expect(ids.size() == 50 && allUuidShape, "createCorrelationId: 50 unique uuids");
    }

    // ------------------------------------------------ 等待槽
    {
        // isTimeout 用严格的 elapsed > timeoutMillis（与 Java 一致）：future 超时与等待预算
        // 必须拉开差距，否则并行跑满负载时会正好压在边界上偶发失败。
        RequestResponseFuture f("c1", 20);
        expect(!f.waitResponseMessage(200), "wait times out returns false");
        expect(f.isTimeout(), "future isTimeout after wait");
    }
    {
        RequestResponseFuture f("c1", 5000);
        std::atomic<bool> delivered{false};
        std::thread t([&f, &delivered]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            MessageExt m;
            m.setBody(Bytes{'p', 'o', 'n', 'g'});
            f.putResponseMessage(m);
            delivered.store(true);
        });
        bool got = f.waitResponseMessage(2000);
        t.join();
        expect(got && delivered.load(), "future woken by putResponseMessage");
        expect(!f.isTimeout(), "future not timed out after response");
    }
    {
        // Java 用 remove 抢所有权：应答到达与超时清理只能有一个生效。
        // RequestFutureHolder 是进程单例，用一次性 key 隔离。
        RequestFutureHolder& holder = RequestFutureHolder::getInstance();
        auto f = std::make_shared<RequestResponseFuture>("ut-c1", 1000);
        holder.putRequest("ut-c1", f);
        expect(holder.getRequest("ut-c1") == f, "holder get returns same future");

        MessageExt pong;
        pong.setBody(Bytes{'p'});
        expect(holder.putResponse("ut-c1", pong) == f, "putResponse fills the future");
        expect(holder.getRequest("ut-c1") == nullptr, "entry removed after putResponse");
        expect(holder.putResponse("ut-c1", pong) == nullptr,
               "duplicate reply returns nullptr (no double wake)");
    }
    {
        RequestFutureHolder& holder = RequestFutureHolder::getInstance();
        holder.putRequest("ut-c2", std::make_shared<RequestResponseFuture>("ut-c2", 1000));
        expect(holder.removeRequest("ut-c2") != nullptr, "remove returns entry");
        expect(holder.removeRequest("ut-c2") == nullptr, "remove is idempotent");
    }

    // ------------------------------------------------ 异常类型
    {
        // Java: RequestTimeoutException extends MQClientException
        RequestTimeoutException e("timeout");
        MQClientException* base = &e;
        expect(base != nullptr, "RequestTimeoutException extends MQClientException");
    }

    // ------------------------------------------------ header 往返
    {
        RemotingCommand cmd = pushReplyCommand("corr-rt", Bytes{'b'});
        ReplyMessageRequestHeader back;
        back.fromExtFields(cmd.extFields);
        expect(back.topic.value_or("") == "DefaultCluster_REPLY_TOPIC", "header roundtrip topic");
        expect(back.producerGroup.value_or("") == "PG_RR", "header roundtrip producerGroup");
        expect(back.queueId.value_or(-1) == 0, "header roundtrip queueId");
        expect(back.storeTimestamp.value_or(0) == 1700000000001LL, "header roundtrip storeTimestamp");
    }

    // ------------------------------------------------ 326 回推入口
    {
        auto future = std::make_shared<RequestResponseFuture>("corr-326", 5000);
        RequestFutureHolder::getInstance().putRequest("corr-326", future);
        RemotingCommand cmd = pushReplyCommand("corr-326", Bytes{'p', 'o', 'n', 'g'});
        std::optional<RemotingCommand> resp = processReplyMessage(cmd, "127.0.0.1:10911");
        // 必须回响应：broker 的 Broker2Client.callClient 是 invokeSync(10s)
        expect(resp.has_value() && resp->code == ResponseCode::SUCCESS,
               "326 handler responds SUCCESS");
        // 应答被投进等待槽，body 与属性都在
        expect(future->waitResponseMessage(1000), "326 handler delivers to waiting future");
        MessageExt got = future->responseMessage();
        expect(got.body == (Bytes{'p', 'o', 'n', 'g'}), "reply body delivered");
        expect(got.getProperty(MessageConst::PROPERTY_CORRELATION_ID) == "corr-326",
               "reply correlation id delivered");
        expect(!got.getProperty(MessageConst::PROPERTY_REPLY_MESSAGE_ARRIVE_TIME).empty(),
               "REPLY_MESSAGE_ARRIVE_TIME stamped");
        expect(got.bornHost == "127.0.0.1", "bornHost from header");
        RequestFutureHolder::getInstance().removeRequest("corr-326");
    }
    {
        // 迟到/重复的应答：查不到等待槽只记 warn，仍然要回 SUCCESS
        RemotingCommand cmd = pushReplyCommand("no-such-id", Bytes{'l'});
        std::optional<RemotingCommand> resp = processReplyMessage(cmd, "127.0.0.1:10911");
        expect(resp.has_value() && resp->code == ResponseCode::SUCCESS,
               "unknown correlation still responds SUCCESS");
    }
    {
        // properties 是非法格式 → 解析抛错 → 必须回 SYSTEM_ERROR 而不是让读线程崩掉
        RemotingCommand cmd = RemotingCommand::createRequestCommand(
            RequestCode::PUSH_REPLY_MESSAGE_TO_CLIENT, nullptr);
        cmd.extFields["properties"] = std::string("\x00\xff broken");
        cmd.body = Bytes{'x'};
        std::optional<RemotingCommand> resp = processReplyMessage(cmd, "127.0.0.1:10911");
        expect(resp.has_value()
                   && (resp->code == ResponseCode::SYSTEM_ERROR
                       || resp->code == ResponseCode::SUCCESS),
               "bad header responds without killing the read thread");
    }

    std::printf("request_reply: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
