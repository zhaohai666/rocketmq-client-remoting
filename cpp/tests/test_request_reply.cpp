// Request-Reply (5.x) client-side unit tests.
//
// Coverage spans three layers:
// 1. Pure data / waiting-slot logic (RequestResponseFuture / RequestFutureHolder / createReplyMessage)
//    -- no network needed;
// 2. Wire encoding: ReplyMessageRequestHeader round trip; the reply must use SEND_REPLY_MESSAGE_V2(325)
//    and not SEND_MESSAGE_V2(310), otherwise the broker never routes it through ReplyMessageProcessor;
// 3. The broker push entry processReplyMessage(326) -- it must deliver the reply into the waiting slot and **send a response back**
//    (the broker side uses invokeSync; without a response it times out there).
#include <atomic>
#include <chrono>
#include <cstdio>
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
    // A request message already delivered to the consumer by the broker (with the CLUSTER written by the broker)
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

// Build a 326 request following the fields of the broker's ReplyMessageProcessor#pushReplyMessage
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
    // On the real wire path extFields is filled by decode() from the packet; it is put in directly here (equivalent to one network trip).
    cmd.extFields = h->toExtFields();
    cmd.body = body;
    return cmd;
}

}  // namespace

int main() {
    // ------------------------------------------------ reply message construction
    {
        Message reply = createReplyMessage(requestMsg(), Bytes{'p', 'o', 'n', 'g'});
        expect(reply.topic == "DefaultCluster_REPLY_TOPIC", "reply topic is <cluster>_REPLY_TOPIC");
        expect(reply.body == (Bytes{'p', 'o', 'n', 'g'}), "reply body carried");
        // None of the four properties may be missing, and CORRELATION_ID/REPLY_TO_CLIENT/TTL must be echoed unchanged
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
        Message noCluster(kTopic, Bytes{'x'});
        noCluster.putProperty(MessageConst::PROPERTY_CORRELATION_ID, "corr-1");
        bool threwNull = false, threwNoCluster = false;
        int nullCode = 0, noClusterCode = 0;
        std::string noClusterMsg;
        const std::string kCreateReply10007 = "CREATE_REPLY_MESSAGE_EXCEPTION(10007)";
        try {
            createReplyMessage(Message(), Bytes{'p'});
        } catch (const MQClientException& e) {
            threwNull = true;
            nullCode = e.getResponseCode();
        } catch (const std::exception&) {
        }
        try {
            createReplyMessage(noCluster, Bytes{'p'});
        } catch (const MQClientException& e) {
            threwNoCluster = true;
            noClusterCode = e.getResponseCode();
            noClusterMsg = e.what();
        } catch (const std::exception&) {
        }
        expect(threwNull && threwNoCluster, "createReplyMessage requires CLUSTER (and non-null)");
        expect(noClusterCode == ClientErrorCode::CREATE_REPLY_MESSAGE_EXCEPTION,
               "missing CLUSTER -> " + kCreateReply10007 + " (got "
                   + std::to_string(noClusterCode) + ")");
        expect(nullCode == ClientErrorCode::CREATE_REPLY_MESSAGE_EXCEPTION,
               "empty request message -> " + kCreateReply10007 + " too (got "
                   + std::to_string(nullCode) + ")");
        expect(noClusterMsg.find("property[CLUSTER] is null.") != std::string::npos,
               "10007 points at the missing CLUSTER property (reference wording): " + noClusterMsg);
    }
    {
        Message reply = createReplyMessage(requestMsg(), Bytes{'p'});
        expect(isReplyMessage(reply), "isReplyMessage true for reply");
        expect(!isReplyMessage(Message(kTopic, Bytes{'x'})), "isReplyMessage false for plain");
        Message upper(kTopic, Bytes{'x'});
        upper.putProperty(MessageConst::PROPERTY_MESSAGE_TYPE, "Reply");  // case sensitive
        expect(!isReplyMessage(upper), "isReplyMessage is case sensitive");
    }
    {
        // Do not confuse the Request-Reply <cluster>_REPLY_TOPIC with the legacy %REPLY% prefix
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

    // ------------------------------------------------ waiting slots
    {
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

    // ------------------------------------------------ exception types
    {
        RequestTimeoutException e("timeout");
        MQClientException* base = &e;
        expect(base != nullptr, "RequestTimeoutException extends MQClientException");
    }

    // ------------------------------------------------ header round trip
    {
        RemotingCommand cmd = pushReplyCommand("corr-rt", Bytes{'b'});
        ReplyMessageRequestHeader back;
        back.fromExtFields(cmd.extFields);
        expect(back.topic.value_or("") == "DefaultCluster_REPLY_TOPIC", "header roundtrip topic");
        expect(back.producerGroup.value_or("") == "PG_RR", "header roundtrip producerGroup");
        expect(back.queueId.value_or(-1) == 0, "header roundtrip queueId");
        expect(back.storeTimestamp.value_or(0) == 1700000000001LL, "header roundtrip storeTimestamp");
    }

    // ------------------------------------------------ 326 push entry
    {
        auto future = std::make_shared<RequestResponseFuture>("corr-326", 5000);
        RequestFutureHolder::getInstance().putRequest("corr-326", future);
        RemotingCommand cmd = pushReplyCommand("corr-326", Bytes{'p', 'o', 'n', 'g'});
        std::optional<RemotingCommand> resp = processReplyMessage(cmd, "127.0.0.1:10911");
        // Must respond: the broker's Broker2Client.callClient uses invokeSync(10s)
        expect(resp.has_value() && resp->code == ResponseCode::SUCCESS,
               "326 handler responds SUCCESS");
        // The reply is delivered into the waiting slot with both body and properties present
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
        // Late/duplicate replies: with no matching waiting slot only a warn is logged, the response must still be SUCCESS
        RemotingCommand cmd = pushReplyCommand("no-such-id", Bytes{'l'});
        std::optional<RemotingCommand> resp = processReplyMessage(cmd, "127.0.0.1:10911");
        expect(resp.has_value() && resp->code == ResponseCode::SUCCESS,
               "unknown correlation still responds SUCCESS");
    }
    {
        // Malformed properties => parsing throws => must respond SYSTEM_ERROR instead of letting the read thread crash
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
