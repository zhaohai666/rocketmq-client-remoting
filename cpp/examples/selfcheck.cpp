// Self-check example: build / encode / decode round trip of the protocol layer.
//
// Needs no real cluster; used to quickly verify that the C++ protocol layer works.
#include <iostream>
#include <string>

#include "rocketmq/common/message.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/remoting/protocol/headers.h"
#include "rocketmq/remoting/protocol/remoting_command.h"

using namespace rocketmq;

int main() {
    // 1) Build a heartbeat request (JSON header), encode -> decode round trip
    auto hb = std::make_shared<HeartbeatRequestHeader>();
    hb->clientId = "127.0.0.1@10911";

    RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT, hb);
    Bytes wire = req.encode();
    RemotingCommand dec = RemotingCommand::decode(wire);

    std::cout << "request  : " << req.toString() << "\n";
    std::cout << "wire     : " << wire.size() << " bytes\n";
    std::cout << "decoded  : " << dec.toString() << "\n";

    bool ok = dec.code == RequestCode::HEART_BEAT &&
              dec.getExtField("clientID") == "127.0.0.1@10911";
    std::cout << (ok ? "[PASS] HEART_BEAT json round-trip\n"
                     : "[FAIL] HEART_BEAT json round-trip\n");

    // 2) ROCKETMQ binary header round trip
    RemotingCommand req2 = RemotingCommand::createRequestCommand(RequestCode::GET_MAX_OFFSET);
    req2.serializeTypeCurrentRPC = SerializeType::ROCKETMQ;
    req2.addExtField("topic", "TBW102");
    req2.addExtField("queueId", "0");
    RemotingCommand dec2 = RemotingCommand::decode(req2.encode());
    bool ok2 = dec2.serializeTypeCurrentRPC == SerializeType::ROCKETMQ &&
               dec2.getExtField("topic") == "TBW102";
    std::cout << (ok2 ? "[PASS] GET_MAX_OFFSET rocketmq-binary round-trip\n"
                      : "[FAIL] GET_MAX_OFFSET rocketmq-binary round-trip\n");

    // 3) Message body encode -> decode round trip
    MessageExt m;
    m.topic = "SELF_CHECK";
    m.body = "hello-rocketmq-cpp";
    m.bornHost = "127.0.0.1";
    m.bornHostPort = 10911;
    m.storeHost = "127.0.0.1";
    m.storeHostPort = 10911;
    m.setTags("SelfCheck");
    Bytes raw = encodeMessageExt(m, false);
    MessageExt dm;
    bool ok3 = decodeMessage(raw, dm, true) && dm.body == std::string("hello-rocketmq-cpp") &&
               dm.getTags() == "SelfCheck";
    std::cout << (ok3 ? "[PASS] message encode/decode round-trip\n"
                      : "[FAIL] message encode/decode round-trip\n");

    if (ok && ok2 && ok3) {
        std::cout << "\nselfcheck: ALL PASS\n";
        return 0;
    }
    std::cout << "\nselfcheck: FAILED\n";
    return 1;
}
