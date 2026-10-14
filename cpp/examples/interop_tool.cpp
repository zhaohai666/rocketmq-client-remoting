// Cross-language interop tool: cross-checks protocol bytes with the Python reference client.
//
// Usage:
//   rmq_interop --emit                 print the canonical command as JSON / ROCKETMQ encodings (hex)
//   rmq_interop --decode <hex>         decode the given hex frame and print its fields (for Python -> C++ checks)
//   rmq_interop --emit-route           print the JSON of the canonical TopicRouteData
//   rmq_interop --emit-heartbeat       print the JSON of the canonical HeartbeatData
//   rmq_interop --decode-route <hex>   decode hex(body) as TopicRouteData, emit the re-serialized JSON
//   rmq_interop --decode-heartbeat <hex>  decode hex(body) as HeartbeatData, emit the re-serialized JSON
//
// "Decode, re-serialize, then have the other side compare the JSON with json.loads" exposes a
// field-name mismatch as a missing key, instead of relying on the fragile string-equality assertion.
//
// Canonical command (both sides must use the same one):
//   code=10, language=1(CPP), opaque=7, version=0, flag=0,
//   remark="hello", extFields{topic=T_TEST, queueId=2}, body="payload"
#include <iostream>
#include <map>
#include <string>

#include "rocketmq/common/subscription_data.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/protocol/route.h"

using namespace rocketmq;

namespace {

RemotingCommand canonicalCommand(uint8_t serializeType) {
    RemotingCommand cmd;
    cmd.code = 10;
    cmd.language = LanguageCode::CPP;
    cmd.version = 0;
    cmd.opaque = 7;
    cmd.flag = 0;
    cmd.remark = "hello";
    cmd.hasRemark = true;
    cmd.addExtField("topic", "T_TEST");
    cmd.addExtField("queueId", "2");
    cmd.body = "payload";
    cmd.hasBody = true;
    cmd.serializeTypeCurrentRPC = serializeType;
    return cmd;
}

TopicRouteData canonicalRoute() {
    TopicRouteData t;
    t.setOrderTopicConf("");
    t.queueDatas.push_back(
        QueueData("broker-a", 4, 4, PermName::PERM_READ | PermName::PERM_WRITE, 0));
    t.queueDatas.push_back(QueueData("broker-b", 2, 2, PermName::PERM_READ, 0));
    std::map<int64_t, std::string> addrsA;
    addrsA[0] = "127.0.0.1:10911";
    addrsA[1] = "127.0.0.1:10912";
    std::map<int64_t, std::string> addrsB;
    addrsB[0] = "127.0.0.1:10913";
    t.brokerDatas.push_back(BrokerData("DefaultCluster", "broker-a", addrsA, "", false));
    t.brokerDatas.push_back(BrokerData("DefaultCluster", "broker-b", addrsB, "", false));
    return t;
}

HeartbeatData canonicalHeartbeat() {
    HeartbeatData hb("10.0.0.1@12345");
    hb.addProducerData(ProducerData("pg_probe"));
    ConsumerData cd("cg_probe", ConsumeType::CONSUME_PASSIVELY, MessageModel::CLUSTERING,
                    ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET);
    cd.setUnitMode(false);
    SubscriptionData sd = FilterAPI::buildSubscriptionData("TopicProbe", "TagA||TagB");
    sd.setSubVersion(1700000000000LL);
    cd.addSubscriptionData(sd);
    hb.addConsumerData(cd);
    return hb;
}

// Decode the hex, re-serialize it with the given structure, print one line of JSON
template <typename DecodeFn>
int decodeAndDump(const char* hex, DecodeFn fn) {
    Bytes raw = UtilAll::string2Bytes(hex);
    if (raw.empty()) {
        std::cerr << "empty/invalid hex\n";
        return 2;
    }
    JsonValue out;
    if (!fn(raw, out)) {
        std::cerr << "decode failed\n";
        return 3;
    }
    std::cout << out.dump() << "\n";
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: rmq_interop --emit | --decode <hex> | --emit-route | "
                     "--emit-heartbeat | --decode-route <hex> | --decode-heartbeat <hex>\n";
        return 1;
    }
    const std::string mode = argv[1];

    if (mode == "--emit") {
        RemotingCommand jsonCmd = canonicalCommand(SerializeType::JSON);
        RemotingCommand rmqCmd = canonicalCommand(SerializeType::ROCKETMQ);
        std::cout << "JSON " << UtilAll::bytes2String(jsonCmd.encode()) << "\n";
        std::cout << "ROCKETMQ " << UtilAll::bytes2String(rmqCmd.encode()) << "\n";
        return 0;
    }

    if (mode == "--emit-route") {
        std::cout << canonicalRoute().toJson().dump() << "\n";
        return 0;
    }

    if (mode == "--emit-heartbeat") {
        std::cout << canonicalHeartbeat().toJson().dump() << "\n";
        return 0;
    }

    if (mode == "--decode" && argc >= 3) {
        Bytes raw = UtilAll::string2Bytes(argv[2]);
        if (raw.empty()) {
            std::cerr << "empty/invalid hex\n";
            return 2;
        }
        RemotingCommand cmd;
        std::string err;
        if (!RemotingCommand::tryDecode(raw, cmd, &err)) {
            std::cerr << "decode failed: " << err << "\n";
            return 3;
        }
        std::cout << "code=" << cmd.code
                  << ";language=" << static_cast<int>(cmd.language)
                  << ";opaque=" << cmd.opaque
                  << ";version=" << cmd.version
                  << ";remark=" << cmd.remark
                  << ";body=" << cmd.body;
        for (const auto& kv : cmd.extFields) {
            std::cout << ";ext." << kv.first << "=" << kv.second;
        }
        std::cout << "\n";
        return 0;
    }

    if (mode == "--decode-route" && argc >= 3) {
        return decodeAndDump(argv[2], [](const Bytes& raw, JsonValue& out) {
            TopicRouteData t;
            if (!TopicRouteData::decode(raw, t)) {
                return false;
            }
            out = t.toJson();
            return true;
        });
    }

    if (mode == "--decode-heartbeat" && argc >= 3) {
        return decodeAndDump(argv[2], [](const Bytes& raw, JsonValue& out) {
            HeartbeatData hb;
            if (!HeartbeatData::decode(raw, hb)) {
                return false;
            }
            out = hb.toJson();
            return true;
        });
    }

    std::cerr << "unknown mode: " << mode << "\n";
    return 1;
}
