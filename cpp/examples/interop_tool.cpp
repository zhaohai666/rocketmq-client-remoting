// 跨语言互操作工具：与 Python 参考客户端交叉验证协议字节。
//
// 用法：
//   rmq_interop --emit                 打印规范命令的 JSON / ROCKETMQ 两种编码（十六进制）
//   rmq_interop --decode <hex>         解码给定十六进制帧并打印字段（供 Python -> C++ 验证）
//   rmq_interop --emit-route           打印规范 TopicRouteData 的 JSON
//   rmq_interop --emit-heartbeat       打印规范 HeartbeatData 的 JSON
//   rmq_interop --decode-route <hex>   按 TopicRouteData 解码 hex(body)，回吐重序列化 JSON
//   rmq_interop --decode-heartbeat <hex>  按 HeartbeatData 解码 hex(body)，回吐重序列化 JSON
//
// "解码后重新序列化再交给 Python 用 json.loads 比对"这种形式，
// 能把"字段名不一致"暴露成缺失键，而不是靠字符串相等这种脆弱断言。
//
// 规范命令（两侧必须一致）：
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

// 解码 hex 后按指定结构重序列化，输出单行 JSON
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
