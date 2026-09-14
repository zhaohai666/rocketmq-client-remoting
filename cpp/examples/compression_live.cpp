// 自动压缩的**真实集群 + 跨客户端**联调工具（对应 python/verify_compression_live.py）。
//
// 为什么需要它：单元测试只能证明「zlib 往返正确」，证明不了
//   1. producer 真的在超过 compressMsgBodyOverHowmuch(4096) 时压缩并置 COMPRESSED_FLAG；
//   2. broker 存的是压缩体（storeSize 远小于原文）；
//   3. 消费端真的解压并把 COMPRESSED_FLAG 清掉（对齐 Java MessageDecoder 第 520-523 行）；
//   4. **别的客户端（Java）产生的压缩消息，我们能正确解压** —— 这是真机才暴露的静默数据损坏点。
//
// 载荷是**确定性**的（重复一行固定文本后截断），与 Java 探针 CompressProbe 完全相同，
// 因此两端各自本地重建后比较 CRC32 即可，无需交换文件。
//
// ⚠ CRC32 显示值会不同，这不是 bug：Java `UtilAll.crc32` 返回 `(int)(value & 0x7FFFFFFF)`，
// 砍掉了最高位；本工具用标准 CRC-32。所以 Java 打印 1785582993 对应本工具打印 3933066641
// （差正好 2^31）。判定互通要看各自的 `match=` 字段，不要直接比两边打印的 CRC 数字。
//
// 用法：
//   rmq_compression_live selftest <namesrv>
//   rmq_compression_live send     <namesrv> <topic> <group> <size>
//   rmq_compression_live recv     <namesrv> <topic> <group> <size>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/compression.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"

using namespace rocketmq;

namespace {

const char* kLine = "rocketmq-compress-interop-payload-line-0123456789\n";

// Java CompressProbe.buildPayload 的等价实现（必须逐字节一致）
std::string buildPayload(int size) {
    std::string s;
    s.reserve(static_cast<size_t>(size) + 64);
    while (static_cast<int>(s.size()) < size) s += kLine;
    s.resize(static_cast<size_t>(size));
    return s;
}

Bytes str2bytes(const std::string& s) { return Bytes(s.begin(), s.end()); }

// 消费 1 条消息（FIRST_OFFSET），超时返回 false
class FirstMsgListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                            ConsumeConcurrentlyContext&) override {
        for (const MessageExt& m : msgs) {
            std::lock_guard<std::mutex> lk(m_);
            if (!have_) {
                first_ = m;
                have_ = true;
            }
            ++count_;
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
    bool have() {
        std::lock_guard<std::mutex> lk(m_);
        return have_;
    }
    MessageExt first() {
        std::lock_guard<std::mutex> lk(m_);
        return first_;
    }
    int64_t count() {
        std::lock_guard<std::mutex> lk(m_);
        return count_;
    }

private:
    std::mutex m_;
    MessageExt first_;
    bool have_ = false;
    int64_t count_ = 0;
};

// 把一条消息收回来；失败返回 false
bool recvOne(const std::string& namesrv, const std::string& topic, const std::string& group,
             int32_t timeoutSec, MessageExt& out) {
    auto listener = std::make_shared<FirstMsgListener>();
    DefaultMQPushConsumer cons(group);
    cons.setNamesrvAddr(namesrv);
    cons.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    cons.subscribe(topic, "*");
    cons.setMessageListener(listener);
    cons.setPullTimeoutMillis(3000);
    cons.setPullSuspendTimeoutMillis(1000);
    cons.start();
    for (int i = 0; i < timeoutSec * 4 && !listener->have(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    bool ok = listener->have();
    if (ok) out = listener->first();
    cons.shutdown();
    return ok;
}

int usage() {
    std::cerr << "usage: rmq_compression_live selftest <namesrv>\n"
                 "       rmq_compression_live send <namesrv> <topic> <group> <size>\n"
                 "       rmq_compression_live recv <namesrv> <topic> <group> <size>\n";
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) return usage();
    const std::string mode = argv[1];
    const std::string namesrv = argv[2];

    if (mode == "send") {
        if (argc < 6) return usage();
        const std::string topic = argv[3];
        const std::string group = argv[4];
        const int size = std::stoi(argv[5]);
        const std::string payload = buildPayload(size);
        const Bytes body = str2bytes(payload);
        DefaultMQProducer prod(group);
        prod.setNamesrvAddr(namesrv);
        prod.setSendMsgTimeout(10000);
        prod.start();
        SendResult sr = prod.send(Message(topic, body));
        prod.shutdown();
        std::cout << "SEND_OK len=" << body.size()
                  << " crc32=" << crc32(body)
                  << " msgId=" << sr.msgId << std::endl;
        return sr.sendStatus == SendStatus::SEND_OK ? 0 : 1;
    }

    if (mode == "recv") {
        if (argc < 6) return usage();
        const std::string topic = argv[3];
        const std::string group = argv[4];
        const int size = std::stoi(argv[5]);
        const std::string payload = buildPayload(size);
        MessageExt m;
        if (!recvOne(namesrv, topic, group, 30, m)) {
            std::cout << "RECV_TIMEOUT" << std::endl;
            return 3;
        }
        const bool lenOk = static_cast<int>(m.body.size()) == size;
        const bool crcOk = !m.body.empty() && crc32(m.body) == crc32(str2bytes(payload));
        std::cout << "RECV_OK len=" << m.body.size()
                  << " crc32=" << (m.body.empty() ? 0u : crc32(m.body))
                  << " storeSize=" << m.storeSize
                  << " match=" << ((lenOk && crcOk) ? 1 : 0) << std::endl;
        return (lenOk && crcOk) ? 0 : 1;
    }

    if (mode != "selftest") return usage();

    // ---------------- selftest：自产自销 + 压缩真实性校验 ----------------
    int fail = 0;
    auto check = [&](const std::string& name, bool ok, const std::string& detail) {
        if (!ok) ++fail;
        std::cout << "[" << (ok ? "PASS" : "FAIL") << "] " << name << "  " << detail << std::endl;
    };

    const std::string stamp = std::to_string(UtilAll::currentTimeMillis());
    const std::string topic = "CompressLiveCpp_" + stamp;
    const std::string group = "CompressLiveCppGroup_" + stamp;
    const int size = 8192;  // 远超 compressMsgBodyOverHowmuch(4096)
    const std::string payload = buildPayload(size);
    const Bytes body = str2bytes(payload);
    const uint32_t payloadCrc = crc32(body);
    std::cout << "payload len=" << size << " crc32=" << payloadCrc
              << "（确定性载荷，Java 探针同算法）" << std::endl;

    // 1) 发出
    std::string msgId;
    {
        DefaultMQProducer prod("CompressLiveCppProducer_" + stamp);
        prod.setNamesrvAddr(namesrv);
        prod.setSendMsgTimeout(10000);
        prod.start();
        SendResult sr = prod.send(Message(topic, body));
        msgId = sr.msgId;
        check("发送 " + std::to_string(size) + "B 消息（触发自动压缩）",
              sr.sendStatus == SendStatus::SEND_OK, "msgId=" + sr.msgId);
        prod.shutdown();
    }
    if (fail) return 1;

    // 2) 收回来，正文必须与原文逐字节一致（证明"压缩-存储-解压"闭环）
    MessageExt m;
    if (!recvOne(namesrv, topic, group, 30, m)) {
        check("消费回压缩消息", false, "30s 超时");
        return 1;
    }
    check("消费回压缩消息", true,
          "len=" + std::to_string(m.body.size()) + " storeSize=" + std::to_string(m.storeSize));
    check("解压后正文与原文一致（len + CRC32）",
          m.body.size() == body.size() && crc32(m.body) == payloadCrc,
          "期望 len=" + std::to_string(body.size()) + " crc=" + std::to_string(payloadCrc)
          + " 实际 len=" + std::to_string(m.body.size()) + " crc=" + std::to_string(crc32(m.body)));

    // 3) 证明 broker 里存的**确实是压缩体**：storeSize 应该远小于原文长度。
    //    只断言"明显更小"而不是具体比例 —— 压缩率取决于 zlib 版本。
    check("broker 侧存储为压缩体（storeSize 远小于原文）",
          m.storeSize > 0 && m.storeSize < size / 2,
          "storeSize=" + std::to_string(m.storeSize) + " 原文=" + std::to_string(size)
          + " 压缩比=" + std::to_string(size / (m.storeSize > 0 ? m.storeSize : 1)) + ":1");

    // 4) 解压后 COMPRESSED_FLAG 必须被清掉（对齐 Java MessageDecoder 第 523 行），
    //    否则上层会误以为 body 还是压缩的。
    check("解压后 COMPRESSED_FLAG 已清除",
          !MessageSysFlag::isCompressed(m.sysFlag),
          "sysFlag=" + std::to_string(m.sysFlag));

    // 5) 小消息**不应**被压缩（阈值语义）
    {
        const std::string smallTopic = "CompressLiveSmall_" + stamp;
        const std::string smallGroup = "CompressLiveSmallGroup_" + stamp;
        const std::string small = "tiny-payload-under-threshold";
        DefaultMQProducer prod("CompressLiveSmallProducer_" + stamp);
        prod.setNamesrvAddr(namesrv);
        prod.start();
        prod.send(Message(smallTopic, str2bytes(small)));
        prod.shutdown();
        MessageExt sm;
        if (recvOne(namesrv, smallTopic, smallGroup, 20, sm)) {
            check("小于阈值(4096)的消息不压缩",
                  sm.body.size() == small.size() && !MessageSysFlag::isCompressed(sm.sysFlag),
                  "len=" + std::to_string(sm.body.size())
                  + " storeSize=" + std::to_string(sm.storeSize));
        } else {
            check("小于阈值(4096)的消息不压缩", false, "20s 未消费到");
        }
    }

    // 6) 复现"线上带压缩标志"的编解码路径，确认标志位语义闭环：
    //    置 COMPRESSED_FLAG|ZLIB 的 MessageExt，用 needCompress=true 编码 -> 存储体变小；
    //    解压解码 -> 还原原文；不解压解码 -> 拿到的就是压缩字节。
    //    这正是 broker 写入与消费端读取所走的同一套分支。
    {
        MessageExt probe;
        probe.topic = topic;
        probe.body = body;
        probe.sysFlag = MessageSysFlag::COMPRESSED_FLAG
            | CompressionType::getCompressionFlag(CompressionType::ZLIB);

        Bytes storedCompressed = encodeMessageExt(probe, /*needCompress=*/true);
        MessageExt raw;
        bool rawOk = decodeMessage(storedCompressed, raw, /*readBody=*/true,
                                   /*decompressBody=*/false);
        MessageExt restored;
        bool restoredOk = decodeMessage(storedCompressed, restored, /*readBody=*/true,
                                        /*decompressBody=*/true);

        check("带压缩标志编码后存储体变小",
              storedCompressed.size() < body.size() / 2,
              "stored=" + std::to_string(storedCompressed.size())
              + " 原文=" + std::to_string(body.size()));
        check("不解压解码拿到压缩字节、解压解码还原原文",
              rawOk && restoredOk && raw.body.size() < body.size()
              && restored.body.size() == body.size() && crc32(restored.body) == payloadCrc
              && !MessageSysFlag::isCompressed(restored.sysFlag),
              "rawLen=" + std::to_string(raw.body.size()) + " restoredLen="
              + std::to_string(restored.body.size()));
    }

    std::cout << "\n==== compression live summary ====" << std::endl;
    std::cout << (fail == 0 ? "ALL PASS" : "FAILED") << " (fail=" << fail << ")" << std::endl;
    return fail == 0 ? 0 : 1;
}
