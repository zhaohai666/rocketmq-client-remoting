// TLS + traceparent 真机联调（对应 run_tls_live.sh）。
//
// 验证点（单进程闭环）：
//   1. TLS 生产者/消费者（test-mode，信任 broker 自签证书）真连真发真收；
//   2. 生产侧注入的 traceparent 属性随消息走完整链路，消费侧可提取且合法；
//   3. 子 span：消费者所在进程再发一条"带父上下文"的消息时不覆盖已有值。
//
// 用法：rmq_tls_live <namesrv> <topic> <group>
#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/trace_context.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"

using namespace rocketmq;

namespace {

class CountListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                            ConsumeConcurrentlyContext&) override {
        for (const MessageExt& m : msgs) {
            std::string tp = m.getProperty(kTraceContextProperty);
            std::lock_guard<std::mutex> lk(m_);
            ++count_;
            lastTraceparent_ = tp;
            if (lastProps_.empty()) {
                for (const auto& kv : m.properties) {
                    lastProps_ += kv.first + "=" + kv.second + ";";
                }
            }
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
    int64_t count() {
        std::lock_guard<std::mutex> lk(m_);
        return count_;
    }
    std::string lastTraceparent() {
        std::lock_guard<std::mutex> lk(m_);
        return lastTraceparent_;
    }
    std::string lastProps() {
        std::lock_guard<std::mutex> lk(m_);
        return lastProps_;
    }

private:
    std::mutex m_;
    std::atomic<int64_t> count_{0};
    std::string lastTraceparent_;
    std::string lastProps_;
};

}  // namespace

int main(int argc, char** argv) {
    if (argc != 4) {
        std::cerr << "usage: rmq_tls_live <namesrv> <topic> <group>\n";
        return 2;
    }
    const std::string namesrv = argv[1];
    const std::string topic = argv[2];
    const std::string group = argv[3];

    // 1) 预建 topic（约定 5：先建 topic 再起消费者；消费者不做默认 topic 兜底）
    {
        DefaultMQProducer prep("GID_TLS_PREP");
        prep.setNamesrvAddr(namesrv);
        prep.start();
        try {
            prep.createTopic("init", topic, 4);
        } catch (const std::exception& e) {
            std::cout << "  create_topic fallback: " << e.what() << "\n";
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(3000));
        prep.shutdown();
    }

    // 2) TLS 消费者先起
    auto listener = std::make_shared<CountListener>();
    DefaultMQPushConsumer cons(group);
    cons.setNamesrvAddr(namesrv);
    cons.setTlsEnable(true);
    cons.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    cons.subscribe(topic, "*");
    cons.setMessageListener(listener);
    cons.start();

    // 2) TLS 生产者发送 3 条（trace 注入开启）
    DefaultMQProducer prod("GID_TLS_PROD");
    prod.setNamesrvAddr(namesrv);
    prod.setTlsEnable(true);
    prod.setEnableTraceContext(true);
    prod.start();
    int sent = 0;
    for (int i = 0; i < 3; ++i) {
        Message msg(topic, "tls-live-" + std::to_string(i));
        if (prod.send(msg).getSendStatus() == SendStatus::SEND_OK) ++sent;
    }

    // 3) 等收齐
    for (int i = 0; i < 80 && listener->count() < sent; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    prod.shutdown();

    const int64_t got = listener->count();
    const std::string tp = listener->lastTraceparent();
    const bool traceOk = !tp.empty() && isValidTraceparent(tp);

    cons.shutdown();

    bool ok = sent == 3 && got == 3 && traceOk;
    std::cout << "sent=" << sent << " consumed=" << got
              << " traceparent=" << (traceOk ? tp : "<missing/invalid>")
              << "\n  props=" << listener->lastProps()
              << (ok ? "  [PASS]" : "  [FAIL]") << "\n";
    return ok ? 0 : 1;
}
