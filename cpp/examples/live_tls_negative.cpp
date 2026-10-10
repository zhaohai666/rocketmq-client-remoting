// #25 负探针（生产者-only，不走消费者）：严格 CA 校验下信任【错误 CA】，
// producer 路由层不得吞掉 TLS 握手异常。
//   修复前：send 只报 "Can not find Message Queue for topic: X"（黑盒，
//           用户往"建 topic"方向查，真正坏的是证书/CA）。
//   修复后：报错必须带 "(route fetch failed: <证书/CA 错误文本>)"。
// 用法: rmq_live_tls_negative <namesrv> <topic> <wrongCaCert> [serverName]
// 退出码：0 = 报错含 CA 标记（符合预期）；1 = 未按预期报错；2 = 用法错误。
#include <iostream>
#include <string>

#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/remoting/remoting_client.h"

using namespace rocketmq;

static bool hasCertMarker(const std::string& text) {
    return text.find("certificate") != std::string::npos
        || text.find("CERTIFICATE") != std::string::npos
        || text.find("SSL") != std::string::npos
        || text.find("ssl") != std::string::npos
        || text.find("verify") != std::string::npos;
}

int main(int argc, char** argv) {
    if (argc < 4 || argc > 5) {
        std::cerr << "usage: rmq_live_tls_negative <namesrv> <topic> <wrongCaCert> [serverName]\n";
        return 2;
    }
    const std::string namesrv = argv[1];
    const std::string topic = argv[2];
    TlsOptions tlsOptions;
    tlsOptions.caCert = argv[3];
    if (argc >= 5) tlsOptions.serverName = argv[4];

    std::string errText;
    DefaultMQProducer prod("GID_TlsNeg");
    prod.setNamesrvAddr(namesrv);
    prod.setTlsEnable(true);
    prod.setTlsOptions(tlsOptions);
    try {
        prod.start();
    } catch (const std::exception& e) {
        // start 就抛也行——只要根因可见就算数
        errText = e.what();
        std::cout << "captured (start): " << errText << "\n";
    }
    if (errText.empty()) {
        try {
            const SendResult r = prod.send(Message(topic, "neg-probe"));
            std::cout << "FAIL  send 竟然成功 status="
                      << (r.getSendStatus() == SendStatus::SEND_OK ? "SEND_OK" : "other")
                      << "（严格校验没生效？）\n";
            prod.shutdown();
            return 1;
        } catch (const std::exception& e) {
            errText = e.what();
            std::cout << "captured (send): " << errText << "\n";
        }
    }
    try { prod.shutdown(); } catch (...) {
    }

    const bool ok = errText.find("Can not find Message Queue") != std::string::npos
                 && errText.find("route fetch failed") != std::string::npos
                 && hasCertMarker(errText);
    std::cout << (ok ? "TLS_NEG_PROBE_CPP PASS" : "TLS_NEG_PROBE_CPP FAIL") << "\n";
    return ok ? 0 : 1;
}
