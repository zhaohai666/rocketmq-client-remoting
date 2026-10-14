// #25 negative probe (producer only, no consumer): trusting the [wrong CA] under strict CA verification,
// the producer's route layer must not swallow the TLS handshake exception.
//   Before the fix: send only reports "Can not find Message Queue for topic: X" (a black box;
//           the user looks for "create the topic" while the certificate/CA is what is broken).
//   After the fix: the error must carry "(route fetch failed: <cert/CA error text>)".
// Usage: rmq_live_tls_negative <namesrv> <topic> <wrongCaCert> [serverName]
// Exit codes: 0 = the error carries a CA marker (as expected); 1 = it did not fail as expected; 2 = bad usage.
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
        // Throwing already at start is fine too -- what counts is that the root cause stays visible
        errText = e.what();
        std::cout << "captured (start): " << errText << "\n";
    }
    if (errText.empty()) {
        try {
            const SendResult r = prod.send(Message(topic, "neg-probe"));
            std::cout << "FAIL  send unexpectedly succeeded status="
                      << (r.getSendStatus() == SendStatus::SEND_OK ? "SEND_OK" : "other")
                      << "(strict verification did not take effect?)\n";
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
