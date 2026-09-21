// 动态 name server（DefaultTopAddressing）单测 —— 本地 mock HTTP server，不依赖外网。
//
// Java 语义锚点与 Python 侧 test_top_addressing.py 同源：
//   * WS 地址 / unitName / para 的 URL 拼装规则；
//   * clearNewLine（trim 后截断到第一个 \r 或 \n）；
//   * 200 → 地址串；非 200 / 连接失败 → 空串；
//   * fetchAndApply：地址**变化才应用**；
//   * MQClientInstance：配了静态地址不 fetch；空地址 fetch 一次并应用；取不到报错；
//     只有走动态取址时才起 10s/2min 的刷新线程（Java scheduleAtFixedRate）；
//   * configureFromEnv(ROCKETMQ_NAMESRV_DOMAIN)：实例构造时自己装配 wsAddr + unitName。
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#if defined(_WIN32)
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include "rocketmq/client/mq_client.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/common/top_addressing.h"

using namespace rocketmq;

#if defined(_WIN32)
static const int kShutdownHow = SD_BOTH;
#else
static const int kShutdownHow = SHUT_RDWR;
#endif

namespace {

int fails = 0;
int checks = 0;

void expect(bool ok, const std::string& name, const std::string& detail = "") {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("FAIL %s %s\n", name.c_str(), detail.c_str());
    }
}

void expectEq(const std::string& actual, const std::string& expected, const std::string& name) {
    ++checks;
    if (actual != expected) {
        ++fails;
        std::printf("FAIL %s (actual=[%s] expected=[%s])\n", name.c_str(), actual.c_str(),
                    expected.c_str());
    }
}

// 临时改环境变量、析构时还原（值设成空串即等价于"未设置"：实现按 env[0]=='\0' 判断）。
// ::setenv 是 POSIX 专有，MSVC 只有 _putenv_s。
class ScopedEnv {
 public:
    ScopedEnv(const char* name, const char* value) : name_(name) {
        const char* old = std::getenv(name);
        if (old != nullptr) oldValue_ = old;
        apply(name, value);
    }
    ~ScopedEnv() { apply(name_, hadOld() ? oldValue_.c_str() : ""); }

    ScopedEnv(const ScopedEnv&) = delete;
    ScopedEnv& operator=(const ScopedEnv&) = delete;

    bool hadOld() const { return !oldValue_.empty(); }

 private:
    void apply(const char* name, const char* value) {
#if defined(_WIN32)
        _putenv_s(name, value);
#else
        ::setenv(name, value, 1);
#endif
    }

    const char* name_;
    std::string oldValue_;
};

// ------------------------------------------------------------------ mock server

class MockAddrServer {
 public:
    MockAddrServer() {
        netcompat::ensureInitialized();
        listen_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(listen_, SOL_SOCKET, SO_REUSEADDR,
                     reinterpret_cast<const char*>(&one), sizeof(one));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;    // 系统分配
        ::bind(listen_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        netcompat::socklen_type len = sizeof(addr);
        ::getsockname(listen_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        ::listen(listen_, 8);
        thread_ = std::thread([this]() { loop(); });
    }

    ~MockAddrServer() {
        running_ = false;
        ::shutdown(listen_, kShutdownHow);
        netcompat::closeSocket(listen_);
        thread_.join();
    }

    int port() const { return port_; }

    void setResponse(int status, const std::string& body) {
        std::lock_guard<std::mutex> lk(m_);
        status_ = status;
        body_ = body;
    }

    int hitCount() {
        std::lock_guard<std::mutex> lk(m_);
        return hits_;
    }

 private:
    void loop() {
        running_ = true;
        while (running_) {
            sockaddr_in peer{};
            netcompat::socklen_type len = sizeof(peer);
            netcompat::socket_t conn = ::accept(listen_, reinterpret_cast<sockaddr*>(&peer), &len);
            if (!netcompat::isValid(conn)) break;
            handle(conn);
            netcompat::closeSocket(conn);
        }
    }

    void handle(netcompat::socket_t conn) {
        char buf[4096] = {0};
        if (::recv(conn, buf, sizeof(buf) - 1, 0) <= 0) return;
        std::lock_guard<std::mutex> lk(m_);
        ++hits_;
        const std::string head = "HTTP/1.0 " + std::to_string(status_)
            + (status_ == 200 ? " OK" : " ERR")
            + "\r\nContent-Length: " + std::to_string(body_.size())
            + "\r\nConnection: close\r\n\r\n";
        const std::string resp = head + body_;
        ::send(conn, resp.data(), static_cast<int>(resp.size()), netcompat::sendFlags());
    }

    netcompat::socket_t listen_;
    int port_ = 0;
    std::thread thread_;
    std::atomic<bool> running_{false};
    std::mutex m_;
    int status_ = 200;
    std::string body_ = "127.0.0.1:9876";
    int hits_ = 0;
};

std::string topAddrDomain(const MockAddrServer& s) {
    return "127.0.0.1:" + std::to_string(s.port());
}

// C++ 构造函数第一个参数是 **wsAddr**（与 Python 的 domain= 关键字不同），
// 这里统一用 getWsAddr 生成完整 URL。
std::string topAddrWs(const MockAddrServer& s) {
    return DefaultTopAddressing::getWsAddr(topAddrDomain(s));
}

// ------------------------------------------------------------------ 用例

void testGetWsAddr() {
    expectEq(DefaultTopAddressing::getWsAddr("jmenv.tbsite.net"),
             "http://jmenv.tbsite.net:8080/rocketmq/nsaddr", "default port appended");
    expectEq(DefaultTopAddressing::getWsAddr("host:12345"),
             "http://host:12345/rocketmq/nsaddr", "domain with port skips default");
    expectEq(DefaultTopAddressing::getWsAddr("h", "mygrp"),
             "http://h:8080/rocketmq/mygrp", "custom subgroup");
}

void testBuildUrl() {
    DefaultTopAddressing plain("http://h:8080/rocketmq/nsaddr");
    expectEq(plain.buildUrl(), "http://h:8080/rocketmq/nsaddr", "plain url");

    DefaultTopAddressing withUnit("http://h:8080/rocketmq/nsaddr", "unitA");
    expectEq(withUnit.buildUrl(), "http://h:8080/rocketmq/nsaddr-unitA?nofix=1", "unit name");

    std::map<std::string, std::string> para;
    para["k"] = "v";
    DefaultTopAddressing unitPara("http://h:8080/rocketmq/nsaddr", "u", para);
    expectEq(unitPara.buildUrl(), "http://h:8080/rocketmq/nsaddr-u?nofix=1&k=v",
             "unit name with para");

    DefaultTopAddressing blankUnit("http://h:8080/rocketmq/nsaddr", "   ");
    expectEq(blankUnit.buildUrl(), "http://h:8080/rocketmq/nsaddr", "blank unit ignored");
}

void testClearNewLine() {
    expectEq(clearNewLine("  1.2.3.4:9876\r\nrest"), "1.2.3.4:9876", "cut at cr");
    expectEq(clearNewLine("a:9876\nb:9877"), "a:9876", "cut at lf");
    expectEq(clearNewLine("  a:9876  "), "a:9876", "trim only");
    expectEq(clearNewLine("\r\n"), "", "empty");
}

void testFetchBehavior(MockAddrServer& s) {
    s.setResponse(200, "10.0.0.1:9876;10.0.0.2:9876\nextra");
    DefaultTopAddressing ta(topAddrWs(s));
    expectEq(ta.fetchNsAddr(false), "10.0.0.1:9876;10.0.0.2:9876", "200 returns cleared body");

    s.setResponse(500, "ignored");
    expectEq(ta.fetchNsAddr(false), "", "non-200 returns empty");

    DefaultTopAddressing dead("http://127.0.0.1:1/rocketmq/nsaddr", std::string(),
                              std::map<std::string, std::string>(), /*timeoutMillis=*/300);
    expectEq(dead.fetchNsAddr(false), "", "connection error returns empty");
}

void testFetchAndApply(MockAddrServer& s) {
    s.setResponse(200, "127.0.0.1:9876");
    DefaultTopAddressing ta(topAddrWs(s));
    expectEq(ta.fetchAndApply(), "127.0.0.1:9876", "first change applied");
    expectEq(ta.fetchAndApply(), "", "same addr not applied again");
    s.setResponse(200, "10.0.0.9:9876");
    expectEq(ta.fetchAndApply(), "10.0.0.9:9876", "changed addr applied");
}

void testMqClientIntegration(MockAddrServer& s) {
    // 配了静态地址 → 不该问地址服务器（hits 用差值比较，前面用例已经打过它）
    const int baseline = s.hitCount();
    {
        MQClientInstance mqc("c@cppdyn", {"127.0.0.1:9876"});
        mqc.topAddressing().setWsAddr(topAddrWs(s));
        mqc.start();
        mqc.shutdown();
        expectEq(s.hitCount() == baseline ? "0" : "n", "0", "static addrs never fetch");
    }
    // 空地址 → start 时 fetch 一次并应用
    s.setResponse(200, "127.0.0.1:9876");
    {
        MQClientInstance mqc("c@cppdyn2", {});
        mqc.topAddressing().setWsAddr(topAddrWs(s));
        mqc.start();
        const std::vector<std::string> addrs = mqc.nameServerAddrs();
        expect(addrs.size() == 1 && addrs[0] == "127.0.0.1:9876",
               "empty addrs fetched at start", "size=" + std::to_string(addrs.size()));
        expectEq(s.hitCount() > 0 ? "y" : "n", "y", "address server hit");
        mqc.shutdown();
    }
    // 地址服务器取不到 → start 报错
    s.setResponse(500, "");
    {
        MQClientInstance mqc("c@cppdyn3", {});
        mqc.topAddressing().setWsAddr(topAddrWs(s));
        bool threw = false;
        try {
            mqc.start();
        } catch (const std::exception&) {
            threw = true;
        }
        expect(threw, "start fails when address server returns none");
    }
}

void testDefaultConstructedIsDisabled() {
    DefaultTopAddressing ta;
    expect(ta.wsAddr().empty(), "default ctor has no ws addr");
    expectEq(ta.fetchNsAddr(false), "", "disabled fetch returns empty");
}

// configureFromEnv：对应 Java MQClientAPIImpl 构造里的 `new DefaultTopAddressing(unitName)`
// —— 那个构造函数自己读域名（Java 是系统属性，本端口统一用环境变量
// ROCKETMQ_NAMESRV_DOMAIN，与 python/rust/dotnet 三端口一致）。
// 这条链路此前只能靠调用方手工 setWsAddr 才通，实例自己装配是缺的。
void testConfigureFromEnv(MockAddrServer& s) {
    const std::string key = "ROCKETMQ_NAMESRV_DOMAIN";
    // 1) 未配置 = 完全 no-op（不能让没设 env 的用户白等 3s 域名解析）
    {
        ScopedEnv off(key.c_str(), "");
        expect(!DefaultTopAddressing::isConfigured(), "env 未设置时 isConfigured=false");
        DefaultTopAddressing ta;
        ta.configureFromEnv("unitA");
        expect(ta.wsAddr().empty(), "env 未设置 → 不改 wsAddr");
        expect(ta.unitName().empty(), "env 未设置 → 不记 unitName");
        expectEq(ta.fetchNsAddr(false), "", "env 未设置时 fetch 仍是 no-op");
    }
    // 2) 配置了 domain：wsAddr 按 MixAll.getWSAddr 拼，unitName 进 URL
    s.setResponse(200, "127.0.0.1:9876");
    {
        ScopedEnv on(key.c_str(), topAddrDomain(s).c_str());
        expect(DefaultTopAddressing::isConfigured(), "env 设置后 isConfigured=true");
        DefaultTopAddressing ta;
        ta.configureFromEnv("unitA");
        expectEq(ta.wsAddr(), topAddrWs(s), "configureFromEnv 拼出 wsAddr");
        expectEq(ta.unitName(), "unitA", "unitName 落到地址服务器配置");
        // domain 自带端口 ⇒ 不追加 :8080；unitName 段是 "-<unit>?nofix=1"
        expectEq(ta.buildUrl(), topAddrWs(s) + "-unitA?nofix=1", "env + unitName 的 URL");
        expectEq(ta.fetchNsAddr(false), "127.0.0.1:9876", "env 驱动的 fetch 取到地址");
    }
    // 3) MQClientInstance 构造时自己装配：facade 不用手工 setWsAddr
    {
        ScopedEnv on(key.c_str(), topAddrDomain(s).c_str());
        MQClientInstance mqc("c@cppenv", {}, 3000, 15000, false, "unitZ");
        expectEq(mqc.topAddressing().wsAddr(), topAddrWs(s), "实例从 env 装好 wsAddr");
        expectEq(mqc.topAddressing().unitName(), "unitZ", "实例把 unitName 传给取址配置");
        mqc.start();
        const std::vector<std::string> addrs = mqc.nameServerAddrs();
        expect(addrs.size() == 1 && addrs[0] == "127.0.0.1:9876",
               "env 驱动的实例 start 时取到地址", "addrs=" + std::to_string(addrs.size()));
        mqc.shutdown();
    }
    // 4) 没有 env 时实例保持关闭动态取址，也不碰地址服务器
    {
        ScopedEnv off(key.c_str(), "");
        MQClientInstance mqc("c@cppenv2", {"127.0.0.1:9876"});
        expect(mqc.topAddressing().wsAddr().empty(), "无 env 时实例 wsAddr 为空");
        const int before = s.hitCount();
        mqc.start();
        mqc.shutdown();
        expectEq(std::to_string(s.hitCount() - before), "0", "无 env 时不请求地址服务器");
    }
}

}  // namespace

int main() {
    testGetWsAddr();
    testBuildUrl();
    testClearNewLine();
    {
        MockAddrServer s;
        testFetchBehavior(s);
        testFetchAndApply(s);
        testMqClientIntegration(s);
    }
    testDefaultConstructedIsDisabled();
    {
        MockAddrServer s;
        testConfigureFromEnv(s);
    }

    std::printf("%s: %d checks, %d failed\n", fails == 0 ? "PASS" : "FAIL", checks, fails);
    return fails == 0 ? 0 : 1;
}
