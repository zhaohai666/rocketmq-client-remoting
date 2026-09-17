#include "rocketmq/common/top_addressing.h"

#include <cstdio>
#include <cstdlib>
#include <utility>

#include "rocketmq/common/logging.h"
#include "rocketmq/common/net_compat.h"

namespace rocketmq {

namespace {

std::string trimCopy(const std::string& s) {
    size_t b = 0;
    size_t e = s.size();
    while (b < e && std::isspace(static_cast<unsigned char>(s[b]))) ++b;
    while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1]))) --e;
    return s.substr(b, e - b);
}

// 从 "http://host:port/path" 拆出 host / port / path。port 缺省 80，path 缺省 "/"。
bool splitHttpUrl(const std::string& url, std::string& host, std::string& port,
                  std::string& path) {
    static const std::string kScheme = "http://";
    if (url.rfind(kScheme, 0) != 0) return false;
    const std::string rest = url.substr(kScheme.size());
    const size_t slash = rest.find('/');
    const std::string authority = slash == std::string::npos ? rest : rest.substr(0, slash);
    path = slash == std::string::npos ? "/" : rest.substr(slash);
    const size_t colon = authority.rfind(':');
    if (colon == std::string::npos) {
        host = authority;
        port = "80";
    } else {
        host = authority.substr(0, colon);
        port = authority.substr(colon + 1);
    }
    return !host.empty() && !port.empty();
}

}  // namespace

std::string clearNewLine(const std::string& content) {
    const std::string s = trimCopy(content);
    const size_t cr = s.find('\r');
    if (cr != std::string::npos) return s.substr(0, cr);
    const size_t lf = s.find('\n');
    if (lf != std::string::npos) return s.substr(0, lf);
    return s;
}

DefaultTopAddressing::DefaultTopAddressing(std::string wsAddr, std::string unitName,
                                           std::map<std::string, std::string> para,
                                           int32_t timeoutMillis)
    : wsAddr_(std::move(wsAddr)),
      unitName_(std::move(unitName)),
      para_(std::move(para)),
      timeoutMillis_(timeoutMillis) {}

std::string DefaultTopAddressing::getWsAddr(const std::string& domain,
                                            const std::string& subgroup) {
    // Java MixAll.getWSAddr：domain 自带端口（含 ':'）时不追加默认 :8080
    if (domain.find(':') != std::string::npos) {
        return "http://" + domain + "/rocketmq/" + subgroup;
    }
    return "http://" + domain + ":8080/rocketmq/" + subgroup;
}

bool DefaultTopAddressing::isConfigured() {
    const char* env = std::getenv("ROCKETMQ_NAMESRV_DOMAIN");
    return env != nullptr && env[0] != '\0';
}

std::string DefaultTopAddressing::buildUrl() const {
    // Java fetchNSAddr 的 URL 拼装（unitName / para 规则逐条照抄）
    std::string url = wsAddr_;
    if (!para_.empty()) {
        if (!trimCopy(unitName_).empty()) {
            url += "-" + unitName_ + "?nofix=1&";
        } else {
            url += "?";
        }
        bool first = true;
        for (const auto& kv : para_) {
            if (!first) url += "&";
            url += kv.first + "=" + kv.second;
            first = false;
        }
    } else if (!trimCopy(unitName_).empty()) {
        url += "-" + unitName_ + "?nofix=1";
    }
    return url;
}

std::string DefaultTopAddressing::fetchNsAddr(bool verbose) {
    if (wsAddr_.empty()) {
        return std::string();
    }
    const std::string url = buildUrl();
    std::string host;
    std::string port;
    std::string path;
    if (!splitHttpUrl(url, host, port, path)) {
        if (verbose) {
            logger_error("fetch nameserver address failed, bad url: " + url);
        }
        return std::string();
    }
    std::string body;
    const int status = httpGet(host, port, path, timeoutMillis_, body);
    if (status == 200) {
        return clearNewLine(body);
    }
    if (verbose) {
        logger_error("fetch nameserver address failed. statusCode=" + std::to_string(status)
                     + " url=" + url);
    }
    return std::string();
}

std::string DefaultTopAddressing::fetchAndApply() {
    // Java MQClientAPIImpl.fetchNameServerAddr：地址**变化才应用**
    std::lock_guard<std::mutex> lk(m_);
    const std::string addrs = fetchNsAddr();
    if (!trimCopy(addrs).empty() && addrs != nsAddr_) {
        logger_info("name server address changed, old=" + nsAddr_ + ", new=" + addrs);
        nsAddr_ = addrs;
        return nsAddr_;
    }
    return std::string();
}

int DefaultTopAddressing::httpGet(const std::string& host, const std::string& port,
                                  const std::string& path, int32_t timeoutMillis,
                                  std::string& body) {
    netcompat::ensureInitialized();
    ::addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    ::addrinfo* res = nullptr;
    if (::getaddrinfo(host.c_str(), port.c_str(), &hints, &res) != 0 || res == nullptr) {
        return -1;
    }
    netcompat::socket_t sock = netcompat::kInvalidSocket;
    for (::addrinfo* ai = res; ai != nullptr; ai = ai->ai_next) {
        sock = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (!netcompat::isValid(sock)) continue;
        netcompat::tuneSocket(sock);
        if (::connect(sock, ai->ai_addr, static_cast<netcompat::socklen_type>(ai->ai_addrlen)) == 0) {
            break;
        }
        netcompat::closeSocket(sock);
        sock = netcompat::kInvalidSocket;
    }
    ::freeaddrinfo(res);
    if (!netcompat::isValid(sock)) {
        return -1;
    }

    // 收发整体一个超时预算（简化：SO_RCVTIMEO/SO_SNDTIMEO 按 timeoutMillis）
    timeval tv{};
    tv.tv_sec = timeoutMillis / 1000;
    tv.tv_usec = (timeoutMillis % 1000) * 1000;
    ::setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&tv), sizeof(tv));
    ::setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&tv), sizeof(tv));

    const std::string req = "GET " + path
        + " HTTP/1.0\r\nAccept: */*\r\nHost: " + host + ":" + port
        + "\r\nConnection: close\r\n\r\n";
    size_t sent = 0;
    while (sent < req.size()) {
        const int n = ::send(sock, req.data() + sent,
                             static_cast<int>(req.size() - sent), netcompat::sendFlags());
        if (n <= 0) {
            netcompat::closeSocket(sock);
            return -1;
        }
        sent += static_cast<size_t>(n);
    }

    std::string raw;
    char buf[4096];
    for (;;) {
        const int n = ::recv(sock, buf, sizeof(buf), 0);
        if (n <= 0) break;
        raw.append(buf, static_cast<size_t>(n));
        if (raw.size() > 1024u * 1024u) break;   // 1MB 上限，防异常服务器拖死客户端
    }
    netcompat::closeSocket(sock);

    // 解析状态行 + 头，取 body
    const size_t headEnd = raw.find("\r\n\r\n");
    if (headEnd == std::string::npos) return -1;
    const std::string statusLine = raw.substr(0, raw.find("\r\n"));
    int code = -1;
    // "HTTP/1.x 200 OK"
    if (statusLine.rfind("HTTP/", 0) == 0) {
        const size_t sp = statusLine.find(' ');
        if (sp != std::string::npos) {
            code = std::atoi(statusLine.c_str() + sp + 1);
        }
    }
    body = raw.substr(headEnd + 4);
    // HTTP/1.1 chunked：这里请求用 HTTP/1.0（服务器不会分块），防御性再剥一次
    if (raw.find("Transfer-Encoding: chunked") != std::string::npos) {
        std::string out;
        size_t i = 0;
        while (i < body.size()) {
            const size_t lineEnd = body.find("\r\n", i);
            if (lineEnd == std::string::npos) break;
            const int chunkLen = std::strtol(body.substr(i, lineEnd - i).c_str(), nullptr, 16);
            if (chunkLen <= 0) break;
            out.append(body, lineEnd + 2, static_cast<size_t>(chunkLen));
            i = lineEnd + 2 + static_cast<size_t>(chunkLen) + 2;
        }
        body = out;
    }
    return code;
}

}  // namespace rocketmq
