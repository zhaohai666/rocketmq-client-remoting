#ifndef ROCKETMQ_COMMON_TOP_ADDRESSING_H
#define ROCKETMQ_COMMON_TOP_ADDRESSING_H

#include <cstdint>
#include <map>
#include <mutex>
#include <string>

namespace rocketmq {

std::string clearNewLine(const std::string& content);

class DefaultTopAddressing {
 public:
    // 默认构造 = 动态取址关闭（wsAddr 为空，fetch 直接返回空串）。
    DefaultTopAddressing() = default;
    explicit DefaultTopAddressing(std::string wsAddr, std::string unitName = std::string(),
                                  std::map<std::string, std::string> para = std::map<std::string, std::string>(),
                                  int32_t timeoutMillis = 3000);

    static std::string getWsAddr(const std::string& domain, const std::string& subgroup = "nsaddr");
    // 动态取址是否已配置（ROCKETMQ_NAMESRV_DOMAIN 环境变量）
    static bool isConfigured();

    void setWsAddr(std::string wsAddr) { wsAddr_ = std::move(wsAddr); }
    const std::string& wsAddr() const { return wsAddr_; }

    void setUnitName(std::string unitName) { unitName_ = std::move(unitName); }
    const std::string& unitName() const { return unitName_; }

    void configureFromEnv(const std::string& unitName = std::string());

    // 对应 fetchNSAddr 里的 URL 拼装（unitName / para 规则逐条照抄）
    std::string buildUrl() const;

    std::string fetchNsAddr(bool verbose = true);

    std::string fetchAndApply();

 private:
    // 极简 HTTP GET：返回 HTTP 状态码，body 带出响应体。零第三方依赖。
    static int httpGet(const std::string& host, const std::string& port,
                       const std::string& path, int32_t timeoutMillis, std::string& body);

    std::string wsAddr_;
    std::string unitName_;
    std::map<std::string, std::string> para_;
    int32_t timeoutMillis_ = 3000;
    std::string nsAddr_;
    mutable std::mutex m_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_TOP_ADDRESSING_H
