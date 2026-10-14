#ifndef ROCKETMQ_COMMON_TOP_ADDRESSING_H
#define ROCKETMQ_COMMON_TOP_ADDRESSING_H

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <utility>

namespace rocketmq {

std::string clearNewLine(const std::string& content);

class DefaultTopAddressing {
 public:
    // Default construction = dynamic addressing disabled (wsAddr empty, fetch returns an empty string directly).
    DefaultTopAddressing() = default;
    explicit DefaultTopAddressing(std::string wsAddr, std::string unitName = std::string(),
                                  std::map<std::string, std::string> para = std::map<std::string, std::string>(),
                                  int32_t timeoutMillis = 3000);

    static std::string getWsAddr(const std::string& domain, const std::string& subgroup = "nsaddr");
    // Whether dynamic addressing is configured (ROCKETMQ_NAMESRV_DOMAIN environment variable)
    static bool isConfigured();

    void setWsAddr(std::string wsAddr) { wsAddr_ = std::move(wsAddr); }
    const std::string& wsAddr() const { return wsAddr_; }

    void setUnitName(std::string unitName) { unitName_ = std::move(unitName); }
    const std::string& unitName() const { return unitName_; }

    void configureFromEnv(const std::string& unitName = std::string());

    // URL assembly used when fetching the name server address (unitName / para rules followed item by item)
    std::string buildUrl() const;

    std::string fetchNsAddr(bool verbose = true);

    std::string fetchAndApply();

 private:
    // Minimal HTTP GET: returns the HTTP status code and writes the response body out. No third-party dependencies.
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
