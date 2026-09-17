// 动态 name server 取址（对应 org.apache.rocketmq.common.namesrv.TopAddressing /
// DefaultTopAddressing 与 MixAll.getWSAddr；Python 参考实现 client/top_addressing.py）。
//
// Java 语义（5.5.1 逐条核对，与 Python 侧逐条同源）：
//   * WS 地址：``http://<domain>:8080/rocketmq/<subgroup>``；domain 自带端口（含 ':'）
//     时不追加 :8080；
//   * unitName 非空白 → URL 追加 ``-<unitName>?nofix=1``；para 非空 → ``?k=v&...``；
//   * HTTP GET（超时 3000ms）code==200 → 响应体 clearNewLine（trim 后截断到第一个
//     \r 或 \n）作为 NS 地址串；失败返回空串；
//   * fetchAndApply（Java MQClientAPIImpl.fetchNameServerAddr）：地址**变化才应用**；
//   * MQClientInstance：只在未配置静态地址时，start() fetch 一次 + 10s/2min 周期刷新。
//
// 有意差异（与 Python 参考实现一致）：Java 默认 domain 是 jmenv.tbsite.net（依赖
// /etc/hosts 绑定），我们若照抄，未配置的用户会被必然失败的域名拖 3s。因此 domain
// 必须显式给出（构造参数或环境变量 ROCKETMQ_NAMESRV_DOMAIN），未配置 = 关闭动态取址。
#ifndef ROCKETMQ_COMMON_TOP_ADDRESSING_H
#define ROCKETMQ_COMMON_TOP_ADDRESSING_H

#include <cstdint>
#include <map>
#include <mutex>
#include <string>

namespace rocketmq {

// Java DefaultTopAddressing.clearNewLine：trim 后截断到第一个 \r 或 \n。
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

    // 对应 fetchNSAddr 里的 URL 拼装（unitName / para 规则逐条照抄）
    std::string buildUrl() const;

    // 取一次 NS 地址串；不可用 / 非 200 / 网络失败返回空串（Java 返回 null）。
    std::string fetchNsAddr(bool verbose = true);

    // Java MQClientAPIImpl.fetchNameServerAddr：地址变化才返回新串并记录，否则空串。
    std::string fetchAndApply();

 private:
    // 极简 HTTP GET：返回 HTTP 状态码，body 带出响应体。零第三方依赖。
    static int httpGet(const std::string& host, const std::string& port,
                       const std::string& path, int32_t timeoutMillis, std::string& body);

    std::string wsAddr_;
    std::string unitName_;
    std::map<std::string, std::string> para_;
    int32_t timeoutMillis_ = 3000;
    // Java 的 nameSrvAddr 缓存：上次成功应用的地址串
    std::string nsAddr_;
    mutable std::mutex m_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_TOP_ADDRESSING_H
