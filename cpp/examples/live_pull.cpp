// 主动拉取消费者（DefaultMQPullConsumer）真机验证。
// 用法：rmq_live_pull 127.0.0.1:9876
//
// 与 Python verify_pull_live.py 完全同场景（三语言对拍用同一套断言）。
//
// 场景（拉模式的核心是「调用方自己拉、自己管位点」，断言都围绕这一点）：
//   S1 建 topic + fetchSubscribeMessageQueues → 拿到 4 个队列
//   S2 生产 12 条 → 每队列 min/max offset 差值 = 3（消息均匀落到 4 队列）
//   S3 手动拉取：逐队列从 min offset 拉到 max offset → 收全 12 条且 body 与发送集合一致
//   S4 手动提交位点：updateConsumeOffset → fetchConsumeOffset 回读一致（broker 往返）
//   S5 位点由调用方掌控：从已提交位点再拉 → NO_NEW_MSG；把位点退回 min 再拉 → FOUND
//      （push 模式做不到这一点，这正是 pull 模式的存在意义）
//   S6 searchOffset(now) / earliestMsgStoreTime → 均 > 0
//   S7 sendMessageBack → 消息落到 %RETRY%group，可被拉取到（回投链路真实可用）
//
// ⚠ 两个踩过的坑（不要"顺手优化"掉）：
//   1. 必须**先建 topic 再取队列**：消费者不做默认 topic 兜底，topic 不存在就拿不到路由；
//   2. 生产完**不能立刻查 maxOffset**：broker 的 consumequeue 是异步分发的，会读到 0
//      （探针实测），必须轮询到各队列 max-min 之和到位。
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <iterator>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <typeinfo>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/pull_consumer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/util_all.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
        std::printf("  [PASS] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    } else {
        ++gFail;
        std::printf("  [FAIL] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    }
}

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

std::vector<std::string> splitAddrs(const std::string& addr) {
    std::vector<std::string> out;
    std::string cur;
    for (char c : addr) {
        if (c == ';' || c == ',') {
            if (!cur.empty()) out.push_back(cur);
            cur.clear();
        } else if (c != ' ') {
            cur.push_back(c);
        }
    }
    if (!cur.empty()) out.push_back(cur);
    return out;
}

std::string bodyOf(const MessageExt& m) { return std::string(m.body.begin(), m.body.end()); }

// 与 Python 的 set(dict) 等价：MessageQueue 需要可比较才能做 map 键（C++ 用 offsetKey）。
std::string qKey(const MessageQueue& q) {
    return q.brokerName + ":" + std::to_string(q.queueId);
}

std::string join(const std::set<std::string>& s) {
    std::string out;
    for (const std::string& v : s) {
        out += v + " ";
    }
    return out;
}

}  // namespace

int main(int argc, char** argv) {
    const std::string namesrv = argc > 1 ? argv[1] : std::string("127.0.0.1:9876");
    const std::vector<std::string> nsAddrs = splitAddrs(namesrv);

    const std::string stamp = std::to_string(nowMs());
    const std::string topic = "PullLiveCpp_" + stamp;
    const std::string group = "PG_PullLiveCpp_" + stamp;
    const std::string retryTopic = MixAll::getRetryTopic(group);
    const int32_t nMsg = 12;
    const int32_t queueNum = 4;
    const int32_t perQueue = nMsg / queueNum;

    std::printf("======================================================================\n");
    std::printf("PullConsumer live (C++): namesrv=%s topic=%s group=%s\n", namesrv.c_str(),
                topic.c_str(), group.c_str());
    std::printf("======================================================================\n");

    // ---------------- 建 topic ----------------
    {
        DefaultMQProducer prep("PG_PrepareCpp_" + stamp);
        prep.setNamesrvAddr(namesrv);
        prep.start();
        try {
            prep.createTopic("TBW102", topic, queueNum);
        } catch (const std::exception& e) {
            std::printf("!! createTopic failed: %s\n", e.what());
        }
        prep.shutdown();
    }

    DefaultMQPullConsumer consumer(group);
    consumer.setNamesrvAddr(namesrv);

    // ---------------- S1 队列 ----------------
    std::printf("\nS1 建 topic + fetchSubscribeMessageQueues\n");
    std::vector<MessageQueue> routes;
    {
        consumer.start();
        const int64_t deadline = nowMs() + 20000;
        std::string last = "not tried";
        while (nowMs() < deadline) {
            try {
                routes = consumer.fetchSubscribeMessageQueues(topic);
                if (static_cast<int32_t>(routes.size()) >= queueNum) break;
                last = std::to_string(routes.size()) + " queues";
            } catch (const std::exception& e) {
                last = std::string(typeid(e).name()) + ": " + e.what();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        if (static_cast<int32_t>(routes.size()) != queueNum) {
            check("S1 拿到 " + std::to_string(queueNum) + " 个队列", false,
                  "got=" + std::to_string(routes.size()) + " last=" + last);
            std::printf("\nPullConsumer: PASS=%d FAIL=%d\n", gPass, gFail);
            return 1;
        }
    }
    check("S1 拿到 " + std::to_string(queueNum) + " 个队列", true,
          "got=" + std::to_string(routes.size()));
    {
        bool namesOk = true;
        for (const MessageQueue& q : routes) {
            if (q.topic != topic || q.brokerName.empty()) namesOk = false;
        }
        check("S1 队列 topic 与 broker 名非空", namesOk,
              "sample=" + routes[0].brokerName + ":" + std::to_string(routes[0].queueId));
    }

    // ---------------- S2 生产 ----------------
    std::printf("\nS2 生产 %d 条\n", nMsg);
    std::set<std::string> sent;
    {
        DefaultMQProducer prod("PG_PullLiveCpp_" + stamp);
        prod.setNamesrvAddr(namesrv);
        prod.start();
        int32_t sentOk = 0;
        for (int i = 0; i < nMsg; ++i) {
            char buf[32];
            std::snprintf(buf, sizeof(buf), "pull-%02d", i);
            const std::string body(buf);
            try {
                Message msg(topic, body);
                msg.setKeys(std::string("pull-key-") + buf);
                SendResult r = prod.send(msg);
                if (r.getSendStatus() == SendStatus::SEND_OK) {
                    ++sentOk;
                    sent.insert(body);
                }
            } catch (const std::exception& e) {
                std::printf("   send %d failed: %s\n", i, e.what());
            }
        }
        check("S2 生产 " + std::to_string(nMsg) + " 条成功", sentOk == nMsg,
              "sentOk=" + std::to_string(sentOk));
        prod.shutdown();
    }

    // 轮询等 consumequeue 分发落地（不能刚发完就查 maxOffset）
    std::map<std::string, int64_t> lo;
    std::map<std::string, int64_t> hi;
    {
        const int64_t deadline = nowMs() + 25000;
        while (nowMs() < deadline) {
            lo.clear();
            hi.clear();
            int64_t total = 0;
            for (const MessageQueue& q : routes) {
                lo[qKey(q)] = consumer.minOffset(q);
                hi[qKey(q)] = consumer.maxOffset(q);
                total += std::max<int64_t>(0, hi[qKey(q)] - lo[qKey(q)]);
            }
            if (total >= nMsg) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }
    for (const MessageQueue& q : routes) {
        const int64_t diff = hi[qKey(q)] - lo[qKey(q)];
        check("S2 队列 " + qKey(q) + " 有 " + std::to_string(perQueue) + " 条", diff == perQueue,
              "min=" + std::to_string(lo[qKey(q)]) + " max=" + std::to_string(hi[qKey(q)]));
    }

    // ---------------- S3 手动拉取 ----------------
    std::printf("\nS3 手动拉取（逐队列 min -> max）\n");
    std::set<std::string> got;
    {
        for (const MessageQueue& q : routes) {
            int64_t offset = lo[qKey(q)];
            const int64_t end = hi[qKey(q)];
            int guard = 0;
            while (offset < end && guard < 64) {
                ++guard;
                PullResult r;
                try {
                    r = consumer.pull(q, "*", offset, 32, 5000);
                } catch (const std::exception& e) {
                    check("S3 拉取 " + qKey(q) + " 异常", false, e.what());
                    break;
                }
                if (r.status == PullStatus::FOUND) {
                    for (const MessageExt& m : r.msgFoundList) {
                        got.insert(bodyOf(m));
                    }
                    if (r.nextBeginOffset <= offset) break;
                    offset = r.nextBeginOffset;
                } else if (r.status == PullStatus::NO_NEW_MSG) {
                    break;
                } else if (r.status == PullStatus::OFFSET_ILLEGAL) {
                    break;
                } else {
                    check("S3 拉取 " + qKey(q) + " 状态异常", false,
                          std::string("status=") + pullStatusName(r.status));
                    break;
                }
            }
        }
        std::set<std::string> missing;
        std::set<std::string> extra;
        std::set_difference(sent.begin(), sent.end(), got.begin(), got.end(),
                            std::inserter(missing, missing.begin()));
        std::set_difference(got.begin(), got.end(), sent.begin(), sent.end(),
                            std::inserter(extra, extra.begin()));
        check("S3 手动拉取收全 " + std::to_string(nMsg) + " 条且内容一致", got == sent,
              "got=" + std::to_string(got.size()) + " missing=[" + join(missing) + "] extra=[" +
                  join(extra) + "]");
    }

    if (got.empty()) {
        std::printf("\n!! 一条都没拉到，后续场景跳过\n");
        std::printf("\nPullConsumer: PASS=%d FAIL=%d\n", gPass, gFail);
        consumer.shutdown();
        return 1;
    }

    // ---------------- S4 手动提交位点 ----------------
    std::printf("\nS4 手动提交位点并回读\n");
    {
        const MessageQueue& q0 = routes[0];
        const int64_t target = hi[qKey(q0)];
        consumer.updateConsumeOffset(q0, target);
        int64_t back = -1;
        const bool haveOffset = consumer.fetchConsumeOffset(q0, back);
        check("S4 位点提交后回读一致", haveOffset && back == target,
              "committed=" + std::to_string(back) + " target=" + std::to_string(target));
    }

    // ---------------- S5 位点由调用方掌控 ----------------
    std::printf("\nS5 位点由调用方掌控\n");
    {
        const MessageQueue& q0 = routes[0];
        const int64_t committed = hi[qKey(q0)];
        PullResult r1 = consumer.pull(q0, "*", committed, 32, 5000);
        check("S5 从已提交位点再拉 = NO_NEW_MSG",
              r1.status == PullStatus::NO_NEW_MSG && r1.msgFoundList.empty(),
              std::string("status=") + pullStatusName(r1.status) +
                  " n=" + std::to_string(r1.msgFoundList.size()));

        PullResult r2 = consumer.pull(q0, "*", lo[qKey(q0)], 32, 5000);
        check("S5 位点退回 min 后可重拉（pull 模式的核心能力）",
              r2.status == PullStatus::FOUND && !r2.msgFoundList.empty(),
              std::string("status=") + pullStatusName(r2.status) +
                  " n=" + std::to_string(r2.msgFoundList.size()));
    }

    // ---------------- S6 位点查询 ----------------
    std::printf("\nS6 searchOffset / earliestMsgStoreTime / min/max\n");
    {
        const MessageQueue& q0 = routes[0];
        const int64_t so = consumer.searchOffset(q0, UtilAll::currentTimeMillis());
        check("S6 searchOffset(now) > 0", so > 0, "searchOffset=" + std::to_string(so));
        const int64_t emst = consumer.earliestMsgStoreTime(q0);
        check("S6 earliestMsgStoreTime > 0", emst > 0, "earliest=" + std::to_string(emst));
        check("S6 minOffset <= maxOffset", lo[qKey(q0)] <= hi[qKey(q0)],
              "min=" + std::to_string(lo[qKey(q0)]) + " max=" + std::to_string(hi[qKey(q0)]));
    }

    // ---------------- S7 回投 ----------------
    std::printf("\nS7 sendMessageBack -> %%RETRY%%group 可拉取\n");
    {
        MessageExt sample;
        bool haveSample = false;
        for (const MessageQueue& q : routes) {
            PullResult r = consumer.pull(q, "*", lo[qKey(q)], 1, 5000);
            if (!r.msgFoundList.empty()) {
                sample = r.msgFoundList[0];
                haveSample = true;
                break;
            }
        }
        if (!haveSample) {
            check("S7 取样本消息", false, "no message available");
        } else {
            const std::string want = bodyOf(sample);
            try {
                consumer.sendMessageBack(sample, 0);
                check("S7 回投请求被 broker 接受", true,
                      "offset=" + std::to_string(sample.commitLogOffset) + " body=" + want);
            } catch (const std::exception& e) {
                check("S7 回投请求被 broker 接受", false, e.what());
                std::printf("\nPullConsumer: PASS=%d FAIL=%d\n", gPass, gFail);
                consumer.shutdown();
                return gFail == 0 ? 0 : 1;
            }

            bool found = false;
            std::string detail = "not tried";
            const int64_t deadline = nowMs() + 40000;
            while (nowMs() < deadline && !found) {
                std::vector<MessageQueue> rqs;
                try {
                    rqs = consumer.fetchSubscribeMessageQueues(retryTopic);
                } catch (const std::exception& e) {
                    detail = std::string("retry topic not routable yet: ") + e.what();
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    continue;
                }
                for (const MessageQueue& rq : rqs) {
                    int64_t rlo = 0;
                    int64_t rhi = 0;
                    try {
                        rlo = consumer.minOffset(rq);
                        rhi = consumer.maxOffset(rq);
                    } catch (const std::exception& e) {
                        detail = e.what();
                        continue;
                    }
                    if (rhi <= rlo) continue;
                    PullResult r = consumer.pull(rq, "*", rlo, 32, 5000);
                    for (const MessageExt& m : r.msgFoundList) {
                        if (bodyOf(m) == want) {
                            found = true;
                            detail = "queue=" + std::to_string(rq.queueId) +
                                     " reconsumeTimes=" + std::to_string(m.reconsumeTimes) +
                                     " body=" + bodyOf(m);
                            break;
                        }
                    }
                    if (found) break;
                }
                if (!found) std::this_thread::sleep_for(std::chrono::seconds(1));
            }
            check("S7 %RETRY% 拉到了被回投的消息", found, detail);
        }
    }

    consumer.shutdown();

    std::printf("\nPullConsumer: PASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
