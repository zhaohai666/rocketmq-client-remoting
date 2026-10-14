// Static topic routing: typed parsing + endpoint derivation (see the wire-format facts in static_topic.h).
#include "rocketmq/client/static_topic.h"

#include <algorithm>
#include <exception>

namespace rocketmq {
namespace static_topic {

bool parseTopicQueueMappingInfo(const JsonValue& raw, TopicQueueMappingInfo& out) {
    if (!raw.isObject()) {
        return false;
    }
    out = TopicQueueMappingInfo();
    const JsonValue* scope = raw.find("scope");
    if (scope != nullptr && scope->isString()) {
        out.scope = scope->stringValue();
    }
    const JsonValue* totalQueues = raw.find("totalQueues");
    if (totalQueues != nullptr && totalQueues->isNumber()) {
        out.totalQueues = static_cast<int32_t>(totalQueues->intValue());
    }
    const JsonValue* bname = raw.find("bname");
    if (bname != nullptr && bname->isString()) {
        out.bname = bname->stringValue();
    }
    const JsonValue* epoch = raw.find("epoch");
    if (epoch != nullptr && epoch->isNumber()) {
        out.epoch = epoch->intValue();
    }
    const JsonValue* currIdMap = raw.find("currIdMap");
    if (currIdMap != nullptr && currIdMap->isObject()) {
        for (const auto& kv : currIdMap->objectItems()) {
            // Keys are decimal globalId strings; unparsable entries are skipped (lenient parsing, so one bad record cannot break the whole mapping).
            try {
                std::size_t consumed = 0;
                const int gid = std::stoi(kv.first, &consumed);
                if (consumed != kv.first.size() || !kv.second.isNumber()) {
                    continue;
                }
                out.currIdMap[gid] = static_cast<int32_t>(kv.second.intValue());
            } catch (const std::exception&) {
                continue;
            }
        }
    }
    return true;
}

bool topicRouteData2EndpointsForStaticTopic(
    const std::string& topic, const TopicRouteData& route,
    std::map<MessageQueue, std::string>& out) {
    if (route.topicQueueMappingByBroker.isNull()) {
        return false;
    }
    if (!route.topicQueueMappingByBroker.isObject() ||
        route.topicQueueMappingByBroker.objectItems().empty()) {
        return false;
    }

    std::map<std::string, std::vector<std::pair<std::string, TopicQueueMappingInfo>>> infosByScope;
    for (const auto& entry : route.topicQueueMappingByBroker.objectItems()) {
        TopicQueueMappingInfo info;
        if (!parseTopicQueueMappingInfo(entry.second, info)) {
            continue;
        }
        infosByScope[info.scope].emplace_back(entry.first, info);
    }

    std::map<MessageQueue, std::string> endpoints;
    for (auto& scopeEntry : infosByScope) {
        const std::string& scope = scopeEntry.first;
        auto& infos = scopeEntry.second;
        std::stable_sort(infos.begin(), infos.end(),
                         [](const std::pair<std::string, TopicQueueMappingInfo>& a,
                            const std::pair<std::string, TopicQueueMappingInfo>& b) {
                             return a.second.epoch > b.second.epoch;
                         });

        int32_t maxTotalNums = 0;
        const int64_t maxTotalNumOfEpoch = -1;
        const std::string mockName = getMockBrokerName(scope);
        std::map<MessageQueue, std::pair<int64_t, std::string>> mqEndPoints;
        for (const auto& ni : infos) {
            const TopicQueueMappingInfo& info = ni.second;
            if (info.epoch >= maxTotalNumOfEpoch && info.totalQueues > maxTotalNums) {
                maxTotalNums = info.totalQueues;
            }
            // Only the global id (key) shape determines the endpoint set; physical ids are broker-internal.
            for (const auto& gid : info.currIdMap) {
                MessageQueue mq(topic, mockName, gid.first);
                auto old = mqEndPoints.find(mq);
                if (old == mqEndPoints.end() || old->second.first <= info.epoch) {
                    mqEndPoints[mq] = {info.epoch, info.bname};
                }
            }
        }

        // Fill in the static logical queues: every id below total exists; ones without
        // a host queue point at the NOT_EXIST sentinel.
        for (int32_t i = 0; i < maxTotalNums; ++i) {
            MessageQueue mq(topic, mockName, i);
            auto holder = mqEndPoints.find(mq);
            endpoints[mq] = holder != mqEndPoints.end() ? holder->second.second
                                                        : std::string(kLogicalQueueMockNotExist);
        }
    }
    if (endpoints.empty()) {
        return false;
    }
    out = std::move(endpoints);
    return true;
}

}  // namespace static_topic
}  // namespace rocketmq
