// Application callback for a queue assignment change.
//
// One call per topic whose set of assigned queues moved, carrying that topic's FULL queue set from
// the route and this instance's share of it, so an application that rebuilds per-queue state has the
// two sides of the split in one call.
#pragma once

#include <string>
#include <vector>

#include "rocketmq/common/message.h"

namespace rocketmq {

class MessageQueueListener {
public:
    virtual ~MessageQueueListener() = default;
    virtual void messageQueueChanged(const std::string& topic,
                                     const std::vector<MessageQueue>& mqAll,
                                     const std::vector<MessageQueue>& mqDivided) = 0;
};

}  // namespace rocketmq
