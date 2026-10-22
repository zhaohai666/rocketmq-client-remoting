// Live-broker proof for BATCH_ACK_MESSAGE(200151): clearing a whole POP batch with one request.
// Usage: rmq_live_pop_batch_ack 127.0.0.1:9876 [legs]     # legs = l1,l2,l3 (default: all)
//
// Why this leg exists on its own: the POP consume path acknowledges message by message through
// ACK_MESSAGE(200051) and never builds a batch, so no runtime path reaches the batch wire path. The
// merge key, the bit index, the base64 bit set and the "everything rides the JSON body" request shape
// are only reachable by calling batchAckMessage directly -- and a primitive nobody has pointed at a
// broker is a primitive that may not work, silently: the broker answers SUCCESS to a batch body whose
// group key, start offset or bits are wrong. What it does NOT answer is the message going away, so
// every criterion here is a bounded observation window plus broker-side visible state.
//
// The batch body carries no per-message offsets, only a start offset plus a bit set, so the request
// bytes are pinned too: this leg installs an RPC hook on the client instance that records every 200151
// frame exactly as it leaves the process, and the body assertions run on those captured bytes, decoded
// with an independent base64 reader rather than with the encoder that produced them.
//
//   L1 ONE REQUEST, WHOLE BATCH. Pop 8 messages off a single queue and clear them with one 200151. The
//      body must acknowledge exactly the 8 popped offsets once each, hold exactly one entry per merge
//      key, echo every checkpoint field, name the first checkpoint's broker, carry no custom header,
//      survive a codec round trip, and be answered SUCCESS. Then 2.5x the invisibility window of
//      silence: a wrong offset or bit leaves a checkpoint alive and the revive service puts that
//      message back inside the window.
//   L2 PAIRED CONTROL. The same batch with exactly one checkpoint left out. Only that message may come
//      back -- this is the assertion that fails if a bit set is treated as "the whole batch". The
//      follow-up pins the retry direction: the revived checkpoint carries retry marker "1", and the
//      body that clears it names the BUSINESS topic with marker "1" on every entry, because the broker
//      re-derives the physical retry queue from marker + group. The retry topic's route is read from
//      the name server AFTER the ack, since route registration can lag a broker heartbeat. Which RPC
//      answered the message is deliberately not asserted: a POP of the business topic also consults
//      the retry queue and the reply topic is rewritten to the requested one, so it is not evidence;
//      the checkpoint marker and the retry topic's route are.
//   L3 NO OVER-MERGING. Two pop rounds of one queue cleared by one request. The merge key is
//      retry@queueId@ckQueueOffset@popTime, so the rounds must stay in separate entries; collapsing
//      them would acknowledge offsets nobody popped.
//
// Window discipline: the probe that waits for a revival stops at the first sighting plus a short tail,
// and the ack goes out immediately. A sighted checkpoint left to keep expiring is revived again before
// the ack lands, and no ack can take a revival back, so a longer probe would fail on timing instead of
// on the wire path. Every window and every RPC carries an explicit second-scale budget: nothing here
// blocks without a deadline, and nothing relies on a default timeout.
//
// The broker needs no switch for this command: 200151 is registered unconditionally, so the standard
// test cluster already serves it. The POP prerequisites this leg does rely on are timerWheelEnable,
// defaultMessageRequestMode=PULL and popResponseReturnActualRetryTopic=false -- ordinary test-cluster
// settings; this tool never changes broker configuration.
//
// Each leg creates its own single-queue topic up front and waits until the queue really holds what was
// sent, because a partially dispatched batch would make "one request cleared the batch" unprovable.
// Message sends have a bounded retry budget and every wait is gated on recorded state, not a fixed sleep.
//
// Output is [PASS] / [FAIL] per check plus a final "PASS=<n> FAIL=<m>" line; exits non-zero on any FAIL.
#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/extra_info.h"
#include "rocketmq/remoting/protocol/json.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/protocol/route.h"
#include "rocketmq/remoting/protocol/serialize.h"
#include "rocketmq/remoting/rpchook.h"

using namespace rocketmq;

namespace {

// ------------------------------------------------------------------ reporting

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

// ------------------------------------------------------------------ timing budget
//
// Ten seconds of invisibility sits far above the broker's revive scan interval, so a checkpoint no ack
// covered is always observed coming back, and it keeps the whole run inside a few minutes.
constexpr int64_t kInvisibleMs = 10000;
constexpr int64_t kObserveMs = kInvisibleMs * 5 / 2;  // 25 s of expected silence
constexpr int64_t kReviveTailMs = 3000;               // extra watching after the first sighting
constexpr int64_t kReviveProbeMs = 40000;             // upper bound on the probe, not a wait target
constexpr int64_t kDepthWaitMs = 30000;
constexpr int64_t kRouteWaitMs = 30000;
constexpr int64_t kRoundIntervalMs = 1000;
constexpr int32_t kRpcTimeoutMs = 5000;
constexpr int64_t kPopOverheadMs = 5000;
constexpr int kSendAttempts = 6;

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

void sleepMs(int64_t ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

std::string clip(const std::string& s, size_t max = 240) {
    return s.size() <= max ? s : (s.substr(0, max) + "...");
}

std::string join(const std::vector<std::string>& v, const std::string& sep) {
    std::string out;
    for (size_t i = 0; i < v.size(); ++i) {
        if (i) out += sep;
        out += v[i];
    }
    return out;
}

std::vector<std::string> sortedUnique(const std::vector<std::string>& in) {
    std::vector<std::string> out(in.begin(), in.end());
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
}

std::string joinInts(const std::vector<int64_t>& v) {
    std::vector<std::string> s;
    for (const int64_t x : v) s.push_back(std::to_string(x));
    return "[" + join(s, ",") + "]";
}

std::vector<int64_t> sortedUniqueInts(const std::vector<int64_t>& in) {
    std::vector<int64_t> out(in);
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
}

bool sameOffsets(const std::vector<int64_t>& a, const std::vector<int64_t>& b) {
    std::vector<int64_t> x(a), y(b);
    std::sort(x.begin(), x.end());
    std::sort(y.begin(), y.end());
    return x == y;
}

std::string popCkOf(const MessageExt& m) {
    auto it = m.properties.find(MessageConst::PROPERTY_POP_CK);
    return it == m.properties.end() ? std::string() : it->second;
}

// ------------------------------------------------------------------ checkpoints

struct Ck {
    std::string raw;
    std::string body;
    std::string brokerName;
    std::string retry;
    int64_t ckQueueOffset = 0;
    int64_t queueOffset = 0;
    int64_t popTime = 0;
    int64_t invisibleTime = 0;
    int32_t queueId = 0;
    int32_t reviveQid = 0;
    bool parsable = false;
    std::string problem;
};

// The 8-segment POP_CK a popped message carries, read segment by segment. Nothing is inferred: a
// checkpoint that cannot be parsed is a failure and not a skip, because the batch body is built
// entirely out of these values.
Ck parseCk(const std::string& raw, const std::string& body) {
    Ck c;
    c.raw = raw;
    c.body = body;
    if (raw.empty()) {
        c.problem = body + ": no POP_CK on the message";
        return c;
    }
    const std::vector<std::string> seg = extra_info::split(raw);
    if (seg.size() != 8) {
        c.problem = body + ": checkpoint has " + std::to_string(seg.size())
            + " segments, want 8: '" + raw + "'";
        return c;
    }
    try {
        c.ckQueueOffset = extra_info::getCkQueueOffset(seg);
        c.popTime = extra_info::getPopTime(seg);
        c.invisibleTime = extra_info::getInvisibleTime(seg);
        c.reviveQid = extra_info::getReviveQid(seg);
        c.retry = extra_info::getRetry(seg);
        c.brokerName = extra_info::getBrokerName(seg);
        c.queueId = extra_info::getQueueId(seg);
        c.queueOffset = extra_info::getQueueOffset(seg);
    } catch (const std::exception& e) {
        c.problem = body + ": " + e.what();
        return c;
    }
    c.parsable = true;
    return c;
}

std::vector<Ck> parseAll(const std::vector<MessageExt>& msgs, std::vector<std::string>& problems) {
    std::vector<Ck> out;
    for (const MessageExt& m : msgs) {
        Ck c = parseCk(popCkOf(m), m.getBody());
        if (!c.parsable) {
            problems.push_back(c.problem);
        } else if (c.queueOffset != m.queueOffset) {
            // Segment 7 is what the body turns into a bit index; if it were not this message's own
            // consume-queue offset, the body would be addressing somebody else's message.
            problems.push_back(c.body + ": checkpoint segment 7 = " + std::to_string(c.queueOffset)
                + " but the message sits at " + std::to_string(m.queueOffset));
        }
        out.push_back(c);
    }
    return out;
}

// Recomputed here, independently of the builder, so "the builder grouped it" is not the same statement
// as "the key says it should group".
std::string mergeKeyOf(const Ck& c) {
    return c.retry + "@" + std::to_string(c.queueId) + "@" + std::to_string(c.ckQueueOffset) + "@"
        + std::to_string(c.popTime);
}

std::vector<std::string> mergeKeysOf(const std::vector<Ck>& cks) {
    std::vector<std::string> out;
    for (const Ck& c : cks) out.push_back(mergeKeyOf(c));
    return sortedUnique(out);
}

std::vector<int64_t> offsetsOf(const std::vector<Ck>& cks) {
    std::vector<int64_t> out;
    for (const Ck& c : cks) out.push_back(c.queueOffset);
    std::sort(out.begin(), out.end());
    return out;
}

std::vector<std::string> bodiesOf(const std::vector<Ck>& cks) {
    std::vector<std::string> out;
    for (const Ck& c : cks) out.push_back(c.body);
    return out;
}

std::vector<std::string> markersOf(const std::vector<Ck>& cks) {
    std::vector<std::string> out;
    for (const Ck& c : cks) out.push_back(c.retry);
    return sortedUnique(out);
}

std::string describe(const std::vector<Ck>& cks) {
    std::string out;
    for (const Ck& c : cks) {
        out += c.body + "{ck=" + std::to_string(c.ckQueueOffset) + " off=" + std::to_string(c.queueOffset)
            + " pt=" + std::to_string(c.popTime) + " r=" + c.retry + " q=" + std::to_string(c.queueId)
            + " rq=" + std::to_string(c.reviveQid) + " broker=" + c.brokerName + "} ";
    }
    return clip(out);
}

std::vector<std::string> rawsOf(const std::vector<Ck>& cks) {
    std::vector<std::string> out;
    for (const Ck& c : cks) out.push_back(c.raw);
    return out;
}

// ------------------------------------------------------------------ the captured request
//
// An independent base64 reader: the assertion must look at the decoded wire bytes, not re-run the
// encoder the implementation used, or a symmetric mistake in encoder and checker would hide.
const char* kB64Table = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string b64Decode(const std::string& text) {
    std::string out;
    int val = 0;
    int bits = 0;
    for (const char ch : text) {
        if (ch == '=') break;
        const char* p = std::strchr(kB64Table, ch);
        if (p == nullptr) continue;
        val = (val << 6) | static_cast<int>(p - kB64Table);
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out.push_back(static_cast<char>((val >> bits) & 0xFF));
        }
    }
    return out;
}

// One entry of the body, in exactly the terms the merge rule is stated in.
struct BodyEntry {
    std::string consumerGroup;
    std::string topic;
    std::string retry;
    std::string bitSetB64;
    std::string bitSetBytes;  // decoded, little-endian: bit i lives in byte i/8 at position i%8
    int64_t startOffset = 0;
    int64_t queueId = 0;
    int64_t reviveQueueId = 0;
    int64_t popTime = 0;
    int64_t invisibleTime = 0;

    // The same formula, read back out of the entry the implementation produced.
    std::string key() const {
        return retry + "@" + std::to_string(queueId) + "@" + std::to_string(startOffset) + "@"
            + std::to_string(popTime);
    }

    // The consume-queue offsets this entry clears: its start offset plus every set bit.
    std::vector<int64_t> ackedOffsets() const {
        std::vector<int64_t> out;
        for (size_t i = 0; i < bitSetBytes.size(); ++i) {
            const unsigned char byte = static_cast<unsigned char>(bitSetBytes[i]);
            for (int k = 0; k < 8; ++k) {
                if (byte & (1 << k)) out.push_back(startOffset + static_cast<int64_t>(i * 8 + k));
            }
        }
        std::sort(out.begin(), out.end());
        return out;
    }

    int highestBit() const {
        int highest = -1;
        for (size_t i = 0; i < bitSetBytes.size(); ++i) {
            const unsigned char byte = static_cast<unsigned char>(bitSetBytes[i]);
            for (int k = 0; k < 8; ++k) {
                if (byte & (1 << k)) highest = static_cast<int>(i * 8 + k);
            }
        }
        return highest;
    }

    // A padded encoder (word-padded, or trailing zero bytes kept) still decodes back to the very same
    // offsets, so only the byte count can tell the two apart.
    bool bitSetIsByteMinimal() const {
        const int highest = highestBit();
        if (highest < 0) return bitSetBytes.empty();
        return bitSetBytes.size() == static_cast<size_t>(highest / 8 + 1)
            && static_cast<unsigned char>(bitSetBytes[bitSetBytes.size() - 1]) != 0;
    }

    std::string dump() const {
        return "{c=" + consumerGroup + " t=" + topic + " r=" + retry + " so="
            + std::to_string(startOffset) + " q=" + std::to_string(queueId) + " rq="
            + std::to_string(reviveQueueId) + " pt=" + std::to_string(popTime) + " it="
            + std::to_string(invisibleTime) + " b=" + bitSetB64 + "}";
    }
};

struct Captured {
    std::string remoteAddr;
    std::string bodyJson;
    size_t extFieldCount = 0;
    std::string brokerName;
    std::vector<BodyEntry> entries;
    std::string parseProblem;

    std::vector<int64_t> allAckedOffsets() const {
        std::vector<int64_t> out;
        for (const BodyEntry& e : entries) {
            for (const int64_t o : e.ackedOffsets()) out.push_back(o);
        }
        std::sort(out.begin(), out.end());
        return out;
    }

    std::vector<std::string> entryKeys() const {
        std::vector<std::string> out;
        for (const BodyEntry& e : entries) out.push_back(e.key());
        return out;
    }

    std::string entriesDump() const {
        std::vector<std::string> out;
        for (const BodyEntry& e : entries) out.push_back(e.dump());
        return clip(join(out, " "));
    }
};

// Reads the compact body shape {"brokerName":..,"acks":[{c,t,r,so,q,rq,pt,it,b},..]}.
bool parseBody(Captured& cap) {
    JsonValue root;
    std::string err;
    if (!jsonParse(cap.bodyJson, root, &err)) {
        cap.parseProblem = "body does not parse: " + err;
        return false;
    }
    const JsonValue* broker = root.find("brokerName");
    if (broker == nullptr || !broker->isString()) {
        cap.parseProblem = "body has no string \"brokerName\"";
        return false;
    }
    cap.brokerName = broker->stringValue();
    const JsonValue* acks = root.find("acks");
    if (acks == nullptr || !acks->isArray()) {
        cap.parseProblem = "body has no \"acks\" array";
        return false;
    }
    const char* compact[] = {"c", "t", "r", "so", "q", "rq", "pt", "it", "b"};
    const char* longNames[] = {"consumerGroup", "topic", "retry", "startOffset", "queueId",
                               "reviveQueueId", "popTime", "invisibleTime", "bitSet"};
    for (size_t i = 0; i < acks->size(); ++i) {
        const JsonValue& o = acks->at(i);
        for (const char* k : compact) {
            if (o.find(k) == nullptr) {
                cap.parseProblem = "entry " + std::to_string(i) + " is missing compact key " + k;
                return false;
            }
        }
        // None of the long-form aliases may leak into the body: the broker reads the compact keys.
        for (const char* k : longNames) {
            if (o.find(k) != nullptr) {
                cap.parseProblem = "entry " + std::to_string(i) + " leaks long-form key " + k;
                return false;
            }
        }
        BodyEntry e;
        e.consumerGroup = o.get("c").stringValue();
        e.topic = o.get("t").stringValue();
        e.retry = o.get("r").stringValue();
        e.startOffset = o.get("so").intValue();
        e.queueId = o.get("q").intValue();
        e.reviveQueueId = o.get("rq").intValue();
        e.popTime = o.get("pt").intValue();
        e.invisibleTime = o.get("it").intValue();
        e.bitSetB64 = o.get("b").stringValue();
        e.bitSetBytes = b64Decode(e.bitSetB64);
        cap.entries.push_back(e);
    }
    return true;
}

// Records every BATCH_ACK_MESSAGE frame exactly as the transport is about to put it on the wire. Only
// 200151 is kept, so nothing else in the run can fill the buffer.
class BatchAckCaptor : public RPCHook {
public:
    void doBeforeRequest(const std::string& remoteAddr, RemotingCommand& request) override {
        if (request.code != RequestCode::BATCH_ACK_MESSAGE) return;
        std::lock_guard<std::mutex> lk(mutex_);
        Captured cap;
        cap.remoteAddr = remoteAddr;
        cap.extFieldCount = request.extFields.size();
        cap.bodyJson = request.body;
        frames_.push_back(cap);
    }

    void clear() {
        std::lock_guard<std::mutex> lk(mutex_);
        frames_.clear();
    }

    size_t count() {
        std::lock_guard<std::mutex> lk(mutex_);
        return frames_.size();
    }

    bool last(Captured& out) {
        std::lock_guard<std::mutex> lk(mutex_);
        if (frames_.empty()) return false;
        out = frames_.back();
        return true;
    }

private:
    std::mutex mutex_;
    std::vector<Captured> frames_;
};

// ------------------------------------------------------------------ wire driver

struct Leg {
    std::string topic;
    std::string group;
    std::string retryTopic;
    std::string brokerName;
    std::string addr;
    int32_t reviveQueueId = 0;  // the queue the revive service re-puts an unacked checkpoint into
};

PopResult popOne(MQClientInstance& client, const Leg& fx, const std::string& group,
                 const std::string& topic, int32_t queueId, int32_t maxNums, int64_t pollMs) {
    // initMode 0: a brand-new group resolves its initial offset to the earliest message, which the
    // broker only consults when it has to pick one. The timeout is named here rather than defaulted.
    return client.popMessage(group, topic, queueId, maxNums, kInvisibleMs, pollMs, 0, "", "", false,
                             static_cast<int32_t>(pollMs + kPopOverheadMs), fx.brokerName, fx.addr);
}

int32_t batchAck(MQClientInstance& client, const std::string& group, const std::string& topic,
                 const std::vector<std::string>& raws, const std::string& addr, std::string& err) {
    err.clear();
    try {
        return client.batchAckMessage(group, topic, raws, kRpcTimeoutMs, addr);
    } catch (const std::exception& e) {
        err = e.what();
        return -1;
    }
}

// ------------------------------------------------------------------ observation

struct Sighting {
    std::string body;
    std::string ck;
    std::string answeredFrom;  // diagnostics only, never evidence
};

struct Eyes {
    std::vector<Sighting> sightings;

    bool cameBack() const { return !sightings.empty(); }

    std::vector<std::string> bodies() const {
        std::vector<std::string> out;
        for (const Sighting& s : sightings) out.push_back(s.body);
        return sortedUnique(out);
    }

    size_t countOf(const std::string& body) const {
        size_t n = 0;
        for (const Sighting& s : sightings) {
            if (s.body == body) ++n;
        }
        return n;
    }

    // Every checkpoint the window collected for one body: a message sighted twice has one live
    // checkpoint per sighting, and an ack that clears only one of them leaves the others to revive it
    // again.
    std::vector<std::string> checkpointsOf(const std::string& body) const {
        std::vector<std::string> out;
        for (const Sighting& s : sightings) {
            if (s.body == body && !s.ck.empty()) out.push_back(s.ck);
        }
        return out;
    }

    std::vector<std::string> answeredFrom() const {
        std::vector<std::string> out;
        for (const Sighting& s : sightings) out.push_back(s.answeredFrom);
        return sortedUnique(out);
    }

    std::string summary() const {
        if (sightings.empty()) return "nothing revived";
        return "revived " + join(bodies(), " ") + " (" + std::to_string(sightings.size())
            + " sightings, answers named " + join(answeredFrom(), ",") + ")";
    }
};

// One round pops the business queue and the group's POP retry queue the checkpoints name, plus a random
// retry queue: an un-acked checkpoint is revived into the retry topic, and watching one queue alone
// would make a silence ambiguous.
void popRound(MQClientInstance& client, const Leg& fx,
              const std::vector<std::pair<std::string, int32_t>>& targets, Eyes& eyes) {
    for (const auto& target : targets) {
        PopResult res;
        try {
            res = popOne(client, fx, fx.group, target.first, target.second, 32, 0);
        } catch (const std::exception&) {
            continue;  // a retry topic the group has never revived into simply does not exist yet
        }
        if (!res.isFound()) continue;
        for (const MessageExt& m : res.msgFoundList) {
            eyes.sightings.push_back(Sighting{m.getBody(), popCkOf(m), target.first});
        }
    }
}

std::vector<std::pair<std::string, int32_t>> watchTargets(const Leg& fx) {
    std::vector<std::pair<std::string, int32_t>> targets;
    targets.push_back(std::make_pair(fx.topic, static_cast<int32_t>(0)));
    targets.push_back(std::make_pair(fx.retryTopic, fx.reviveQueueId));
    targets.push_back(std::make_pair(fx.retryTopic, static_cast<int32_t>(-1)));
    return targets;
}

// Fixed window, used where the expected answer is silence.
void watch(MQClientInstance& client, const Leg& fx, int64_t windowMs, Eyes& eyes) {
    const std::vector<std::pair<std::string, int32_t>> targets = watchTargets(fx);
    const int64_t until = nowMs() + windowMs;
    while (nowMs() < until) {
        popRound(client, fx, targets, eyes);
        sleepMs(kRoundIntervalMs);
    }
}

// Bounded probe: stops at the first sighting plus a short tail, so the whole revived batch is recorded
// while the collected checkpoints are still inside their own invisibility.
bool probe(MQClientInstance& client, const Leg& fx, Eyes& eyes) {
    const std::vector<std::pair<std::string, int32_t>> targets = watchTargets(fx);
    const int64_t until = nowMs() + kReviveProbeMs;
    int64_t tailUntil = -1;
    while (nowMs() < until) {
        popRound(client, fx, targets, eyes);
        if (eyes.cameBack()) {
            if (tailUntil < 0) {
                tailUntil = nowMs() + kReviveTailMs;
            } else if (nowMs() >= tailUntil) {
                return true;
            }
        }
        sleepMs(kRoundIntervalMs);
    }
    return eyes.cameBack();
}

// ------------------------------------------------------------------ setup

// Messages reach the consume queue asynchronously, so wait until the queue really holds what was sent.
// State-gated, with an explicit budget.
int64_t awaitDepth(MQClientInstance& client, const Leg& fx, int64_t want, std::string& err) {
    const int64_t until = nowMs() + kDepthWaitMs;
    int64_t depth = -1;
    for (;;) {
        err.clear();
        try {
            const MessageQueue mq(fx.topic, fx.brokerName, 0);
            depth = client.getMaxOffset(mq, kRpcTimeoutMs, fx.addr)
                - client.getMinOffset(mq, kRpcTimeoutMs, fx.addr);
        } catch (const std::exception& e) {
            depth = -1;
            err = e.what();
        }
        if (depth >= want) return depth;
        if (nowMs() > until) return depth;
        sleepMs(kRoundIntervalMs);
    }
}

// Route presence on the name server is broker state: the POP retry topic only exists once the revive
// service has moved a checkpoint into it.
bool routeVisible(MQClientInstance& client, const std::string& topic, std::string& err) {
    const int64_t until = nowMs() + kRouteWaitMs;
    err.clear();
    for (;;) {
        try {
            if (client.updateTopicRouteInfoFromNameServer(topic, kRpcTimeoutMs)) {
                const std::shared_ptr<TopicRouteData> route = client.getTopicRouteData(topic);
                if (route != nullptr && !route->brokerDatas.empty()) return true;
            }
        } catch (const std::exception& e) {
            err = e.what();
        }
        if (nowMs() > until) return false;
        sleepMs(2000);
    }
}

// One leg: a single-queue topic (a multi-queue topic would spread the batch, and the batch semantics
// under test are per-queue), the messages, and the queue depth the batch is expected to see.
bool setupLeg(DefaultMQProducer& producer, MQClientInstance& client, const std::string& stamp,
              const std::string& topicName, const std::string& bodyPrefix, int count, Leg& fx) {
    fx.topic = topicName + "_" + stamp;
    fx.group = "GID_" + topicName + "_" + stamp;
    fx.retryTopic = extra_info::buildPopRetryTopicV1(fx.topic, fx.group);
    std::string err;

    try {
        producer.createTopic(MixAll::DEFAULT_TOPIC, fx.topic, 1);
    } catch (const std::exception& e) {
        check("setup: single-queue topic created", false, fx.topic + ": " + e.what());
        return false;
    }
    if (!routeVisible(client, fx.topic, err)) {
        check("setup: topic route published", false, fx.topic + " " + err);
        return false;
    }
    const std::shared_ptr<TopicRouteData> route = client.getTopicRouteData(fx.topic);
    fx.brokerName = route->brokerDatas.front().brokerName;
    fx.addr = MQClientInstance::findBrokerAddrInRoute(*route, fx.brokerName);

    bool sent = true;
    for (int i = 0; i < count && sent; ++i) {
        const std::string body = bodyPrefix + "-" + std::to_string(i);
        bool one = false;
        for (int attempt = 0; attempt < kSendAttempts && !one; ++attempt) {
            try {
                const Message m(fx.topic, "TagBatchAck", "pk-" + body, body);
                producer.send(m, kRpcTimeoutMs);
                one = true;
            } catch (const std::exception& e) {
                err = e.what();
                sleepMs(2000);  // the route can still be propagating right after createTopic
            }
        }
        if (!one) sent = false;
    }
    check("setup: " + std::to_string(count) + " messages sent to the single-queue topic", sent,
          fx.topic + " @ " + fx.addr + (err.empty() ? "" : " last error: " + err));
    if (!sent) return false;

    const int64_t depth = awaitDepth(client, fx, count, err);
    check("setup: all " + std::to_string(count) + " messages are on the queue before popping",
          depth >= count, "depth=" + std::to_string(depth) + (err.empty() ? "" : " " + err));
    return depth >= count;
}

// Reads the one frame the leg just sent and parses its body. Returns false when there is nothing to
// assert on, so the caller reports the broker's answer instead of dereferencing nothing.
bool capturedOne(BatchAckCaptor& captor, const std::string& label, const std::string& expectAddr,
                 Captured& cap) {
    const size_t n = captor.count();
    check(label + " exactly one BATCH_ACK_MESSAGE(200151) request left this process", n == 1,
          "frames=" + std::to_string(n));
    if (n != 1 || !captor.last(cap)) {
        if (n == 1) check(label + " request body captured", false, "no frame recorded");
        return false;
    }
    check(label + " the captured frame is the request sent to this broker",
          cap.remoteAddr == expectAddr, "captured=" + cap.remoteAddr + " broker=" + expectAddr);
    check(label + " the request carries no custom header (everything rides the JSON body)",
          cap.extFieldCount == 0, "extFields=" + std::to_string(cap.extFieldCount));
    const bool parsed = parseBody(cap);
    check(label + " body parses with the compact per-entry keys and no long-form alias", parsed,
          parsed ? "entries=" + std::to_string(cap.entries.size()) : cap.parseProblem);
    return parsed;
}

// Every field of an entry has to be the value its checkpoint carried: the broker looks the checkpoint up
// by (group, real topic, queueId, popTime, invisibleTime, revive queue) and then walks the bits from
// startOffset, so one invented field makes the whole entry a no-op that still answers SUCCESS.
void assertEntryFields(const Captured& cap, const std::vector<Ck>& cks, const std::string& group,
                       const std::string& topic, const std::string& label) {
    std::map<std::string, const Ck*> byKey;
    for (const Ck& c : cks) byKey[mergeKeyOf(c)] = &c;
    bool ok = !cap.entries.empty();
    std::string problem;
    for (const BodyEntry& e : cap.entries) {
        if (e.consumerGroup != group || e.topic != topic) {
            ok = false;
            problem = "entry c=" + e.consumerGroup + " t=" + e.topic + " want c=" + group + " t=" + topic;
            break;
        }
        const std::map<std::string, const Ck*>::const_iterator found = byKey.find(e.key());
        if (found == byKey.end()) {
            ok = false;
            problem = "entry key " + e.key() + " matches no popped checkpoint " + e.dump();
            break;
        }
        const Ck& src = *found->second;
        if (e.startOffset != src.ckQueueOffset || e.popTime != src.popTime
            || e.invisibleTime != src.invisibleTime || e.queueId != src.queueId
            || e.reviveQueueId != src.reviveQid || e.retry != src.retry) {
            ok = false;
            problem = "entry " + e.key() + " (for " + src.body + ") diverges from its checkpoint: so="
                + std::to_string(e.startOffset) + "/" + std::to_string(src.ckQueueOffset) + " pt="
                + std::to_string(e.popTime) + "/" + std::to_string(src.popTime) + " it="
                + std::to_string(e.invisibleTime) + "/" + std::to_string(src.invisibleTime) + " q="
                + std::to_string(e.queueId) + "/" + std::to_string(src.queueId) + " rq="
                + std::to_string(e.reviveQueueId) + "/" + std::to_string(src.reviveQid) + " r=" + e.retry
                + "/" + src.retry;
            break;
        }
    }
    check(label + " every entry echoes its checkpoint fields (startOffset=ckQueueOffset, queueId, "
                  "reviveQid, popTime, invisibleTime, marker) and names the group and the business topic",
          ok, problem.empty() ? cap.entriesDump() : problem);
    check(label + " body brokerName equals the FIRST checkpoint's broker name",
          !cap.entries.empty() && cap.brokerName == cks.front().brokerName,
          "bodyBroker=" + cap.brokerName + " firstCkBroker=" + cks.front().brokerName);
}

// The wire codec round trip: the bytes the broker is handed must decode and re-encode to themselves,
// with every field and every bit read back the same way.
void assertBodyRoundTrip(const Captured& cap, const std::string& label) {
    JsonValue decoded;
    std::string err;
    const bool decodes = jsonParse(cap.bodyJson, decoded, &err);
    const std::string reencoded = decodes ? RemotingSerializable::encode(decoded) : std::string();
    Captured again;
    again.bodyJson = reencoded;
    const bool reparsed = !reencoded.empty() && parseBody(again);
    check(label + " body round-trips through the wire codec (decode, re-encode, re-parse all agree)",
          decodes && reparsed && reencoded == cap.bodyJson && again.brokerName == cap.brokerName
              && again.entries.size() == cap.entries.size()
              && again.allAckedOffsets() == cap.allAckedOffsets(),
          decodes ? ("bytes=" + std::to_string(cap.bodyJson.size()) + "/"
                     + std::to_string(reencoded.size()) + " "
                     + (reencoded == cap.bodyJson ? "byte-identical" : clip(reencoded)))
                  : err);
}

// ------------------------------------------------------------------ L1: one request, whole batch

void legAll(DefaultMQProducer& producer, MQClientInstance& client, BatchAckCaptor& captor,
            const std::string& stamp) {
    std::printf("=== L1 one request clears a whole batch ===\n");
    const std::string label = "L1";
    Leg fx;
    if (!setupLeg(producer, client, stamp, "CppBaAll", "ba-all", 8, fx)) return;

    PopResult res;
    try {
        res = popOne(client, fx, fx.group, fx.topic, 0, 32, 0);
    } catch (const std::exception& e) {
        check(label + " POP_MESSAGE answered the whole batch", false, e.what());
        return;
    }
    check(label + " POP_MESSAGE answered the whole batch",
          res.isFound() && res.msgFoundList.size() == 8,
          std::string("status=") + popStatusName(res.status) + " got="
              + std::to_string(res.msgFoundList.size()));
    if (res.msgFoundList.size() != 8) return;

    std::vector<std::string> problems;
    const std::vector<Ck> cks = parseAll(res.msgFoundList, problems);
    check(label + " every message carries a parsable 8-segment checkpoint", problems.empty(),
          problems.empty() ? std::to_string(cks.size()) + " checkpoints" : join(problems, "; "));
    if (!problems.empty()) {
        std::printf("        %s\n", describe(cks).c_str());
        return;
    }

    // The body can only address a message through segment 7 and only route through segments 4-6, so pin
    // them per message before the builder is trusted with them.
    bool uniform = true;
    std::string why;
    std::vector<int64_t> ckOffsets;
    for (const Ck& c : cks) ckOffsets.push_back(c.ckQueueOffset);
    for (const Ck& c : cks) {
        if (c.brokerName != fx.brokerName || c.queueId != 0 || c.invisibleTime != kInvisibleMs
            || c.retry != extra_info::kNormalTopic || c.popTime <= 0 || c.ckQueueOffset > c.queueOffset) {
            uniform = false;
            why = describe(std::vector<Ck>(1, c));
            break;
        }
    }
    check(label + " checkpoint fields the batch body depends on are uniform (marker 0, this broker, "
                  "queueId 0, invisibility, one start offset, ck <= message offset)",
          uniform && sortedUniqueInts(ckOffsets).size() == 1,
          uniform ? ("ckQueueOffset=" + std::to_string(cks.front().ckQueueOffset) + " popTime="
                     + std::to_string(cks.front().popTime) + " reviveQid="
                     + std::to_string(cks.front().reviveQid))
                  : why);
    if (!uniform || sortedUniqueInts(ckOffsets).size() != 1) {
        std::printf("        %s\n", describe(cks).c_str());
        return;
    }
    fx.reviveQueueId = cks.front().reviveQid;

    const std::vector<int64_t> wantOffsets = offsetsOf(cks);
    const std::vector<std::string> keys = mergeKeysOf(cks);

    std::string err;
    captor.clear();
    const int32_t code = batchAck(client, fx.group, fx.topic, rawsOf(cks), fx.addr, err);

    Captured cap;
    if (!capturedOne(captor, label, fx.addr, cap)) {
        check(label + " one BATCH_ACK_MESSAGE answered SUCCESS", false,
              "code=" + std::to_string(code) + (err.empty() ? "" : " " + err));
        return;
    }

    const std::vector<int64_t> acked = cap.allAckedOffsets();
    check(label + " body acknowledges exactly the popped offsets, once each",
          acked.size() == wantOffsets.size() && sameOffsets(acked, wantOffsets),
          "acked=" + joinInts(acked) + " want=" + joinInts(wantOffsets));
    // Compared as sorted sets: the body may emit its entries in any order, but a duplicated key still
    // shows up as fewer distinct keys than entries.
    const std::vector<std::string> bodyKeys = cap.entryKeys();
    check(label + " body has exactly one entry per merge key",
          sortedUnique(bodyKeys).size() == bodyKeys.size() && sortedUnique(bodyKeys) == keys,
          "entries=" + std::to_string(bodyKeys.size()) + " bodyKeys=" + join(bodyKeys, ",")
              + " ckKeys=" + join(keys, ","));

    assertEntryFields(cap, cks, fx.group, fx.topic, label);

    bool bitsOk = cap.entries.size() == 1;
    std::string bitProblem = "entries=" + std::to_string(cap.entries.size());
    if (bitsOk) {
        const BodyEntry& e = cap.entries.front();
        if (e.ackedOffsets() != wantOffsets) {
            bitsOk = false;
            bitProblem = "decoded=" + joinInts(e.ackedOffsets()) + " b=" + e.bitSetB64;
        } else if (!e.bitSetIsByteMinimal()) {
            bitsOk = false;
            bitProblem = "bit set is not byte-minimal: b=" + e.bitSetB64 + " bytes="
                + std::to_string(e.bitSetBytes.size());
        }
    }
    check(label + " bit set decodes back to exactly the batch offsets as byte-minimal little-endian "
                  "base64",
          bitsOk, bitProblem.empty() ? ("b=" + cap.entries.front().bitSetB64 + " offsets="
                                       + joinInts(cap.entries.front().ackedOffsets()))
                                     : bitProblem);
    assertBodyRoundTrip(cap, label);

    check(label + " one BATCH_ACK_MESSAGE answered SUCCESS", code == ResponseCode::SUCCESS,
          "code=" + std::to_string(code) + (err.empty() ? "" : " " + err) + " over "
              + std::to_string(cks.size()) + " checkpoints / " + std::to_string(cap.entries.size())
              + " entries");

    Eyes eyes;
    watch(client, fx, kObserveMs, eyes);
    check(label + " nothing revived inside 2.5x the invisibility window (the batch really landed)",
          !eyes.cameBack(), eyes.summary());
}

// ------------------------------------------------------------------ L2: paired control

void legPartial(DefaultMQProducer& producer, MQClientInstance& client, BatchAckCaptor& captor,
                const std::string& stamp) {
    std::printf("=== L2 paired control: one checkpoint left out of the batch ===\n");
    const std::string label = "L2";
    Leg fx;
    if (!setupLeg(producer, client, stamp, "CppBaPart", "ba-part", 6, fx)) return;

    PopResult res;
    try {
        res = popOne(client, fx, fx.group, fx.topic, 0, 32, 0);
    } catch (const std::exception& e) {
        check(label + " POP_MESSAGE answered the batch", false, e.what());
        return;
    }
    check(label + " POP_MESSAGE answered the batch",
          res.isFound() && res.msgFoundList.size() == 6,
          std::string("status=") + popStatusName(res.status) + " got="
              + std::to_string(res.msgFoundList.size()));
    if (res.msgFoundList.size() != 6) return;

    std::vector<std::string> problems;
    std::vector<Ck> cks = parseAll(res.msgFoundList, problems);
    check(label + " every message carries a parsable 8-segment checkpoint", problems.empty(),
          problems.empty() ? std::to_string(cks.size()) + " checkpoints" : join(problems, "; "));
    if (!problems.empty()) return;
    std::sort(cks.begin(), cks.end(),
              [](const Ck& a, const Ck& b) { return a.queueOffset < b.queueOffset; });
    fx.reviveQueueId = cks.front().reviveQid;

    // Drop exactly one: the highest queue offset, i.e. the last bit the bit set can carry.
    const Ck dropped = cks.back();
    const std::vector<Ck> kept(cks.begin(), cks.end() - 1);

    std::string err;
    captor.clear();
    const int32_t code = batchAck(client, fx.group, fx.topic, rawsOf(kept), fx.addr, err);
    Captured cap;
    if (!capturedOne(captor, label + " partial", fx.addr, cap)) {
        check(label + " partial BATCH_ACK_MESSAGE answered SUCCESS", false,
              "code=" + std::to_string(code) + (err.empty() ? "" : " " + err));
        return;
    }
    const std::vector<int64_t> keptOffsets = offsetsOf(kept);
    const std::vector<int64_t> acked = cap.allAckedOffsets();
    bool partialOk = sameOffsets(acked, keptOffsets);
    for (const int64_t o : acked) {
        if (o == dropped.queueOffset) partialOk = false;
    }
    check(label + " body acknowledges 5 of 6 offsets and not the dropped one", partialOk,
          "acked=" + joinInts(acked) + " want=" + joinInts(keptOffsets) + " dropped="
              + dropped.body + "@" + std::to_string(dropped.queueOffset));
    check(label + " partial BATCH_ACK_MESSAGE answered SUCCESS", code == ResponseCode::SUCCESS,
          "code=" + std::to_string(code) + (err.empty() ? "" : " " + err) + " over "
              + std::to_string(kept.size()) + " of " + std::to_string(cks.size()) + " checkpoints");

    // THE control: if a bit set were decorative, on the client or on the broker, this leg would look
    // exactly like L1.
    Eyes eyes;
    const bool broughtBack = probe(client, fx, eyes);
    check(label + " the unacked message really came back",
          broughtBack && eyes.countOf(dropped.body) > 0, eyes.summary());
    bool onlyDropped = eyes.cameBack();
    for (const std::string& b : eyes.bodies()) {
        if (b != dropped.body) onlyDropped = false;
    }
    check(label + " the five acked messages stayed gone while the dropped one came back", onlyDropped,
          "sighted=" + join(eyes.bodies(), " ") + " kept=" + join(bodiesOf(kept), " "));
    if (!broughtBack) return;

    // Stop the window and read the checkpoints straight away: their invisibility started the moment the
    // reply handed them over, and everything between the sighting and the ack has to be a local check.
    const std::vector<std::string> raws = eyes.checkpointsOf(dropped.body);
    std::vector<Ck> revived;
    std::vector<std::string> problems2;
    for (const std::string& raw : raws) {
        const Ck c = parseCk(raw, dropped.body);
        if (!c.parsable) problems2.push_back(c.problem);
        revived.push_back(c);
    }
    check(label + " the revived checkpoints are parsable", !revived.empty() && problems2.empty(),
          problems2.empty() ? std::to_string(revived.size()) + " checkpoints" : join(problems2, "; "));
    if (revived.empty()) return;

    bool markers = true;
    for (const Ck& c : revived) {
        if (c.retry != extra_info::kRetryTopic) markers = false;
    }
    check(label + " the revived checkpoints carry the retry marker (they live in the retry queue)",
          markers, "markers=" + join(markersOf(revived), ",") + " over "
              + std::to_string(revived.size()) + " checkpoints");

    // The body names the BUSINESS topic even though the checkpoints came out of the retry topic: the
    // broker re-derives the physical topic from marker + group, so passing the retry topic here would
    // double-wrap the name and ack nothing.
    captor.clear();
    const int32_t code2 = batchAck(client, fx.group, fx.topic, rawsOf(revived), fx.addr, err);
    Captured retryCap;
    if (!capturedOne(captor, label + " retry", fx.addr, retryCap)) {
        check(label + " retry BATCH_ACK_MESSAGE answered SUCCESS", false,
              "code=" + std::to_string(code2) + (err.empty() ? "" : " " + err));
        return;
    }
    bool retryBodyOk = !retryCap.entries.empty();
    for (const BodyEntry& e : retryCap.entries) {
        if (e.topic != fx.topic || e.retry != extra_info::kRetryTopic
            || e.consumerGroup != fx.group) {
            retryBodyOk = false;
        }
    }
    check(label + " the retry body keeps the business topic and carries the marker on every entry",
          retryBodyOk, retryCap.entriesDump());
    check(label + " retry BATCH_ACK_MESSAGE answered SUCCESS", code2 == ResponseCode::SUCCESS,
          "code=" + std::to_string(code2) + (err.empty() ? "" : " " + err) + " over "
              + std::to_string(revived.size()) + " revived checkpoints -> "
              + std::to_string(retryCap.entries.size()) + " entries");

    // Durable broker state, read only after the ack: the POP retry topic now has a route. Reading it
    // earlier could push the ack past the checkpoint's invisibility.
    std::string routeErr;
    check(label + " the POP retry topic exists on the name server (the revive service really moved it "
                   "there)",
          routeVisible(client, fx.retryTopic, routeErr), fx.retryTopic + (routeErr.empty() ? "" : " " + routeErr));

    Eyes settled;
    watch(client, fx, kObserveMs, settled);
    check(label + " the marker-driven retry ack silenced it (no further revival)", !settled.cameBack(),
          settled.summary());
}

// ------------------------------------------------------------------ L3: two rounds, one request

void legRounds(DefaultMQProducer& producer, MQClientInstance& client, BatchAckCaptor& captor,
               const std::string& stamp) {
    std::printf("=== L3 two pop rounds of one queue, one request ===\n");
    const std::string label = "L3";
    Leg fx;
    if (!setupLeg(producer, client, stamp, "CppBaRounds", "ba-rounds", 4, fx)) return;

    std::vector<Ck> rounds[2];
    for (int r = 0; r < 2; ++r) {
        PopResult res;
        try {
            // The first round's messages are invisible for the whole window, so the broker has no choice
            // but to hand out the other two -- that is what makes the rounds distinguishable by checkpoint.
            res = popOne(client, fx, fx.group, fx.topic, 0, 2, 0);
        } catch (const std::exception& e) {
            check(label + " POP round " + std::to_string(r) + " answers 2 messages", false, e.what());
            return;
        }
        if (!res.isFound() || res.msgFoundList.size() != 2) {
            check(label + " POP round " + std::to_string(r) + " answers 2 messages", false,
                  std::string("status=") + popStatusName(res.status) + " got="
                      + std::to_string(res.msgFoundList.size()));
            return;
        }
        std::vector<std::string> problems;
        rounds[r] = parseAll(res.msgFoundList, problems);
        check(label + " POP round " + std::to_string(r) + " answers 2 messages with parsable checkpoints",
              problems.empty(),
              problems.empty() ? join(bodiesOf(rounds[r]), " ") : join(problems, "; "));
        if (!problems.empty()) return;
    }

    std::vector<Ck> all;
    for (int r = 0; r < 2; ++r) {
        for (const Ck& c : rounds[r]) all.push_back(c);
    }
    bool disjoint = true;
    for (const Ck& a : rounds[0]) {
        for (const Ck& b : rounds[1]) {
            if (a.body == b.body) disjoint = false;
        }
    }
    const std::vector<int64_t> wantOffsets = offsetsOf(all);
    check(label + " the two rounds are disjoint batches of one queue",
          disjoint && sortedUniqueInts(wantOffsets).size() == 4,
          "offsets=" + joinInts(wantOffsets) + " A=" + join(bodiesOf(rounds[0]), " ") + " B="
              + join(bodiesOf(rounds[1]), " "));
    if (!disjoint) return;
    fx.reviveQueueId = all.front().reviveQid;

    const std::vector<std::string> keys = mergeKeysOf(all);
    check(label + " the two rounds produce different merge keys, so the body cannot collapse them",
          keys.size() == 2, join(keys, ",") + (keys.size() == 2 ? "" : " " + describe(all)));

    std::string err;
    captor.clear();
    const int32_t code = batchAck(client, fx.group, fx.topic, rawsOf(all), fx.addr, err);
    Captured cap;
    if (!capturedOne(captor, label, fx.addr, cap)) {
        check(label + " one BATCH_ACK_MESSAGE for two rounds answered SUCCESS", false,
              "code=" + std::to_string(code) + (err.empty() ? "" : " " + err));
        return;
    }
    const std::vector<int64_t> acked = cap.allAckedOffsets();
    check(label + " one body covers all 4 offsets exactly once in 2 entries",
          cap.entries.size() == 2 && acked.size() == 4 && sortedUniqueInts(acked).size() == 4
              && sameOffsets(acked, wantOffsets),
          "entries=" + std::to_string(cap.entries.size()) + " acked=" + joinInts(acked) + " want="
              + joinInts(wantOffsets) + " " + cap.entriesDump());
    assertEntryFields(cap, all, fx.group, fx.topic, label);
    assertBodyRoundTrip(cap, label);
    check(label + " one BATCH_ACK_MESSAGE for two rounds answered SUCCESS",
          code == ResponseCode::SUCCESS,
          "code=" + std::to_string(code) + (err.empty() ? "" : " " + err) + " over "
              + std::to_string(all.size()) + " checkpoints -> "
              + std::to_string(cap.entries.size()) + " entries");

    Eyes eyes;
    watch(client, fx, kObserveMs, eyes);
    check(label + " no message of either round revived inside 2.5x the window", !eyes.cameBack(),
          eyes.summary());
}

// ------------------------------------------------------------------ main

void printUsage(const char* argv0) {
    std::printf(
        "Usage: %s <namesrv> [legs]\n"
        "  <namesrv>  name server address, e.g. 127.0.0.1:9876. Required: BATCH_ACK_MESSAGE(200151)\n"
        "             is only provable against a real broker.\n"
        "  [legs]     which legs to run, e.g. l1,l2,l3 (default: all three)\n"
        "  Needs NameServer(9876) + Broker(10911) with autoCreateTopicEnable=true, and no dedicated\n"
        "  broker switch: 200151 is registered unconditionally.\n"
        "  A full run takes a few minutes: the silence windows are 2.5x the 10 s invisibility.\n",
        argv0);
}

std::set<std::string> parseLegs(const std::string& arg, bool& bad) {
    std::set<std::string> wanted;
    bad = false;
    if (arg.empty() || arg == "all") {
        wanted.insert("l1");
        wanted.insert("l2");
        wanted.insert("l3");
        return wanted;
    }
    size_t start = 0;
    while (start <= arg.size()) {
        const size_t comma = arg.find(',', start);
        std::string token = comma == std::string::npos ? arg.substr(start)
                                                      : arg.substr(start, comma - start);
        while (!token.empty() && (token.front() == ' ' || token.front() == '\t')) token.erase(token.begin());
        while (!token.empty() && (token.back() == ' ' || token.back() == '\t')) token.erase(token.end() - 1);
        std::transform(token.begin(), token.end(), token.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (token.empty()) {
            bad = true;
        } else if (token != "l1" && token != "l2" && token != "l3") {
            bad = true;
            std::printf("unknown leg '%s' (want l1, l2 or l3)\n", token.c_str());
        } else {
            wanted.insert(token);
        }
        if (comma == std::string::npos) break;
        start = comma + 1;
    }
    if (wanted.empty()) bad = true;
    return wanted;
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc >= 2) {
        const std::string first = argv[1];
        if (first == "-h" || first == "--help" || first == "help") {
            printUsage(argv[0]);
            return 0;
        }
    }
    if (argc < 2) {
        printUsage(argv[0]);
        return 2;
    }
    const std::string namesrv = argv[1];
    bool badLegs = false;
    const std::set<std::string> wanted = parseLegs(argc >= 3 ? argv[2] : std::string(), badLegs);
    if (badLegs) {
        printUsage(argv[0]);
        return 2;
    }

    const std::string stamp = std::to_string(nowMs() / 1000);
    std::printf("namesrv=%s stamp=%s invisibleTime=%lldms silenceWindow=%lldms legs=%s\n",
                namesrv.c_str(), stamp.c_str(), static_cast<long long>(kInvisibleMs),
                static_cast<long long>(kObserveMs),
                join(std::vector<std::string>(wanted.begin(), wanted.end()), ",").c_str());

    // The transport calls this hook with the request exactly as it goes out, so the bytes asserted
    // below are the bytes the broker receives. The producer installs its hook chain while starting,
    // so the captor has to be handed to the producer before start() -- a started instance already
    // owns the single hook slot and would refuse a second registration.
    auto captor = std::make_shared<BatchAckCaptor>();
    DefaultMQProducer producer("PG_CppBatchAck_" + stamp);
    producer.setNamesrvAddr(namesrv);
    producer.setRPCHook(captor);
    producer.start();
    MQClientInstance& client = producer.client();

    try {
        if (wanted.count("l1")) legAll(producer, client, *captor, stamp);
        if (wanted.count("l2")) legPartial(producer, client, *captor, stamp);
        if (wanted.count("l3")) legRounds(producer, client, *captor, stamp);
    } catch (const std::exception& e) {
        check("the run finished without an unexpected exception", false, e.what());
    }

    producer.shutdown();
    std::printf("############ PASS=%d FAIL=%d ############\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
