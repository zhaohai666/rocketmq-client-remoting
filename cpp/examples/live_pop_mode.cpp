// Live-broker proof for SET_MESSAGE_REQUEST_MODE(401) and its only in-protocol read-back,
// GET_ALL_MESSAGE_REQUEST_MODE(402): a request code that carries no custom header at all.
// Usage: rmq_live_pop_mode 127.0.0.1:9876 [legs]     # legs = l1,l2,l3 (default: all)
//
// What is on the wire for 401, and only that: the broker decodes the request body as ONE JSON object
// {topic, consumerGroup, mode, popShareQueueNum} and reads all four values out of that object alone. `mode`
// is the string "POP" or "PULL"; `popShareQueueNum` is a JSON number present even when 0; there is no
// custom header. A 401 whose four values ride the header with an empty body leaves the broker with nothing
// to decode: it answers code=1 (SYSTEM_ERROR) naming the decoded body as null, and the mode does not switch.
// That header form is rejected, not silently accepted.
//
// Whether a mode really switched is observable through exactly one door: 402. It carries no body and no
// header, and its response body is
//   {"messageRequestModeMap": {"<topic>": {"<consumerGroup>": {topic, consumerGroup, mode, popShareQueueNum}}}}}
// A never-set mode answers an error with an empty body; a read-back with no usable body is treated here as an
// empty map, so "absent" is never confused with "the door was dead".
//
//   L1 POSITIVE. Point the production 401 (values in the JSON body) at a fresh topic with POP and 4,
//      then read 402 and require an entry keyed by exactly this topic and this group whose mode is the
//      string POP and whose popShareQueueNum is the number 4.
//   L2 REPLACEMENT. Point the production 401 again at the same topic+group with PULL and 0, re-read 402
//      and require the mode is now PULL and the popShareQueueNum is the number 0: the second write is the
//      one visible, and a number survives at 0.
//   L3 PAIRED CONTROL. On the same topic but with a second, different group, send a RAW 401 whose four
//      values ride the header and whose body is empty. Print the response code and remark, then re-read
//      402 and require the control group to be absent from the map under this topic and the response code
//      to be not SUCCESS. Absence is only meaningful against a served read-back, so this leg first anchors
//      the topic with one body-form write. It is what makes L1/L2 more than "the broker answered
//      something": it shows the header form -- the shape a regression would send -- switches nothing.
//
// Every live assertion is gated on broker-visible state (a route, a 402 read-back), never on a bare sleep:
// reads poll within an explicit budget, so a stale name can never satisfy a current assertion and a slow
// broker never turns a correct write red. The topic is created before any 401 because the broker refuses a
// 401 for a topic with no config, which would otherwise look like a broken request shape; the subscription
// group is left to the broker's auto-create.
//
// Output is [PASS] / [FAIL] per check plus a final "PASS=<n> FAIL=<m>" line; exits non-zero on any FAIL.
#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <set>
#include <string>
#include <thread>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/json.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/protocol/route.h"

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

constexpr int32_t kRpcTimeoutMs = 5000;
constexpr int64_t kRouteWaitMs = 30000;
constexpr int64_t kReadbackWaitMs = 15000;
constexpr int64_t kPollIntervalMs = 500;

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

void sleepMs(int64_t ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

std::string clip(const std::string& s, size_t max = 200) {
    return s.size() <= max ? s : (s.substr(0, max) + "...");
}

std::string showOr(const std::string& s, const std::string& ifEmpty) {
    return s.empty() ? ifEmpty : s;
}

// ------------------------------------------------------------------ the 402 read-back
//
// One entry of the map, in exactly the terms the broker decoded it into. Nothing is inferred: the mode is
// read as a string and the share count only counts as a number when the JSON value really is one.
struct ModeEntry {
    bool present = false;
    std::string mode;
    bool hasNum = false;
    int64_t num = 0;
    std::string raw;

    std::string describe() const {
        if (!present) return "absent";
        return "{mode=" + showOr(mode, "<none>") + " popShareQueueNum="
            + (hasNum ? std::to_string(num) : "<absent>") + " " + clip(raw) + "}";
    }
};

// Reads {"messageRequestModeMap":{topic:{group:{...entry...}}}}. A topic or group that is not in the map
// yields present=false; so does an empty or unparseable body, which is how "no mode set yet" shows up.
ModeEntry findEntry(const JsonValue& root, const std::string& topic, const std::string& group) {
    ModeEntry e;
    const JsonValue* map = root.find("messageRequestModeMap");
    if (map == nullptr || !map->isObject()) return e;
    const JsonValue* perTopic = map->find(topic);
    if (perTopic == nullptr || !perTopic->isObject()) return e;
    const JsonValue* ent = perTopic->find(group);
    if (ent == nullptr || !ent->isObject()) return e;
    e.present = true;
    const JsonValue* modeV = ent->find("mode");
    if (modeV != nullptr) e.mode = modeV->stringValue();
    const JsonValue* numV = ent->find("popShareQueueNum");
    e.hasNum = (numV != nullptr) && numV->isNumber();
    if (e.hasNum) e.num = numV->intValue();
    e.raw = ent->dump();
    return e;
}

struct Readback {
    int32_t code = 0;
    std::string remark;
    bool usableBody = false;
    std::string body;
    JsonValue root;

    std::string meta() const {
        return "402 code=" + std::to_string(code)
            + " remark=" + showOr(clip(remark), "(none)")
            + " bodyBytes=" + std::to_string(body.size())
            + (usableBody ? " mapUsable" : " noUsableBody->emptyMap");
    }
};

// 402 carries no header and no body. The raw invoke is used because a never-set map answers an error, and
// the caller must see the answer rather than have it raised as an exception.
Readback readAllModes(MQClientInstance& client, const std::string& addr) {
    Readback rb;
    RemotingCommand resp = client.invokeSyncRaw(addr, RequestCode::GET_ALL_MESSAGE_REQUEST_MODE,
                                               PropertyMap(), Bytes(), false, kRpcTimeoutMs);
    rb.code = resp.code;
    rb.remark = resp.remark;
    rb.body = resp.body;
    if (!resp.body.empty()) {
        std::string err;
        JsonValue parsed;
        if (jsonParse(resp.body, parsed, &err) && parsed.isObject()) {
            rb.root = parsed;
            rb.usableBody = true;
        }
    }
    return rb;
}

// Polls the 402 map until the (topic, group) entry satisfies `done`, within a budget. The read-back is
// broker-visible state, so a positive write settles almost immediately; the polling is what lets a leg
// assert on what the broker really holds instead of on a guess timed against a sleep.
ModeEntry pollMode(MQClientInstance& client, const std::string& addr, const std::string& topic,
                   const std::string& group, const std::function<bool(const ModeEntry&)>& done,
                   Readback& last) {
    const int64_t until = nowMs() + kReadbackWaitMs;
    ModeEntry e;
    for (;;) {
        last = readAllModes(client, addr);
        e = findEntry(last.root, topic, group);
        if (done(e)) return e;
        if (nowMs() >= until) return e;
        sleepMs(kPollIntervalMs);
    }
}

// ------------------------------------------------------------------ legs

// Production 401 with the four values in the JSON body. Returns "" on SUCCESS, the exception text
// otherwise -- a non-SUCCESS 401 raises, so a throw here already says the broker rejected the request.
std::string setModeBodyForm(DefaultMQAdminExt& admin, const std::string& addr,
                            const std::string& topic, const std::string& group,
                            const std::string& mode, int32_t num) {
    try {
        admin.setMessageRequestMode(addr, topic, group, mode, num);
        return std::string();
    } catch (const std::exception& e) {
        return e.what();
    }
}

void legPositive(DefaultMQAdminExt& admin, MQClientInstance& client, const std::string& addr,
                 const std::string& topic, const std::string& group) {
    std::printf("=== L1 positive: production 401 (values in the body) -> POP/4 is visible in 402 ===\n");
    const std::string label = "L1";

    const std::string callErr = setModeBodyForm(admin, addr, topic, group, "POP", 4);
    check(label + " the production 401 with all four values in the JSON body is answered SUCCESS",
          callErr.empty(),
          "topic=" + topic + " group=" + group + " mode=POP popShareQueueNum=4"
              + (callErr.empty() ? std::string() : " raised: " + callErr));

    Readback last;
    const ModeEntry e = pollMode(client, addr, topic, group,
                                 [](const ModeEntry& x) {
                                     return x.present && x.mode == "POP" && x.hasNum && x.num == 4;
                                 },
                                 last);
    check(label + " the 402 map holds an entry keyed by exactly this topic and this group",
          e.present, last.meta() + " entry=" + e.describe());
    check(label + " that entry's mode is the string POP (the request really reached the mode store)",
          e.present && e.mode == "POP", "mode=" + showOr(e.mode, "<absent>") + " " + e.describe());
    check(label + " that entry's popShareQueueNum is the JSON number 4",
          e.hasNum && e.num == 4, "popShareQueueNum=" + (e.hasNum ? std::to_string(e.num)
                                                                   : std::string("<absent>")));
}

void legReplacement(DefaultMQAdminExt& admin, MQClientInstance& client, const std::string& addr,
                    const std::string& topic, const std::string& group) {
    std::printf("=== L2 replacement: production 401 again -> PULL/0 overwrites the earlier POP/4 ===\n");
    const std::string label = "L2";

    const std::string callErr = setModeBodyForm(admin, addr, topic, group, "PULL", 0);
    check(label + " the production 401 re-issued with PULL and popShareQueueNum 0 is answered SUCCESS",
          callErr.empty(),
          "topic=" + topic + " group=" + group + " mode=PULL popShareQueueNum=0"
              + (callErr.empty() ? std::string() : " raised: " + callErr));

    Readback last;
    const ModeEntry e = pollMode(client, addr, topic, group,
                                 [](const ModeEntry& x) {
                                     return x.present && x.mode == "PULL" && x.hasNum && x.num == 0;
                                 },
                                 last);
    check(label + " the same topic+group is still keyed in the 402 map after the second write",
          e.present, last.meta() + " entry=" + e.describe());
    check(label + " the visible mode is now the string PULL, not the earlier POP (the second write wins)",
          e.present && e.mode == "PULL", "mode=" + showOr(e.mode, "<absent>") + " " + e.describe());
    check(label + " the visible popShareQueueNum is the JSON number 0 (present even at zero)",
          e.hasNum && e.num == 0,
          "popShareQueueNum=" + (e.hasNum ? std::to_string(e.num) : std::string("<absent>")));
}

void legHeaderControl(DefaultMQAdminExt& admin, MQClientInstance& client, const std::string& addr,
                      const std::string& topic, const std::string& group,
                      const std::string& ctrlGroup) {
    std::printf("=== L3 paired control: raw 401 riding the header with no body is rejected ===\n");
    const std::string label = "L3";

    // Anchor the read-back with one known-good body-form write on the main group, so a later "absent"
    // verdict for the control group is measured against a map the broker really served, not an empty one.
    const std::string anchorErr = setModeBodyForm(admin, addr, topic, group, "POP", 1);
    Readback anchorRead;
    const ModeEntry anchor = pollMode(client, addr, topic, group,
                                      [](const ModeEntry& x) {
                                          return x.present && x.mode == "POP" && x.hasNum && x.num == 1;
                                      },
                                      anchorRead);
    check(label + " control anchor: a body-form write for the main group is visible in 402, so the "
                  "read-back door is served and the control's absence below is real, not an empty map",
          anchorErr.empty() && anchor.present,
          (anchorErr.empty() ? std::string() : "anchor write raised: " + anchorErr + " ")
              + anchorRead.meta() + " anchor=" + anchor.describe());

    // The rejected shape the control sends: the four values as custom-header fields, an empty body.
    PropertyMap ext;
    ext["topic"] = topic;
    ext["consumerGroup"] = ctrlGroup;
    ext["mode"] = "POP";
    ext["popShareQueueNum"] = "4";
    RemotingCommand resp = client.invokeSyncRaw(addr, RequestCode::SET_MESSAGE_REQUEST_MODE, ext,
                                                Bytes(), false, kRpcTimeoutMs);
    std::printf("        header-form control 401: topic=%s group=%s -> response code=%d remark=%s\n",
                topic.c_str(), ctrlGroup.c_str(), static_cast<int>(resp.code),
                showOr(clip(resp.remark), "(empty remark)").c_str());
    check(label + " the header-form 401 with no body is answered with a code other than SUCCESS",
          resp.code != ResponseCode::SUCCESS,
          "code=" + std::to_string(static_cast<int>(resp.code))
              + " remark=" + showOr(clip(resp.remark), "(empty remark)"));

    // Give the control group the same read-back window a positive write is held to: if the header form
    // could switch a mode, it would appear here; absence across the whole window is the verdict.
    Readback last;
    const ModeEntry control = pollMode(client, addr, topic, ctrlGroup,
                                       [](const ModeEntry& x) { return x.present; }, last);
    check(label + " the control group is absent from the 402 map under this topic (the header form "
                  "switched nothing)",
          !control.present, last.meta() + " control=" + control.describe());
    const ModeEntry mainStill = findEntry(last.root, topic, group);
    check(label + " the same read-back still keys the anchor group under this topic, proving the map was "
                  "served while the control stayed absent",
          mainStill.present, "anchor entry=" + mainStill.describe());
}

// ------------------------------------------------------------------ setup

// The topic is created before any 401 because the broker refuses a 401 for a topic with no config. The
// master broker address is taken from the route -- not hardcoded -- and polled until the route names one.
bool resolveMaster(DefaultMQAdminExt& admin, const std::string& topic, std::string& addr,
                   std::string& brokerName, std::string& err) {
    const int64_t until = nowMs() + kRouteWaitMs;
    for (;;) {
        try {
            TopicRouteData route = admin.examineTopicRoute(topic);
            if (!route.brokerDatas.empty()) {
                brokerName = route.brokerDatas.front().brokerName;
                addr = MQClientInstance::findBrokerAddrInRoute(route, brokerName);
                if (!addr.empty()) return true;
            }
            err = "route has no master yet";
        } catch (const std::exception& e) {
            err = e.what();
        }
        if (nowMs() >= until) return false;
        sleepMs(kPollIntervalMs);
    }
}

// ------------------------------------------------------------------ main

void printUsage(const char* argv0) {
    std::printf(
        "Usage: %s <namesrv> [legs]\n"
        "  <namesrv>  name server address, e.g. 127.0.0.1:9876. Required: 401/402 are only provable\n"
        "             against a real broker.\n"
        "  [legs]     which legs to run, e.g. l1,l2,l3 (default: all three)\n"
        "  Needs NameServer(9876) + Broker(10911) with autoCreateTopicEnable and\n"
        "  autoCreateSubscriptionGroup on. This tool never changes broker configuration.\n",
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
    const std::string topic = "PopModeCppTopic_" + stamp;
    const std::string group = "GID_PopModeCpp_" + stamp;
    const std::string ctrlGroup = "GID_PopModeCppCtrl_" + stamp;
    std::string legList;
    for (const std::string& l : wanted) {
        if (!legList.empty()) legList += ",";
        legList += l;
    }
    std::printf("namesrv=%s stamp=%s topic=%s group=%s controlGroup=%s legs=%s\n", namesrv.c_str(),
                stamp.c_str(), topic.c_str(), group.c_str(), ctrlGroup.c_str(), legList.c_str());

    DefaultMQAdminExt admin("ADMIN_PopMode_" + stamp);
    admin.setNamesrvAddr(namesrv);
    admin.start();

    std::string addr;
    std::string brokerName;
    bool ready = false;
    try {
        admin.createTopic(MixAll::DEFAULT_TOPIC, topic, 1);
        std::string routeErr;
        ready = resolveMaster(admin, topic, addr, brokerName, routeErr);
        check("setup: fresh single-queue topic created on the broker before any 401", ready,
              ready ? ("topic=" + topic + " brokerName=" + brokerName + " master=" + addr)
                    : (topic + " route/master not resolvable: " + routeErr));
    } catch (const std::exception& e) {
        check("setup: fresh single-queue topic created on the broker before any 401", false, e.what());
    }

    if (ready) {
        MQClientInstance& client = admin.client();
        try {
            if (wanted.count("l1")) legPositive(admin, client, addr, topic, group);
            if (wanted.count("l2")) legReplacement(admin, client, addr, topic, group);
            if (wanted.count("l3")) legHeaderControl(admin, client, addr, topic, group, ctrlGroup);
        } catch (const std::exception& e) {
            check("the run finished without an unexpected exception", false, e.what());
        }
    }

    // Cleanup is best-effort: a failure to delete prints a note and must not turn the run red.
    try {
        admin.deleteTopic(topic);
        std::printf("  [note] cleanup: asked the broker and name server to delete topic %s\n",
                    topic.c_str());
    } catch (const std::exception& e) {
        std::printf("  [note] cleanup: could not delete topic %s (%s); leaving it to expire\n",
                    topic.c_str(), e.what());
    }

    admin.shutdown();
    std::printf("############ PASS=%d FAIL=%d ############\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
