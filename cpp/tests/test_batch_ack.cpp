// Offline unit test for the batch POP acknowledge primitive (BATCH_ACK_MESSAGE) -- no cluster needed.
//
// Criteria are taken from the **wire traffic**: a real loopback endpoint reads the request frame off
// the socket, records the request code, the extFields and the decoded JSON body, and only what it
// actually captured decides a pass. The grouping rule and the per-entry offset-bit arithmetic are
// asserted on the recorded body, never on what the caller claims.
//
// The merge rule pinned here: the merge key is "<retry>@<queueId>@<ckQueueOffset>@<popTime>". The
// first extra info of a key creates one entry pre-filled from that info (startOffset = ckQueueOffset,
// plus consumerGroup/topic/retry/queueId/reviveQueueId/popTime/invisibleTime) with an empty offset
// bit set; a repeated key only sets the bit at index (queueOffset - ckQueueOffset). The body's broker
// name comes from the first extra info. The request carries no custom header, and the body uses the
// compact per-entry keys {c,t,r,so,q,rq,pt,it,b} under {"brokerName", "acks"}.
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <initializer_list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/extra_info.h"
#include "rocketmq/remoting/protocol/json.h"
#include "rocketmq/remoting/protocol/remoting_command.h"

using namespace rocketmq;

namespace {

using netcompat::kInvalidSocket;
using netcompat::socket_t;
using netcompat::socklen_type;

#ifdef _WIN32
constexpr int kShutdownHow = SD_BOTH;
#else
constexpr int kShutdownHow = SHUT_RDWR;
#endif

int fails = 0;
int checks = 0;

void expect(bool ok, const std::string& name, const std::string& detail = "") {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("FAIL %s %s\n", name.c_str(), detail.c_str());
    }
}

// ---------------------------------------------------------------- socket utilities

bool readN(socket_t s, char* buf, size_t n) {
    size_t got = 0;
    while (got < n) {
        int r = static_cast<int>(::recv(s, buf + got, static_cast<int>(n - got), 0));
        if (r <= 0) return false;
        got += static_cast<size_t>(r);
    }
    return true;
}

bool writeAll(socket_t s, const Bytes& data) {
    size_t sent = 0;
    while (sent < data.size()) {
        int n = static_cast<int>(::send(s, reinterpret_cast<const char*>(data.data() + sent),
                                        static_cast<int>(data.size() - sent),
                                        netcompat::sendFlags()));
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

// select until readable (a 100ms round so it exits promptly once alive is false), then read one frame.
bool readFrame(socket_t s, const std::atomic<bool>& alive, Bytes& out) {
    for (;;) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(s, &rfds);
        timeval tv;
        tv.tv_sec = 0;
        tv.tv_usec = 100 * 1000;
        int ready = ::select(netcompat::selectNfds(s), &rfds, nullptr, nullptr, &tv);
        if (!alive.load()) return false;
        if (ready == 0) continue;
        if (ready < 0) return false;
        break;
    }
    char lenBuf[4];
    if (!readN(s, lenBuf, 4)) return false;
    Bytes lenStr(lenBuf, 4);
    const int32_t totalLen = ByteReader::getInt32At(lenStr, 0);
    if (totalLen <= 0 || totalLen > 8 * 1024 * 1024) return false;
    Bytes body(static_cast<size_t>(totalLen), '\0');
    if (!readN(s, reinterpret_cast<char*>(body.data()), static_cast<size_t>(totalLen))) {
        return false;
    }
    out = lenStr + body;
    return true;
}

// ---------------------------------------------------------------- captured request

struct Captured {
    int32_t code = 0;
    PropertyMap extFields;
    std::string body;
};

// Binds one loopback port, records every request frame it receives, and answers SUCCESS (or the
// scripted BATCH_ACK code) so a synchronous client call returns once the frame is already recorded.
class RecordingEndpoint {
public:
    RecordingEndpoint() {
        netcompat::ensureInitialized();
        listen_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(listen_, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&one),
                     static_cast<int>(sizeof(one)));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        ::bind(listen_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ::listen(listen_, 16);
        socklen_type len = sizeof(addr);
        ::getsockname(listen_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        running_.store(true);
        acceptor_ = std::thread([this]() { acceptLoop(); });
    }

    ~RecordingEndpoint() {
        running_.store(false);
        if (acceptor_.joinable()) acceptor_.join();
        for (;;) {
            std::vector<std::thread> pending;
            {
                std::lock_guard<std::mutex> lk(workersM_);
                pending.swap(workers_);
            }
            if (pending.empty()) break;
            for (auto& t : pending) {
                if (t.joinable()) t.join();
            }
        }
        netcompat::closeSocket(listen_);
    }

    RecordingEndpoint(const RecordingEndpoint&) = delete;
    RecordingEndpoint& operator=(const RecordingEndpoint&) = delete;

    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }

    void clear() {
        std::lock_guard<std::mutex> lk(state_);
        captured_.clear();
    }

    void setBatchResponseCode(int32_t code) { batchRespCode_.store(code); }

    int countOf(int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        int n = 0;
        for (const Captured& c : captured_) {
            if (c.code == code) ++n;
        }
        return n;
    }

    // The most recent frame seen for `code`; false when none was captured.
    bool lastForCode(int32_t code, Captured& out) {
        std::lock_guard<std::mutex> lk(state_);
        for (auto it = captured_.rbegin(); it != captured_.rend(); ++it) {
            if (it->code == code) {
                out = *it;
                return true;
            }
        }
        return false;
    }

private:
    void acceptLoop() {
        while (running_.load()) {
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(listen_, &rfds);
            timeval tv;
            tv.tv_sec = 0;
            tv.tv_usec = 100 * 1000;
            int ready = ::select(netcompat::selectNfds(listen_), &rfds, nullptr, nullptr, &tv);
            if (ready == 0) continue;
            if (ready < 0) break;
            socket_t sock = ::accept(listen_, nullptr, nullptr);
            if (sock == kInvalidSocket) break;
            std::lock_guard<std::mutex> lk(workersM_);
            workers_.emplace_back([this, sock]() { serveConn(sock); });
        }
    }

    void serveConn(socket_t sock) {
        Bytes frame;
        while (running_.load() && readFrame(sock, running_, frame)) {
            RemotingCommand req;
            if (!RemotingCommand::tryDecode(frame, req, nullptr)) break;
            if (req.isResponseType()) continue;
            {
                std::lock_guard<std::mutex> lk(state_);
                Captured c;
                c.code = req.code;
                c.extFields = req.extFields;
                c.body.assign(req.body.begin(), req.body.end());
                captured_.push_back(std::move(c));
            }
            RemotingCommand resp;
            resp.opaque = req.opaque;
            resp.markResponseType();
            resp.code = req.code == RequestCode::BATCH_ACK_MESSAGE ? batchRespCode_.load()
                                                                   : ResponseCode::SUCCESS;
            Bytes out = resp.encode();
            if (!writeAll(sock, out)) break;
        }
        ::shutdown(sock, kShutdownHow);
        netcompat::closeSocket(sock);
    }

    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::atomic<int32_t> batchRespCode_{ResponseCode::SUCCESS};
    std::mutex state_;
    std::vector<Captured> captured_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- scaffolding

const char* kGroup = "GID_BatchAck";
const char* kTopic = "BatchAckTopic";
const char* kBroker = "broker-a";

std::unique_ptr<MQClientInstance> makeInstance() {
    return std::make_unique<MQClientInstance>("127.0.0.1@batchack#1",
                                              std::vector<std::string>{"127.0.0.1:1"});
}

// 8-segment POP_CK. The retry segment is derived from the topic shape by buildExtraInfo.
std::string ckFor(const std::string& topic, int32_t queueId, int64_t ckQueueOffset, int64_t popTime,
                  int64_t invisible, int32_t reviveQid, int64_t msgQueueOffset) {
    return extra_info::buildExtraInfo(ckQueueOffset, popTime, invisible, reviveQid, topic, kBroker,
                                      queueId, msgQueueOffset);
}

// Bounded predicate poll: a synchronous client call returns only after the frame was recorded, but
// this settles any residual visibility without an unbounded wait. maxMs is the explicit budget.
bool eventually(const std::function<bool()>& pred, int maxMs) {
    for (int waited = 0; waited <= maxMs; waited += 10) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

// ---------------------------------------------------------------- base64 + bitset decode helpers
//
// A standard-alphabet base64 decoder written here on purpose: the assertion must check the decoded
// wire bytes, not re-run the encoder the implementation used.

const char* kB64Table =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::vector<int> b64Decode(const std::string& text) {
    std::vector<int> out;
    int val = 0;
    int bits = 0;
    for (char ch : text) {
        if (ch == '=') break;
        const char* p = std::strchr(kB64Table, ch);
        if (p == nullptr) continue;  // ignore stray whitespace
        val = (val << 6) | static_cast<int>(p - kB64Table);
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out.push_back((val >> bits) & 0xFF);
        }
    }
    return out;
}

// The set of bit indices encoded in the offset bit-set byte array (bit i lives in byte i/8 at
// position i%8, little-endian with trailing zero bytes dropped).
std::set<int> bitsOf(const std::string& base64) {
    std::set<int> bits;
    const std::vector<int> bytes = b64Decode(base64);
    for (size_t i = 0; i < bytes.size(); ++i) {
        for (int k = 0; k < 8; ++k) {
            if ((bytes[i] >> k) & 1) bits.insert(static_cast<int>(i * 8 + k));
        }
    }
    return bits;
}

bool sameBits(const std::set<int>& actual, std::initializer_list<int> expected) {
    return actual == std::set<int>(expected);
}

const JsonValue* findAck(const JsonValue& root, int64_t queueId, int64_t startOffset) {
    const JsonValue* acks = root.find("acks");
    if (acks == nullptr || !acks->isArray()) return nullptr;
    for (size_t i = 0; i < acks->size(); ++i) {
        const JsonValue& o = acks->at(i);
        const JsonValue* q = o.find("q");
        const JsonValue* so = o.find("so");
        if (q && so && q->intValue() == queueId && so->intValue() == startOffset) return &o;
    }
    return nullptr;
}

// ---------------------------------------------------------------- 1. one request, single entry, both bits

void testSingleEntryBothBits(RecordingEndpoint& ep, MQClientInstance& inst) {
    ep.clear();
    const std::string addr = ep.address();
    // Three extra infos sharing the merge key 0@0@100@5000, offsets 100 / 103 / 100.
    std::vector<std::string> list = {
        ckFor(kTopic, 0, 100, 5000, 60000, 0, 100),
        ckFor(kTopic, 0, 100, 5000, 60000, 0, 103),
        ckFor(kTopic, 0, 100, 5000, 60000, 0, 100),
    };
    const int32_t rc = inst.batchAckMessage(kGroup, kTopic, list, 3000, addr);

    // positive: the batch request was sent
    expect(eventually([&] { return ep.countOf(RequestCode::BATCH_ACK_MESSAGE) == 1; }, 1000),
           "merge.oneBatchRequest");
    // control: the per-message acknowledge path was NOT used instead
    expect(ep.countOf(RequestCode::ACK_MESSAGE) == 0, "merge.noSingleAcksSent");

    Captured cap;
    expect(ep.lastForCode(RequestCode::BATCH_ACK_MESSAGE, cap), "merge.requestRecorded");
    JsonValue root;
    std::string err;
    const bool parsed = jsonParse(cap.body, root, &err);
    expect(parsed, "merge.bodyParses", err);
    if (!parsed) return;

    // positive: three infos of one key collapse to a single entry
    const JsonValue* acks = root.find("acks");
    expect(acks != nullptr && acks->isArray() && acks->size() == 1, "merge.oneEntryForOneKey",
           acks ? std::to_string(acks->size()) : std::string("<none>"));
    if (acks == nullptr || acks->size() != 1) return;
    const JsonValue& entry = acks->at(0);

    // control: had the merge key ignored queueId/ckQueueOffset/popTime the count would not be 1,
    // and a broken bit fold would drop one of the two offset bits -- both fail here, not elsewhere.
    std::set<int> bits = bitsOf(entry.get("b").stringValue());
    expect(sameBits(bits, {0, 3}), "merge.bothOffsetBitsSet", std::to_string(bits.size()));
    // control: the wire bytes must be byte-minimal (trailing zero bytes dropped), so bits {0,3}
    // is the single byte 0x09 ("CQ=="), not a word-padded 8-byte chunk. A padded encoder still
    // decodes back to {0,3}, so the bit check above cannot catch it -- only this string can.
    expect(entry.get("b").stringValue() == "CQ==", "merge.bitSetIsByteMinimal",
           entry.get("b").stringValue());
    expect(rc == ResponseCode::SUCCESS, "merge.returnsBrokerSuccess", std::to_string(rc));
}

// ---------------------------------------------------------------- 2. no custom header

void testNoCustomHeader(RecordingEndpoint& ep, MQClientInstance& inst) {
    ep.clear();
    const std::string addr = ep.address();
    std::vector<std::string> list = {ckFor(kTopic, 0, 7, 900, 30000, 0, 7)};
    inst.batchAckMessage(kGroup, kTopic, list, 3000, addr);

    Captured cap;
    expect(eventually([&] { return ep.lastForCode(RequestCode::BATCH_ACK_MESSAGE, cap); }, 1000),
           "header.batchRecorded");
    // positive: the batch request carries no custom header (empty extFields)
    expect(cap.extFields.empty(), "header.batchHasNoExtFields",
           std::to_string(cap.extFields.size()));

    // control: a header-bearing single ack on the same mock does populate extFields, proving the
    // empty batch extFields above is real and not the mock failing to capture headers at all.
    ep.clear();
    inst.ackMessage(kGroup, kTopic, 0, ckFor(kTopic, 0, 7, 900, 30000, 0, 7), 7, 3000, kBroker,
                    addr);
    Captured ackCap;
    expect(eventually([&] { return ep.lastForCode(RequestCode::ACK_MESSAGE, ackCap); }, 1000),
           "header.singleAckRecorded");
    expect(!ackCap.extFields.empty(), "header.singleAckHasExtFields",
           std::to_string(ackCap.extFields.size()));
}

// ---------------------------------------------------------------- 3. distinct merge keys -> two entries

void testDistinctKeys(RecordingEndpoint& ep, MQClientInstance& inst) {
    ep.clear();
    const std::string addr = ep.address();
    // queueId 0, ck 100, popTime 5000 -> entry 1; queueId 1, ck 200, popTime 6000 -> entry 2.
    // Each carries one offset, expressed relative to its own ckQueueOffset.
    std::vector<std::string> list = {
        ckFor(kTopic, 0, 100, 5000, 60000, 0, 100),  // bit 0
        ckFor(kTopic, 1, 200, 6000, 70000, 2, 205),  // bit 5 (205 - 200)
    };
    inst.batchAckMessage(kGroup, kTopic, list, 3000, addr);

    Captured cap;
    expect(eventually([&] { return ep.lastForCode(RequestCode::BATCH_ACK_MESSAGE, cap); }, 1000),
           "split.batchRecorded");
    JsonValue root;
    std::string err;
    expect(jsonParse(cap.body, root, &err), "split.bodyParses", err);
    const JsonValue* acks = root.find("acks");
    // positive: distinct keys stay distinct
    expect(acks != nullptr && acks->isArray() && acks->size() == 2, "split.twoEntriesForTwoKeys",
           acks ? std::to_string(acks->size()) : std::string("<none>"));
    if (acks == nullptr || acks->size() != 2) return;

    const JsonValue* e0 = findAck(root, 0, 100);
    const JsonValue* e1 = findAck(root, 1, 200);
    expect(e0 != nullptr && e1 != nullptr, "split.entriesLocatable");
    if (e0 == nullptr || e1 == nullptr) return;

    // control against the bug "bit index = absolute queueOffset": for entry 2 the relative index is
    // 5, an absolute index would be 205 and spill into the 26th byte. Staying at bit 5 is the proof.
    expect(sameBits(bitsOf(e0->get("b").stringValue()), {0}), "split.entry0BitRelativeToCk");
    expect(sameBits(bitsOf(e1->get("b").stringValue()), {5}), "split.entry1BitRelativeToCk");

    // pre-filled fields come from the first extra info of each key.
    expect(e1->get("rq").intValue() == 2, "split.entry1ReviveQueueId",
           std::to_string(e1->get("rq").intValue()));
    expect(e1->get("it").intValue() == 70000, "split.entry1InvisibleTime",
           std::to_string(e1->get("it").intValue()));
}

// ---------------------------------------------------------------- 4. retry dimension is in the key

void testRetryDimension(RecordingEndpoint& ep, MQClientInstance& inst) {
    ep.clear();
    const std::string addr = ep.address();
    const std::string retryTopic = extra_info::buildPopRetryTopicV1(kTopic, kGroup);
    // queueId 0, ck 100, popTime 5000 in BOTH -- only the retry segment differs (0 vs 1).
    std::vector<std::string> list = {
        ckFor(kTopic, 0, 100, 5000, 60000, 0, 100),      // retry 0, bit 0
        ckFor(retryTopic, 0, 100, 5000, 60000, 0, 100),  // retry 1, bit 0
        ckFor(kTopic, 0, 100, 5000, 60000, 0, 101),      // retry 0 again -> folds bit 1
    };
    inst.batchAckMessage(kGroup, kTopic, list, 3000, addr);

    Captured cap;
    expect(eventually([&] { return ep.lastForCode(RequestCode::BATCH_ACK_MESSAGE, cap); }, 1000),
           "retry.batchRecorded");
    JsonValue root;
    std::string err;
    expect(jsonParse(cap.body, root, &err), "retry.bodyParses", err);
    const JsonValue* acks = root.find("acks");
    // positive: a different retry flag splits the key into two entries
    expect(acks != nullptr && acks->isArray() && acks->size() == 2, "retry.normalVsRetryTwoEntries",
           acks ? std::to_string(acks->size()) : std::string("<none>"));
    if (acks == nullptr || acks->size() != 2) return;

    const JsonValue* normal = nullptr;
    const JsonValue* retry = nullptr;
    for (size_t i = 0; i < acks->size(); ++i) {
        const JsonValue& o = acks->at(i);
        if (o.get("r").stringValue() == "1") retry = &o;
        if (o.get("r").stringValue() == "0") normal = &o;
    }
    expect(normal != nullptr && retry != nullptr, "retry.bothRetryFlagsPresent");
    if (normal == nullptr || retry == nullptr) return;
    // control: the third info reused the normal key, so it folded into that entry rather than adding
    // a third one -- the retry segment is part of the key and dedup still applies within it.
    expect(sameBits(bitsOf(normal->get("b").stringValue()), {0, 1}), "retry.normalEntryFoldedBit");
    expect(sameBits(bitsOf(retry->get("b").stringValue()), {0}), "retry.retryEntrySingleBit");
}

// ---------------------------------------------------------------- 5. compact key spellings

void testCompactKeys(RecordingEndpoint& ep, MQClientInstance& inst) {
    ep.clear();
    const std::string addr = ep.address();
    std::vector<std::string> list = {ckFor(kTopic, 3, 42, 8888, 12345, 4, 45)};
    inst.batchAckMessage(kGroup, kTopic, list, 3000, addr);

    Captured cap;
    expect(eventually([&] { return ep.lastForCode(RequestCode::BATCH_ACK_MESSAGE, cap); }, 1000),
           "keys.batchRecorded");
    JsonValue root;
    std::string err;
    expect(jsonParse(cap.body, root, &err), "keys.bodyParses", err);
    // the envelope keys are the long form (they live on the body DTO, not the compact BatchAck)
    expect(root.find("brokerName") != nullptr, "keys.envelopeBrokerName");
    expect(root.get("brokerName").stringValue() == kBroker, "keys.brokerNameIsFirstExtraInfo",
           root.get("brokerName").stringValue());
    expect(root.find("acks") != nullptr, "keys.envelopeAcks");

    const JsonValue* acks = root.find("acks");
    if (acks == nullptr || acks->size() != 1) {
        expect(false, "keys.oneEntry", acks ? std::to_string(acks->size()) : std::string("<none>"));
        return;
    }
    const JsonValue& entry = acks->at(0);
    // positive: the compact keys are all present
    const char* compact[] = {"c", "t", "r", "so", "q", "rq", "pt", "it", "b"};
    bool allCompact = true;
    for (const char* k : compact) {
        if (entry.find(k) == nullptr) allCompact = false;
    }
    expect(allCompact, "keys.compactAllPresent");
    // control: none of the long-form aliases leaked into the body
    const char* longNames[] = {"consumerGroup", "topic",    "retry",       "startOffset",
                               "queueId",       "reviveQueueId", "popTime", "invisibleTime",
                               "bitSet"};
    bool noneLong = true;
    for (const char* k : longNames) {
        if (entry.find(k) != nullptr) noneLong = false;
    }
    expect(noneLong, "keys.noLongFormAliases");

    // exact field values from the single extra info
    expect(entry.get("c").stringValue() == kGroup, "keys.consumerGroup", entry.get("c").stringValue());
    expect(entry.get("t").stringValue() == kTopic, "keys.topic", entry.get("t").stringValue());
    expect(entry.get("r").stringValue() == "0", "keys.retry", entry.get("r").stringValue());
    expect(entry.get("so").intValue() == 42, "keys.startOffsetEqualsCk",
           std::to_string(entry.get("so").intValue()));
    expect(entry.get("q").intValue() == 3, "keys.queueId", std::to_string(entry.get("q").intValue()));
    expect(entry.get("rq").intValue() == 4, "keys.reviveQueueId",
           std::to_string(entry.get("rq").intValue()));
    expect(entry.get("pt").intValue() == 8888, "keys.popTime",
           std::to_string(entry.get("pt").intValue()));
    expect(entry.get("it").intValue() == 12345, "keys.invisibleTime",
           std::to_string(entry.get("it").intValue()));
    expect(sameBits(bitsOf(entry.get("b").stringValue()), {3}), "keys.bitIndexEqualsOffsetMinusCk",
           std::to_string(bitsOf(entry.get("b").stringValue()).size()));
}

// ---------------------------------------------------------------- 6. broker code passthrough

void testResponsePassthrough(RecordingEndpoint& ep, MQClientInstance& inst) {
    const std::string addr = ep.address();
    std::vector<std::string> list = {ckFor(kTopic, 0, 1, 2, 3, 0, 1)};

    ep.clear();
    ep.setBatchResponseCode(ResponseCode::SUCCESS);
    const int32_t okCode = inst.batchAckMessage(kGroup, kTopic, list, 3000, addr);
    expect(okCode == ResponseCode::SUCCESS, "resp.successPassthrough", std::to_string(okCode));

    // control: a different broker code must reach the caller verbatim -- a stub that always returned
    // SUCCESS (the pre-feature state) would keep this red.
    ep.clear();
    ep.setBatchResponseCode(ResponseCode::NO_MESSAGE);
    const int32_t missCode = inst.batchAckMessage(kGroup, kTopic, list, 3000, addr);
    expect(missCode == ResponseCode::NO_MESSAGE, "resp.errorPassthrough", std::to_string(missCode));
    ep.setBatchResponseCode(ResponseCode::SUCCESS);
}

// ---------------------------------------------------------------- 7. empty input is rejected locally

void testEmptyListRejected(RecordingEndpoint& ep, MQClientInstance& inst) {
    ep.clear();
    const std::string addr = ep.address();
    bool threwEmpty = false;
    try {
        inst.batchAckMessage(kGroup, kTopic, std::vector<std::string>{}, 3000, addr);
    } catch (const std::exception&) {
        threwEmpty = true;
    }
    // positive: an empty list never reaches the wire
    expect(threwEmpty, "empty.rejected");
    expect(ep.countOf(RequestCode::BATCH_ACK_MESSAGE) == 0, "empty.nothingSent");
    // control: a non-empty list is accepted (the rejection keys on emptiness, not on the call itself)
    bool threwOne = false;
    try {
        inst.batchAckMessage(kGroup, kTopic, {ckFor(kTopic, 0, 1, 2, 3, 0, 1)}, 3000, addr);
    } catch (const std::exception&) {
        threwOne = true;
    }
    expect(!threwOne, "empty.nonEmptyNotRejected");
    expect(eventually([&] { return ep.countOf(RequestCode::BATCH_ACK_MESSAGE) == 1; }, 1000),
           "empty.nonEmptySentOnce");
}

}  // namespace

int main() {
    RecordingEndpoint ep;
    auto inst = makeInstance();

    testSingleEntryBothBits(ep, *inst);
    testNoCustomHeader(ep, *inst);
    testDistinctKeys(ep, *inst);
    testRetryDimension(ep, *inst);
    testCompactKeys(ep, *inst);
    testResponsePassthrough(ep, *inst);
    testEmptyListRejected(ep, *inst);

    std::printf("batch_ack: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
