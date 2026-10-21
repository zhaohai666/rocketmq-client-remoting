// Wire-contract tests for the AUTH user-management and ACL-management admin operations (3001..3010).
// No cluster is required.
//
// A loopback listener plays the broker: for every request in 3001..3010 it records the request code,
// the exact extFields map and the exact request-body bytes, then answers from a per-code script.
// A second listener sits exactly two ports below the first, and the admin runs with the VIP channel
// switched on, so any port-shifted addressing shows up as a recorded arrival on the decoy instead of
// silently "working" against a real broker.
// Every request-body expectation compares the full byte string (key presence, absence and order), and
// every positive assertion is paired with an absence control, because the wire spelling of these
// operations is invisible on a single broker and a padded or renamed key still round-trips through
// the decoder.
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/remoting/protocol/admin_body.h"
#include "rocketmq/remoting/protocol/codes.h"
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

int checks = 0;
int fails = 0;

void expect(bool ok, const std::string& name, const std::string& detail = "") {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("FAIL %s %s\n", name.c_str(), detail.c_str());
    }
}

void expectInt(long long actual, long long expected, const std::string& name) {
    ++checks;
    if (actual != expected) {
        ++fails;
        std::printf("FAIL %s (actual=%lld expected=%lld)\n", name.c_str(), actual, expected);
    }
}

void expectStr(const std::string& actual, const std::string& expected, const std::string& name) {
    ++checks;
    if (actual != expected) {
        ++fails;
        std::printf("FAIL %s\n  actual=[%s]\n  expect=[%s]\n", name.c_str(), actual.c_str(),
                    expected.c_str());
    }
}

bool isAuthCode(int32_t code) { return code >= 3001 && code <= 3010; }

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
    Bytes payload(static_cast<size_t>(totalLen), '\0');
    if (!readN(s, reinterpret_cast<char*>(payload.data()), static_cast<size_t>(totalLen))) {
        return false;
    }
    out = lenStr + payload;
    return true;
}

// ---------------------------------------------------------------- recording listener

struct ScriptedReply {
    int32_t code = ResponseCode::SUCCESS;
    std::string remark;
    std::string body;
};

struct WireRecord {
    int32_t code = 0;
    PropertyMap ext;
    std::string body;
};

class RecordingListener {
public:
    // port == 0 asks the kernel for an ephemeral port
    explicit RecordingListener(uint16_t port = 0) {
        netcompat::ensureInitialized();
        listen_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(listen_, SOL_SOCKET, SO_REUSEADDR,
                     reinterpret_cast<const char*>(&one), static_cast<int>(sizeof(one)));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = htons(port);
        if (::bind(listen_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            netcompat::closeSocket(listen_);
            listen_ = kInvalidSocket;
            return;
        }
        if (::listen(listen_, 16) != 0) {
            netcompat::closeSocket(listen_);
            listen_ = kInvalidSocket;
            return;
        }
        socklen_type len = sizeof(addr);
        ::getsockname(listen_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        running_.store(true);
        acceptor_ = std::thread([this]() { acceptLoop(); });
    }

    ~RecordingListener() {
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
        if (listen_ != kInvalidSocket) netcompat::closeSocket(listen_);
    }

    bool ok() const { return listen_ != kInvalidSocket; }
    uint16_t port() const { return port_; }
    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }

    // Sets the script for one request code and clears the arrivals recorded for it.
    void script(int32_t code, std::vector<ScriptedReply> replies) {
        std::lock_guard<std::mutex> lk(state_);
        scripts_[code] = std::move(replies);
        records_.erase(code);
        arrivals_.erase(code);
    }

    int requestCount(int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        auto it = arrivals_.find(code);
        return it == arrivals_.end() ? 0 : it->second;
    }

    WireRecord record(int32_t code, size_t i) {
        std::lock_guard<std::mutex> lk(state_);
        auto it = records_.find(code);
        if (it == records_.end() || i >= it->second.size()) return WireRecord{};
        return it->second[i];
    }

    // Every AUTH arrival seen so far, across all codes
    int authTotal() {
        std::lock_guard<std::mutex> lk(state_);
        int total = 0;
        for (const auto& kv : arrivals_) {
            if (isAuthCode(kv.first)) total += kv.second;
        }
        return total;
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
            RemotingCommand resp;
            resp.opaque = req.opaque;
            resp.markResponseType();
            if (isAuthCode(req.code)) {
                ScriptedReply reply;
                {
                    std::lock_guard<std::mutex> lk(state_);
                    int idx = arrivals_[req.code]++;
                    records_[req.code].push_back(
                        {req.code, req.extFields, std::string(req.body.begin(), req.body.end())});
                    auto it = scripts_.find(req.code);
                    if (it != scripts_.end() && idx < static_cast<int>(it->second.size())) {
                        reply = it->second[static_cast<size_t>(idx)];
                    }
                }
                resp.code = reply.code;
                resp.remark = reply.remark;
                resp.hasRemark = !reply.remark.empty();
                if (!reply.body.empty()) {
                    resp.body.assign(reply.body.begin(), reply.body.end());
                    resp.hasBody = true;
                }
            } else {
                resp.code = ResponseCode::SUCCESS;
            }
            Bytes out = resp.encode();
            if (!writeAll(sock, out)) break;
        }
        ::shutdown(sock, kShutdownHow);
        netcompat::closeSocket(sock);
    }

    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::mutex state_;
    std::map<int32_t, std::vector<ScriptedReply>> scripts_;
    std::map<int32_t, std::vector<WireRecord>> records_;
    std::map<int32_t, int> arrivals_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- assertions on recorded state

void expectExt(const PropertyMap& ext,
               const std::vector<std::pair<std::string, std::string>>& want,
               const std::string& name) {
    expectInt(static_cast<long long>(ext.size()), static_cast<long long>(want.size()),
              name + ".extFieldCount");
    for (const auto& kv : want) {
        auto it = ext.find(kv.first);
        expect(it != ext.end() && it->second == kv.second, name + ".ext." + kv.first,
               it == ext.end() ? std::string("(absent)") : it->second);
    }
}

// ---------------------------------------------------------------- user management (3001..3005)

void testCreateUserConvenience(DefaultMQAdminExt& admin, RecordingListener& peer) {
    peer.script(RequestCode::AUTH_CREATE_USER, {{ResponseCode::SUCCESS, "", ""}});
    admin.createUser(peer.address(), "rmonitor", "pwd#2026", UserType::SUPER);

    expectInt(peer.requestCount(RequestCode::AUTH_CREATE_USER), 1, "createUser.convenience.sent");
    const WireRecord rec = peer.record(RequestCode::AUTH_CREATE_USER, 0);
    expectInt(rec.code, RequestCode::AUTH_CREATE_USER, "createUser.convenience.requestCode");
    expectExt(rec.ext, {{"username", "rmonitor"}}, "createUser.convenience");
    expectStr(rec.body,
              "{\"username\":\"rmonitor\",\"password\":\"pwd#2026\",\"userType\":\"Super\"}",
              "createUser.convenience.bodyBytes");
    expect(rec.body.find("userStatus") == std::string::npos,
           "createUser.convenience.bodyHasNoUserStatusKey", rec.body);
    expect(rec.body.find("null") == std::string::npos,
           "createUser.convenience.bodyHasNoNullLiteral", rec.body);
}

void testCreateUserObject(DefaultMQAdminExt& admin, RecordingListener& peer) {
    peer.script(RequestCode::AUTH_CREATE_USER, {{ResponseCode::SUCCESS, "", ""}});
    UserInfo user;
    user.username = "opsuser";
    user.password = "pw1";
    user.hasPassword = true;
    user.userType = UserType::NORMAL;
    user.hasUserType = true;
    user.userStatus = UserStatus::DISABLE;
    user.hasUserStatus = true;
    admin.createUser(peer.address(), user);

    const WireRecord rec = peer.record(RequestCode::AUTH_CREATE_USER, 0);
    expectInt(rec.code, RequestCode::AUTH_CREATE_USER, "createUser.object.requestCode");
    expectExt(rec.ext, {{"username", "opsuser"}}, "createUser.object");
    expectStr(rec.body,
              "{\"username\":\"opsuser\",\"password\":\"pw1\",\"userType\":\"Normal\","
              "\"userStatus\":\"disable\"}",
              "createUser.object.bodyBytes");
}

void testUpdateUser(DefaultMQAdminExt& admin, RecordingListener& peer) {
    peer.script(RequestCode::AUTH_UPDATE_USER, {{ResponseCode::SUCCESS, "", ""}});
    admin.updateUser(peer.address(), "rmonitor", "pwd2", UserType::NORMAL);

    expectInt(peer.requestCount(RequestCode::AUTH_UPDATE_USER), 1, "updateUser.sent");
    const WireRecord rec = peer.record(RequestCode::AUTH_UPDATE_USER, 0);
    expectInt(rec.code, RequestCode::AUTH_UPDATE_USER, "updateUser.requestCode");
    expectExt(rec.ext, {{"username", "rmonitor"}}, "updateUser.convenience");
    expectStr(rec.body, "{\"username\":\"rmonitor\",\"password\":\"pwd2\",\"userType\":\"Normal\"}",
              "updateUser.convenience.bodyBytes");
    expect(rec.body.find("userStatus") == std::string::npos,
           "updateUser.convenience.bodyHasNoUserStatusKey", rec.body);

    peer.script(RequestCode::AUTH_UPDATE_USER, {{ResponseCode::SUCCESS, "", ""}});
    UserInfo user;
    user.username = "rmonitor";
    user.userStatus = UserStatus::ENABLE;
    user.hasUserStatus = true;
    admin.updateUser(peer.address(), user);
    const WireRecord rec2 = peer.record(RequestCode::AUTH_UPDATE_USER, 0);
    expectStr(rec2.body, "{\"username\":\"rmonitor\",\"userStatus\":\"enable\"}",
              "updateUser.object.statusOnlyBodyBytes");
    expect(rec2.body.find("password") == std::string::npos, "updateUser.object.noPasswordKey",
           rec2.body);
}

void testDeleteUser(DefaultMQAdminExt& admin, RecordingListener& peer) {
    peer.script(RequestCode::AUTH_DELETE_USER, {{ResponseCode::SUCCESS, "", ""}});
    admin.deleteUser(peer.address(), "rmonitor");

    expectInt(peer.requestCount(RequestCode::AUTH_DELETE_USER), 1, "deleteUser.sent");
    const WireRecord rec = peer.record(RequestCode::AUTH_DELETE_USER, 0);
    expectInt(rec.code, RequestCode::AUTH_DELETE_USER, "deleteUser.requestCode");
    expectExt(rec.ext, {{"username", "rmonitor"}}, "deleteUser");
    expect(rec.body.empty(), "deleteUser.carriesNoBody", rec.body);
}

void testGetUser(DefaultMQAdminExt& admin, RecordingListener& peer) {
    peer.script(RequestCode::AUTH_GET_USER,
                {{ResponseCode::SUCCESS, "",
                  "{\"username\":\"alice\",\"password\":\"s3cret\",\"userType\":\"Super\","
                  "\"userStatus\":\"enable\"}"}});
    UserInfo got = admin.getUser(peer.address(), "alice");

    expectInt(peer.requestCount(RequestCode::AUTH_GET_USER), 1, "getUser.sent");
    const WireRecord rec = peer.record(RequestCode::AUTH_GET_USER, 0);
    expectInt(rec.code, RequestCode::AUTH_GET_USER, "getUser.requestCode");
    expectExt(rec.ext, {{"username", "alice"}}, "getUser");
    expect(rec.body.empty(), "getUser.carriesNoBody", rec.body);
    expectStr(got.username, "alice", "getUser.decoded.username");
    expectStr(got.password, "s3cret", "getUser.decoded.password");
    expect(got.hasPassword, "getUser.decoded.hasPassword");
    expectStr(got.userType, "Super", "getUser.decoded.userType");
    expect(got.hasUserType, "getUser.decoded.hasUserType");
    expectStr(got.userStatus, "enable", "getUser.decoded.userStatus");
    expect(got.hasUserStatus, "getUser.decoded.hasUserStatus");

    // control: absent keys stay absent (a decoder that pads defaults would pass the case above
    // but fail here)
    peer.script(RequestCode::AUTH_GET_USER, {{ResponseCode::SUCCESS, "", "{\"username\":\"bob\"}"}});
    UserInfo partial = admin.getUser(peer.address(), "bob");
    expectStr(partial.username, "bob", "getUser.partial.username");
    expect(!partial.hasPassword, "getUser.partial.noPasswordKey");
    expect(!partial.hasUserType, "getUser.partial.noUserTypeKey");
    expect(!partial.hasUserStatus, "getUser.partial.noUserStatusKey");

    // empty body: the untouched default value signals "nothing there"
    peer.script(RequestCode::AUTH_GET_USER, {{ResponseCode::SUCCESS, "", ""}});
    UserInfo empty = admin.getUser(peer.address(), "ghost");
    expect(empty.username.empty() && !empty.hasPassword && !empty.hasUserType &&
               !empty.hasUserStatus,
           "getUser.emptyBodyIsDefault");
}

void testListUser(DefaultMQAdminExt& admin, RecordingListener& peer) {
    peer.script(RequestCode::AUTH_LIST_USER,
                {{ResponseCode::SUCCESS, "",
                  "[{\"username\":\"u1\",\"userStatus\":\"enable\"},"
                  "{\"username\":\"u2\",\"password\":\"p2\"}]"}});
    std::vector<UserInfo> users = admin.listUser(peer.address());

    expectInt(peer.requestCount(RequestCode::AUTH_LIST_USER), 1, "listUser.sent");
    const WireRecord rec = peer.record(RequestCode::AUTH_LIST_USER, 0);
    expectInt(rec.code, RequestCode::AUTH_LIST_USER, "listUser.requestCode");
    expectExt(rec.ext, {}, "listUser.noFilter");
    expect(rec.body.empty(), "listUser.carriesNoBody", rec.body);
    expectInt(static_cast<long long>(users.size()), 2, "listUser.decoded.count");
    if (users.size() == 2) {
        expectStr(users[0].username, "u1", "listUser.decoded[0].username");
        expectStr(users[0].userStatus, "enable", "listUser.decoded[0].userStatus");
        expect(!users[0].hasPassword, "listUser.decoded[0].noPassword");
        expectStr(users[1].username, "u2", "listUser.decoded[1].username");
        expectStr(users[1].password, "p2", "listUser.decoded[1].password");
        expect(!users[1].hasUserStatus, "listUser.decoded[1].noUserStatus");
    }

    // filter present -> exactly one header, and its value verbatim (never "")
    peer.script(RequestCode::AUTH_LIST_USER, {{ResponseCode::SUCCESS, "", "[]"}});
    std::vector<UserInfo> filtered = admin.listUser(peer.address(), "u1");
    const WireRecord rec2 = peer.record(RequestCode::AUTH_LIST_USER, 0);
    expectExt(rec2.ext, {{"filter", "u1"}}, "listUser.withFilter");
    expect(filtered.empty(), "listUser.emptyArrayDecodesEmpty");

    // empty body decodes to the empty list
    peer.script(RequestCode::AUTH_LIST_USER, {{ResponseCode::SUCCESS, "", ""}});
    expect(admin.listUser(peer.address()).empty(), "listUser.emptyBodyDecodesEmpty");
}

// ---------------------------------------------------------------- ACL management (3006..3010)

void testCreateAclConvenience(DefaultMQAdminExt& admin, RecordingListener& peer) {
    peer.script(RequestCode::AUTH_CREATE_ACL, {{ResponseCode::SUCCESS, "", ""}});
    admin.createAcl(peer.address(), "User:alice", {"TopicA", "TopicB"}, {"PUB", "SUB"},
                    {"192.168.0.0/16"}, AclDecision::ALLOW);

    expectInt(peer.requestCount(RequestCode::AUTH_CREATE_ACL), 1, "createAcl.convenience.sent");
    const WireRecord rec = peer.record(RequestCode::AUTH_CREATE_ACL, 0);
    expectInt(rec.code, RequestCode::AUTH_CREATE_ACL, "createAcl.convenience.requestCode");
    expectExt(rec.ext, {{"subject", "User:alice"}}, "createAcl.convenience");
    expectStr(rec.body,
              "{\"subject\":\"User:alice\",\"policies\":[{"
              "\"entries\":["
              "{\"resource\":\"TopicA\",\"actions\":[\"PUB\",\"SUB\"],"
              "\"sourceIps\":[\"192.168.0.0/16\"],\"decision\":\"Allow\"},"
              "{\"resource\":\"TopicB\",\"actions\":[\"PUB\",\"SUB\"],"
              "\"sourceIps\":[\"192.168.0.0/16\"],\"decision\":\"Allow\"}"
              "]}]}",
              "createAcl.convenience.bodyBytes");
    // one policy, no policyType, one entry per resource repeating the shared fields
    expect(rec.body.find("policyType") == std::string::npos,
           "createAcl.convenience.bodyHasNoPolicyTypeKey", rec.body);
    // the brace count is exactly the two entries + one policy + one outer object
    expectInt(static_cast<long long>(std::count(rec.body.begin(), rec.body.end(), '{')), 4,
              "createAcl.convenience.bodyHasOnePolicyTwoEntries");
}

void testCreateAclObject(DefaultMQAdminExt& admin, RecordingListener& peer) {
    peer.script(RequestCode::AUTH_CREATE_ACL, {{ResponseCode::SUCCESS, "", ""}});
    AclInfo acl;
    acl.subject = "Group:team";
    AclPolicy policy;
    policy.policyType = AclPolicyType::CUSTOM;
    policy.hasPolicyType = true;
    AclEntry entry;
    entry.resource = "TopicC";
    entry.actions = {"SUB"};
    entry.hasActions = true;
    entry.decision = AclDecision::DENY;
    entry.hasDecision = true;
    policy.entries.push_back(entry);
    acl.policies.push_back(policy);
    admin.createAcl(peer.address(), acl);

    const WireRecord rec = peer.record(RequestCode::AUTH_CREATE_ACL, 0);
    expectExt(rec.ext, {{"subject", "Group:team"}}, "createAcl.object");
    expectStr(rec.body,
              "{\"subject\":\"Group:team\",\"policies\":[{"
              "\"policyType\":\"Custom\",\"entries\":[{"
              "\"resource\":\"TopicC\",\"actions\":[\"SUB\"],\"decision\":\"Deny\"}]}]}",
              "createAcl.object.bodyBytes");
    expect(rec.body.find("sourceIps") == std::string::npos, "createAcl.object.noSourceIpsKey",
           rec.body);
}

void testUpdateAcl(DefaultMQAdminExt& admin, RecordingListener& peer) {
    peer.script(RequestCode::AUTH_UPDATE_ACL, {{ResponseCode::SUCCESS, "", ""}});
    admin.updateAcl(peer.address(), "User:bob", {"TopicD"}, {"PUB"}, {"10.0.0.1"},
                   AclDecision::ALLOW);

    expectInt(peer.requestCount(RequestCode::AUTH_UPDATE_ACL), 1, "updateAcl.sent");
    const WireRecord rec = peer.record(RequestCode::AUTH_UPDATE_ACL, 0);
    expectInt(rec.code, RequestCode::AUTH_UPDATE_ACL, "updateAcl.requestCode");
    expectExt(rec.ext, {{"subject", "User:bob"}}, "updateAcl.convenience");
    expectStr(rec.body,
              "{\"subject\":\"User:bob\",\"policies\":[{"
              "\"entries\":[{\"resource\":\"TopicD\",\"actions\":[\"PUB\"],"
              "\"sourceIps\":[\"10.0.0.1\"],\"decision\":\"Allow\"}]}]}",
              "updateAcl.convenience.bodyBytes");
    expect(rec.body.find("policyType") == std::string::npos,
           "updateAcl.convenience.bodyHasNoPolicyTypeKey", rec.body);
}

void testDeleteAcl(DefaultMQAdminExt& admin, RecordingListener& peer) {
    peer.script(RequestCode::AUTH_DELETE_ACL, {{ResponseCode::SUCCESS, "", ""}});
    admin.deleteAcl(peer.address(), "User:alice", "TopicA");

    expectInt(peer.requestCount(RequestCode::AUTH_DELETE_ACL), 1, "deleteAcl.sent");
    const WireRecord rec = peer.record(RequestCode::AUTH_DELETE_ACL, 0);
    expectInt(rec.code, RequestCode::AUTH_DELETE_ACL, "deleteAcl.requestCode");
    // exactly two headers: subject + resource, nothing else
    expectExt(rec.ext, {{"subject", "User:alice"}, {"resource", "TopicA"}}, "deleteAcl");
    expect(rec.ext.find("policyType") == rec.ext.end(), "deleteAcl.noPolicyTypeHeader");
    expect(rec.ext.find("subjectFilter") == rec.ext.end(), "deleteAcl.noSubjectFilterHeader");
    expect(rec.ext.find("username") == rec.ext.end(), "deleteAcl.noUsernameHeader");
    expect(rec.body.empty(), "deleteAcl.carriesNoBody", rec.body);
}

void testGetAcl(DefaultMQAdminExt& admin, RecordingListener& peer) {
    peer.script(RequestCode::AUTH_GET_ACL,
                {{ResponseCode::SUCCESS, "",
                  "{\"subject\":\"User:alice\",\"policies\":[{"
                  "\"policyType\":\"Default\",\"entries\":[{"
                  "\"resource\":\"TopicA\",\"actions\":[\"PUB\"],\"sourceIps\":[\"10.0.0.1\"],"
                  "\"decision\":\"Allow\"}]}]}"}});
    AclInfo got = admin.getAcl(peer.address(), "User:alice");

    expectInt(peer.requestCount(RequestCode::AUTH_GET_ACL), 1, "getAcl.sent");
    const WireRecord rec = peer.record(RequestCode::AUTH_GET_ACL, 0);
    expectInt(rec.code, RequestCode::AUTH_GET_ACL, "getAcl.requestCode");
    expectExt(rec.ext, {{"subject", "User:alice"}}, "getAcl");
    expect(rec.body.empty(), "getAcl.carriesNoBody", rec.body);
    expectStr(got.subject, "User:alice", "getAcl.decoded.subject");
    expectInt(static_cast<long long>(got.policies.size()), 1, "getAcl.decoded.policyCount");
    if (!got.policies.empty()) {
        const AclPolicy& p = got.policies[0];
        expect(p.hasPolicyType, "getAcl.decoded.hasPolicyType");
        expectStr(p.policyType, "Default", "getAcl.decoded.policyType");
        expectInt(static_cast<long long>(p.entries.size()), 1, "getAcl.decoded.entryCount");
        if (!p.entries.empty()) {
            const AclEntry& e = p.entries[0];
            expectStr(e.resource, "TopicA", "getAcl.decoded.resource");
            expect(e.hasActions && e.actions == std::vector<std::string>{"PUB"},
                   "getAcl.decoded.actions");
            expect(e.hasSourceIps && e.sourceIps == std::vector<std::string>{"10.0.0.1"},
                   "getAcl.decoded.sourceIps");
            expectStr(e.decision, "Allow", "getAcl.decoded.decision");
            expect(e.hasDecision, "getAcl.decoded.hasDecision");
        }
    }

    // control: a policy without policyType decodes with hasPolicyType false (no padded default)
    peer.script(RequestCode::AUTH_GET_ACL,
                {{ResponseCode::SUCCESS, "",
                  "{\"subject\":\"User:cara\",\"policies\":[{\"entries\":[{\"resource\":\"T\","
                  "\"decision\":\"Deny\"}]}]}"}});
    AclInfo partial = admin.getAcl(peer.address(), "User:cara");
    expect(!partial.policies.empty() && !partial.policies[0].hasPolicyType,
           "getAcl.partial.policyHasNoTypeKey");
    expect(!partial.policies.empty() && !partial.policies[0].entries.empty() &&
               !partial.policies[0].entries[0].hasActions,
           "getAcl.partial.entryHasNoActionsKey");

    // empty body: the untouched default value signals "nothing there"
    peer.script(RequestCode::AUTH_GET_ACL, {{ResponseCode::SUCCESS, "", ""}});
    AclInfo empty = admin.getAcl(peer.address(), "User:ghost");
    expect(empty.subject.empty() && empty.policies.empty(), "getAcl.emptyBodyIsDefault");
}

void testListAcl(DefaultMQAdminExt& admin, RecordingListener& peer) {
    peer.script(RequestCode::AUTH_LIST_ACL,
                {{ResponseCode::SUCCESS, "",
                  "[{\"subject\":\"User:alice\",\"policies\":[]},"
                  "{\"subject\":\"User:bob\",\"policies\":[{\"entries\":[{"
                  "\"resource\":\"TopicA\",\"actions\":[\"SUB\"],\"decision\":\"Deny\"}]}]}]"}});
    std::vector<AclInfo> both =
        admin.listAcl(peer.address(), std::optional<std::string>("User:"),
                      std::optional<std::string>("Topic"));

    expectInt(peer.requestCount(RequestCode::AUTH_LIST_ACL), 1, "listAcl.sent");
    const WireRecord rec = peer.record(RequestCode::AUTH_LIST_ACL, 0);
    expectInt(rec.code, RequestCode::AUTH_LIST_ACL, "listAcl.requestCode");
    expectExt(rec.ext, {{"subjectFilter", "User:"}, {"resourceFilter", "Topic"}}, "listAcl.both");
    expect(rec.body.empty(), "listAcl.carriesNoBody", rec.body);
    expectInt(static_cast<long long>(both.size()), 2, "listAcl.decoded.count");
    if (both.size() == 2) {
        expectStr(both[0].subject, "User:alice", "listAcl.decoded[0].subject");
        expect(both[0].policies.empty(), "listAcl.decoded[0].emptyPolicies");
        expectStr(both[1].subject, "User:bob", "listAcl.decoded[1].subject");
        expectInt(static_cast<long long>(both[1].policies.size()), 1,
                  "listAcl.decoded[1].policyCount");
        if (!both[1].policies.empty()) {
            const AclPolicy& p = both[1].policies[0];
            expect(!p.hasPolicyType, "listAcl.decoded[1].policyHasNoType");
            if (!p.entries.empty()) {
                expect(!p.entries[0].hasSourceIps, "listAcl.decoded[1].entryHasNoSourceIps");
                expectStr(p.entries[0].decision, "Deny", "listAcl.decoded[1].decision");
            }
        }
    }

    // no filters -> no headers at all (an unset filter is never sent as "")
    peer.script(RequestCode::AUTH_LIST_ACL, {{ResponseCode::SUCCESS, "", "[]"}});
    expect(admin.listAcl(peer.address()).empty(), "listAcl.emptyArrayDecodesEmpty");
    const WireRecord rec2 = peer.record(RequestCode::AUTH_LIST_ACL, 0);
    expectExt(rec2.ext, {}, "listAcl.noFilters");

    // empty body decodes to the empty list
    peer.script(RequestCode::AUTH_LIST_ACL, {{ResponseCode::SUCCESS, "", ""}});
    expect(admin.listAcl(peer.address()).empty(), "listAcl.emptyBodyDecodesEmpty");
}

// ---------------------------------------------------------------- result handling

void testNonSuccessCodeThrows(DefaultMQAdminExt& admin, RecordingListener& peer) {
    peer.script(RequestCode::AUTH_GET_USER,
                {{ResponseCode::NO_PERMISSION, "user admin is not permitted", ""}});
    int threw = 0;
    int32_t code = 0;
    std::string msg;
    try {
        admin.getUser(peer.address(), "alice");
    } catch (const MQBrokerException& e) {
        threw = 1;
        code = e.getResponseCode();
        msg = e.getResponseMessage();
    } catch (const std::exception& e) {
        std::printf("  wrong exception type: %s\n", e.what());
    }
    expect(threw == 1, "nonSuccess.getUser.throws");
    expectInt(code, ResponseCode::NO_PERMISSION, "nonSuccess.getUser.responseCode");
    expectStr(msg, "user admin is not permitted", "nonSuccess.getUser.remark");

    // a write operation takes the same rule: the void path must not swallow the failure
    peer.script(RequestCode::AUTH_CREATE_USER, {{ResponseCode::SYSTEM_ERROR, "duplicate user", ""}});
    int wrote = 0;
    try {
        admin.createUser(peer.address(), "dup", "p", UserType::NORMAL);
    } catch (const MQBrokerException& e) {
        wrote = 1;
        expectInt(e.getResponseCode(), ResponseCode::SYSTEM_ERROR, "nonSuccess.createUser.code");
        expectStr(e.getResponseMessage(), "duplicate user", "nonSuccess.createUser.remark");
    } catch (const std::exception& e) {
        std::printf("  wrong exception type: %s\n", e.what());
    }
    expect(wrote == 1, "nonSuccess.createUser.throws");
}

void testEmptyUsernameRejectedLocally(DefaultMQAdminExt& admin, RecordingListener& peer) {
    peer.script(RequestCode::AUTH_DELETE_USER, {{ResponseCode::SUCCESS, "", ""}});
    int threw = 0;
    try {
        admin.deleteUser(peer.address(), "");
    } catch (const MQClientException&) {
        threw = 1;
    }
    expect(threw == 1, "emptyUsername.throwsBeforeSending");
    expectInt(peer.requestCount(RequestCode::AUTH_DELETE_USER), 0,
              "emptyUsername.noRequestOnTheWire");
}

}  // namespace

int main() {
    // Bind the pair first: the decoy sits exactly two ports below the primary, so with the VIP
    // channel on a shifted address would arrive at a listening socket and be counted there.
    std::unique_ptr<RecordingListener> primary;
    std::unique_ptr<RecordingListener> decoy;
    for (int attempt = 0; attempt < 16; ++attempt) {
        primary.reset(new RecordingListener());
        if (!primary->ok() || primary->port() <= 1024) {
            primary.reset();
            continue;
        }
        decoy.reset(new RecordingListener(static_cast<uint16_t>(primary->port() - 2)));
        if (decoy->ok()) break;
        primary.reset();
        decoy.reset();
    }
    if (!primary || !decoy || !decoy->ok()) {
        std::printf("FAIL unable to bind the loopback listener pair\n");
        return 1;
    }

    DefaultMQAdminExt admin("AuthAdminTest");
    admin.setNamesrvAddr(primary->address());
    admin.setTimeoutMillis(5000);
    // left on deliberately: AUTH requests must ignore the VIP rewrite and use the caller's address
    admin.setVipChannelEnabled(true);
    try {
        admin.start();
    } catch (const std::exception& e) {
        std::printf("FAIL admin.start: %s\n", e.what());
        return 1;
    }

    testCreateUserConvenience(admin, *primary);
    testCreateUserObject(admin, *primary);
    testUpdateUser(admin, *primary);
    testDeleteUser(admin, *primary);
    testGetUser(admin, *primary);
    testListUser(admin, *primary);
    testCreateAclConvenience(admin, *primary);
    testCreateAclObject(admin, *primary);
    testUpdateAcl(admin, *primary);
    testDeleteAcl(admin, *primary);
    testGetAcl(admin, *primary);
    testListAcl(admin, *primary);
    testNonSuccessCodeThrows(admin, *primary);
    testEmptyUsernameRejectedLocally(admin, *primary);

    admin.shutdown();
    expectInt(decoy->authTotal(), 0, "addressing.noAuthRequestOnVipPort");

    std::printf("auth_admin: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
