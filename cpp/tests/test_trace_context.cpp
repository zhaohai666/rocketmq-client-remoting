// W3C traceparent 透传单测：生成/校验/子 span/注入不覆盖/属性提取。
// 真机（broker TLS + 消息属性跨客户端透传）由 live 脚本验证。
#include <cstdio>
#include <cstdlib>
#include <string>

#include "rocketmq/client/trace_context.h"
#include "rocketmq/common/message.h"

using namespace rocketmq;

static int g_checks = 0;

static void expectEq(const std::string& got, const std::string& want, const char* what) {
    ++g_checks;
    if (got != want) {
        std::printf("FAIL %s: got [%s], want [%s]\n", what, got.c_str(), want.c_str());
        std::exit(1);
    }
}

static void expectTrue(bool cond, const char* what) {
    ++g_checks;
    if (!cond) {
        std::printf("FAIL %s\n", what);
        std::exit(1);
    }
}

static bool isHex(const std::string& s) {
    for (char c : s) {
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
            return false;
        }
    }
    return true;
}

void testGenerate() {
    const std::string tp = generateTraceparent();
    expectEq(tp.substr(0, 3), "00-", "version");
    expectEq(std::to_string(tp.size()), "55", "total length 55");
    expectEq(tp.substr(35, 1), "-", "sep1");
    expectEq(tp.substr(52, 1), "-", "sep2");
    expectEq(tp.substr(53, 2), "01", "sampled flags");
    expectTrue(isHex(tp.substr(3, 32)), "trace-id hex");
    expectTrue(isHex(tp.substr(36, 16)), "parent-id hex");
    expectTrue(isValidTraceparent(tp), "generated is valid");
}

void testValidate() {
    expectTrue(isValidTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
               "canonical valid");
    expectTrue(isValidTraceparent("00-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-00"),
               "uppercase tolerated");
    expectTrue(!isValidTraceparent(""), "empty invalid");
    expectTrue(!isValidTraceparent("00-00000000000000000000000000000000-00f067aa0ba902b7-01"),
               "all-zero trace-id invalid");
    expectTrue(!isValidTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01"),
               "all-zero parent-id invalid");
    expectTrue(!isValidTraceparent("00-4bf92f-00f067aa0ba902b7-01"), "short trace-id invalid");
    expectTrue(!isValidTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-zz"),
               "bad flags invalid");
    expectTrue(!isValidTraceparent("ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
               "unsupported version invalid");
}

void testChild() {
    const std::string parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    const std::string child = childTraceparent(parent);
    expectTrue(isValidTraceparent(child), "child valid");
    expectEq(child.substr(3, 32), parent.substr(3, 32), "same trace-id");
    expectTrue(child.substr(36, 16) != parent.substr(36, 16), "different parent-id");
    expectTrue(childTraceparent("garbage").empty(), "invalid parent -> empty");
}

void testInjectExtract() {
    Message msg;
    msg.putProperty("k", "v");

    // 无既有值 -> 注入根上下文
    const std::string injected = injectTraceContext(&msg);
    expectTrue(isValidTraceparent(injected), "injected valid");
    expectEq(extractTraceparent(msg), injected, "extract == injected");

    // 已有值 -> 不覆盖（上游传播优先）
    Message msg2;
    msg2.putProperty(kTraceContextProperty, "00-11111111111111111111111111111111-2222222222222222-01");
    const std::string kept = injectTraceContext(&msg2);
    expectEq(kept, "00-11111111111111111111111111111111-2222222222222222-01", "existing not overwritten");

    // 未注入的消息 extract 返回空串
    Message msg3;
    expectEq(extractTraceparent(msg3), "", "extract empty when absent");
}

int main() {
    testGenerate();
    testValidate();
    testChild();
    testInjectExtract();
    std::printf("PASS trace_context: %d checks\n", g_checks);
    return 0;
}
