// broker 主动请求 NOTIFY_CONSUMER_IDS_CHANGED(40) 离线单测 —— 不需要集群。
//
// 为什么单独一个文件：这组请求是 broker 沿**已建立的长连接反向**打进客户端的，
// 真机用例注入不了（live 工具只能正向往 broker 发请求），所以能离线锁死的部分
// 必须先锁死，否则「处理器根本没注册」这种错只会在生产上表现为「成员变了却要
// 等 20s 定时重平衡」，安静得几乎不可见。
//
// 对齐 Java：
//   - `MQClientAPIImpl` 构造函数里把 40 注册在**实例级**的 clientRemotingProcessor
//     （不是每个消费者各注册一份 —— 一个 code 只有一个处理器，各自注册会互相覆盖）
//   - `ClientRemotingProcessor#notifyConsumerIdsChanged` → `rebalanceImmediately()`，
//     返回 null ⇒ 不回包
//
// 覆盖：
//   - 计数 + 整组唤醒（注册两个消费者，两个都被点名）
//   - unregisterRebalanceWakeup 后不再被叫醒，但通知本身仍被处理
//   - 缺 consumerGroup 不抛、仍然计数
//   - 实例 shutdown 清掉唤醒表（对应「回调里捕获 this，实例不能带着悬垂回调存活」）
#include <cstdio>
#include <string>

#include "rocketmq/client/mq_client.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/remoting_command.h"

using namespace rocketmq;

namespace {

int fails = 0;
int checks = 0;

void expect(bool ok, const std::string& name, const std::string& detail = "") {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("FAIL %s %s\n", name.c_str(), detail.c_str());
    }
}

/// broker 发的是 oneway 通知，extFields 里只带 consumerGroup。
RemotingCommand notifyCmd(const std::string& group) {
    RemotingCommand cmd = RemotingCommand::createRequestCommand(
        RequestCode::NOTIFY_CONSUMER_IDS_CHANGED);
    if (!group.empty()) cmd.extFields["consumerGroup"] = group;
    return cmd;
}

void testFanOut() {
    MQClientInstance instance("cpp-unit-40@fanout", {"127.0.0.1:9876"});
    int wakeA = 0;
    int wakeB = 0;
    instance.registerRebalanceWakeup("GID_A", [&wakeA] { ++wakeA; });
    instance.registerRebalanceWakeup("GID_B", [&wakeB] { ++wakeB; });

    instance.processNotifyConsumerIdsChanged(notifyCmd("GID_A"), "127.0.0.1:10911");
    expect(instance.consumerIdsChangedCount() == 1, "notify.counted");
    expect(wakeA == 1, "notify.wakesFirst", "wakeA=" + std::to_string(wakeA));
    expect(wakeB == 1, "notify.wakesEveryConsumer", "wakeB=" + std::to_string(wakeB));

    // group 只用于日志：Java 不读它来决定叫醒谁，所以两次通知叫醒的是同一批人。
    instance.processNotifyConsumerIdsChanged(notifyCmd(""), "127.0.0.1:10911");
    expect(instance.consumerIdsChangedCount() == 2, "notify.blankGroupStillCounts");
    expect(wakeA == 2 && wakeB == 2, "notify.blankGroupWakesAll",
           "wakeA=" + std::to_string(wakeA) + " wakeB=" + std::to_string(wakeB));

    // 注销的那一组不再被叫醒，但通知本身照样处理（计数继续走）。
    instance.unregisterRebalanceWakeup("GID_B");
    instance.processNotifyConsumerIdsChanged(notifyCmd("GID_A"), "127.0.0.1:10911");
    expect(instance.consumerIdsChangedCount() == 3, "notify.countsAfterUnregister");
    expect(wakeA == 3, "notify.stillWakesRegistered", "wakeA=" + std::to_string(wakeA));
    expect(wakeB == 2, "notify.unregisteredNotWoken", "wakeB=" + std::to_string(wakeB));

    // 空表不炸：一个消费者都没注册时（只有生产者、或消费者都已退出）通知仍要吃完。
    instance.unregisterRebalanceWakeup("GID_A");
    instance.rebalanceImmediately();
    expect(instance.consumerIdsChangedCount() == 3, "notify.emptyTableIsNoop");
}

void testShutdownClearsWakeups() {
    MQClientInstance instance("cpp-unit-40@shutdown", {"127.0.0.1:9876"});
    int woken = 0;
    instance.registerRebalanceWakeup("GID_A", [&woken] { ++woken; });
    instance.shutdown();
    instance.rebalanceImmediately();
    expect(woken == 0, "shutdown.clearsWakeups", "woken=" + std::to_string(woken));
    // shutdown 之后再收到通知（读线程可能还在收尾）也不能抛。
    instance.processNotifyConsumerIdsChanged(notifyCmd("GID_A"), "127.0.0.1:10911");
    expect(instance.consumerIdsChangedCount() == 1, "notify.afterShutdownStillCounts");
}

}  // namespace

int main() {
    testFanOut();
    testShutdownClearsWakeups();
    std::printf("broker requests: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
