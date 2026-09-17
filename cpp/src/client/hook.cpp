// 客户端钩子基类实现（仅析构函数需要 TU 定义，避免跨 TU 的 ODR 问题）。
#include "rocketmq/client/hook.h"

namespace rocketmq {

SendMessageHook::~SendMessageHook() = default;
ConsumeMessageHook::~ConsumeMessageHook() = default;
EndTransactionHook::~EndTransactionHook() = default;

}  // namespace rocketmq
