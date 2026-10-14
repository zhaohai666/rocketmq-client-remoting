// Client hook base implementations (only the destructors need a TU definition, avoiding cross-TU ODR problems).
#include "rocketmq/client/hook.h"

namespace rocketmq {

SendMessageHook::~SendMessageHook() = default;
ConsumeMessageHook::~ConsumeMessageHook() = default;
EndTransactionHook::~EndTransactionHook() = default;
CheckForbiddenHook::~CheckForbiddenHook() = default;
FilterMessageHook::~FilterMessageHook() = default;

}  // namespace rocketmq
