// 消息轨迹实现（对应 org.apache.rocketmq.client.trace.TraceDataEncoder 等）。
//
// 字段顺序与 Java 官方实现逐字节一致；回归守卫见 cpp/tests/test_trace.cpp。
#include "rocketmq/client/trace.h"

#include <cstdlib>
#include <sstream>
#include <stdexcept>

#include "rocketmq/common/logging.h"
#include "rocketmq/common/message_const.h"

namespace rocketmq {

namespace {

// 按 sep 切分（与 Java 的 result.split(sep) 等价，但显式丢弃末尾空串）。
// 例："a\x02" -> ["a"]；"a\x02b\x02" -> ["a","b"]；"\x02" -> []。
std::string safeAt(const std::vector<std::string>& parts, size_t idx) {
    return idx < parts.size() ? parts[idx] : std::string();
}

// 按空格切分 keys（Java 用 KEY_SEPARATOR = " " 的 split，且丢弃空段）。
std::vector<std::string> splitBySpace(const std::string& s) {
    std::vector<std::string> out;
    std::string cur;
    for (char c : s) {
        if (c == ' ') {
            if (!cur.empty()) {
                out.push_back(cur);
                cur.clear();
            }
        } else {
            cur.push_back(c);
        }
    }
    if (!cur.empty()) out.push_back(cur);
    return out;
}

// 把若干字段用 CONTENT_SPLITOR 拼起来，末尾补 FIELD_SPLITOR（对应 Java SOH.join + STX）。
std::string joinRecord(const std::vector<std::string>& fields) {
    std::string out;
    for (size_t i = 0; i < fields.size(); ++i) {
        if (i) out += TraceConstants::CONTENT_SPLITOR;
        out += fields[i];
    }
    out += TraceConstants::FIELD_SPLITOR;
    return out;
}

bool toBool(const std::string& s) { return s == "true"; }

int32_t toInt32(const std::string& s, int32_t def) {
    if (s.empty()) return def;
    try {
        return static_cast<int32_t>(std::stoi(s));
    } catch (...) {
        return def;
    }
}

int64_t toInt64(const std::string& s, int64_t def) {
    if (s.empty()) return def;
    try {
        return std::stoll(s);
    } catch (...) {
        return def;
    }
}

bool parseSingleRecord(const std::vector<std::string>& line, TraceContext& ctx) {
    if (line.empty()) return false;
    const std::string& kind = line[0];
    if (kind == "Pub") {
        // 最小 12 段（Python 参考实现实际访问到 line[11]）；老版本无 offsetMsgId 时 13 段。
        if (line.size() < 12) return false;
        ctx.traceType = TraceType::PUB;
        ctx.timeStamp = toInt64(safeAt(line, 1), 0);
        ctx.regionId = safeAt(line, 2);
        ctx.groupName = safeAt(line, 3);
        TraceBean bean;
        bean.topic = safeAt(line, 4);
        bean.msgId = safeAt(line, 5);
        bean.tags = safeAt(line, 6);
        bean.keys = safeAt(line, 7);
        bean.storeHost = safeAt(line, 8);
        bean.bodyLength = toInt32(safeAt(line, 9), 0);
        ctx.costTime = toInt32(safeAt(line, 10), 0);
        bean.msgType = toInt32(safeAt(line, 11), 0);
        if (line.size() == 13) {                           // 老版本：无 offsetMsgId
            ctx.isSuccess = toBool(safeAt(line, 12));
        } else if (line.size() == 14) {
            bean.offsetMsgId = safeAt(line, 12);
            ctx.isSuccess = toBool(safeAt(line, 13));
        }
        if (line.size() >= 15) {                           // 兼容更老版本（带 clientHost）
            bean.offsetMsgId = safeAt(line, 12);
            ctx.isSuccess = toBool(safeAt(line, 13));
            bean.clientHost = safeAt(line, 14);
        }
        ctx.traceBeans = {bean};
        return true;
    }
    if (kind == "SubBefore") {
        // ⚠ 最小 7 段，**不是 8**。被消费的消息没有 keys 时，末段为空，javaSplit 会连末尾的
        //    CONTENT_SPLITOR 一起丢弃（Java String.split 的语义）→ 只剩 type..retryTimes 共 7 段。
        //    这里若要求 8 段就会把这条合法记录整条丢掉（Java 原生实现是 line[7] 直接 AIOOBE）。
        //    回归守卫：test_trace.cpp 的 "SubBefore 空 keys" 用例 + 真机 S17。
        if (line.size() < 7) return false;
        ctx.traceType = TraceType::SUB_BEFORE;
        ctx.timeStamp = toInt64(safeAt(line, 1), 0);
        ctx.regionId = safeAt(line, 2);
        ctx.groupName = safeAt(line, 3);
        ctx.requestId = safeAt(line, 4);
        TraceBean bean;
        bean.msgId = safeAt(line, 5);
        bean.retryTimes = toInt32(safeAt(line, 6), 0);
        bean.keys = safeAt(line, 7);
        ctx.traceBeans = {bean};
        return true;
    }
    if (kind == "SubAfter") {
        // 最小 6 段（keys 在段 5）；Python 参考实现访问到 line[5]，与之对齐。
        if (line.size() < 6) return false;
        ctx.traceType = TraceType::SUB_AFTER;
        ctx.requestId = safeAt(line, 1);
        TraceBean bean;
        bean.msgId = safeAt(line, 2);
        bean.keys = safeAt(line, 5);
        ctx.costTime = toInt32(safeAt(line, 3), 0);
        ctx.isSuccess = toBool(safeAt(line, 4));
        if (line.size() >= 7) {                            // 兼容老版本：contextCode 段
            ctx.contextCode = toInt32(safeAt(line, 6), 0);
        }
        if (line.size() >= 9) {                            // 兼容更老版本：timeStamp + groupName
            ctx.timeStamp = toInt64(safeAt(line, 7), UtilAll::currentTimeMillis());
            ctx.groupName = safeAt(line, 8);
        }
        ctx.traceBeans = {bean};
        return true;
    }
    if (kind == "EndTransaction") {
        if (line.size() < 13) return false;
        ctx.traceType = TraceType::END_TRANSACTION;
        ctx.timeStamp = toInt64(safeAt(line, 1), 0);
        ctx.regionId = safeAt(line, 2);
        ctx.groupName = safeAt(line, 3);
        TraceBean bean;
        bean.topic = safeAt(line, 4);
        bean.msgId = safeAt(line, 5);
        bean.tags = safeAt(line, 6);
        bean.keys = safeAt(line, 7);
        bean.storeHost = safeAt(line, 8);
        bean.msgType = toInt32(safeAt(line, 9), 0);
        bean.transactionId = safeAt(line, 10);
        bean.transactionState = safeAt(line, 11);
        bean.fromTransactionCheck = toBool(safeAt(line, 12));
        ctx.traceBeans = {bean};
        return true;
    }
    if (kind == "Recall") {
        if (line.size() < 7) return false;
        ctx.traceType = TraceType::RECALL;
        ctx.timeStamp = toInt64(safeAt(line, 1), 0);
        ctx.regionId = safeAt(line, 2);
        ctx.groupName = safeAt(line, 3);
        TraceBean bean;
        bean.topic = safeAt(line, 4);
        bean.msgId = safeAt(line, 5);
        ctx.isSuccess = toBool(safeAt(line, 6));
        ctx.traceBeans = {bean};
        return true;
    }
    return false;  // 未知类型
}

}  // namespace

// ---------------------------------------------------------------- 本机地址
std::string localAddressForTrace() {
    std::string ip = UtilAll::localIp();
    if (UtilAll::isIpv4(ip)) return ip;
    // IPv6：与 Python/Java 一致，走 inet_pton 后的十六进制拼法。此处简单回退到原串；
    // 绝大多数联调环境是 IPv4，IPv6 仅影响 storeHost/clientHost 默认展示值。
    if (!ip.empty()) return ip;
    return "127.0.0.1";
}

// ---------------------------------------------------------------- javaSplit
std::vector<std::string> TraceDataEncoder::javaSplit(const std::string& value,
                                                     const std::string& sep) {
    std::vector<std::string> parts;
    if (sep.empty()) {
        parts.push_back(value);
        return parts;
    }
    size_t start = 0;
    while (start <= value.size()) {
        size_t pos = value.find(sep, start);
        if (pos == std::string::npos) {
            parts.push_back(value.substr(start));
            break;
        }
        parts.push_back(value.substr(start, pos - start));
        start = pos + sep.size();
    }
    // 丢弃末尾空串（Java String.split 的语义）
    while (!parts.empty() && parts.back().empty()) {
        parts.pop_back();
    }
    return parts;
}

// ---------------------------------------------------------------- encoder
std::optional<TraceTransferBean> TraceDataEncoder::encoderFromContextBean(const TraceContext* ctx) {
    if (ctx == nullptr) return std::nullopt;
    const std::string SOH = TraceConstants::CONTENT_SPLITOR;
    const std::string STX = TraceConstants::FIELD_SPLITOR;
    TraceTransferBean tb;
    if (!ctx->traceType.has_value()) return std::nullopt;
    TraceType t = *ctx->traceType;

    if (t == TraceType::PUB) {
        if (ctx->traceBeans.empty()) return std::nullopt;
        const TraceBean& bean = ctx->traceBeans[0];
        std::vector<std::string> sb;
        sb.push_back(traceTypeName(t));
        sb.push_back(std::to_string(ctx->timeStamp));
        sb.push_back(ctx->regionId);
        sb.push_back(ctx->groupName);
        sb.push_back(bean.topic);
        sb.push_back(bean.msgId);
        sb.push_back(bean.tags);
        sb.push_back(bean.keys);
        sb.push_back(bean.storeHost);
        sb.push_back(std::to_string(bean.bodyLength));
        sb.push_back(std::to_string(ctx->costTime));
        sb.push_back(std::to_string(bean.msgType));
        sb.push_back(bean.offsetMsgId);
        sb.push_back(ctx->isSuccess ? "true" : "false");
        tb.transData = joinRecord(sb);
    } else if (t == TraceType::SUB_BEFORE) {
        for (const TraceBean& bean : ctx->traceBeans) {
            std::vector<std::string> sb;
            sb.push_back(traceTypeName(t));
            sb.push_back(std::to_string(ctx->timeStamp));
            sb.push_back(ctx->regionId);
            sb.push_back(ctx->groupName);
            sb.push_back(ctx->requestId);
            sb.push_back(bean.msgId);
            sb.push_back(std::to_string(bean.retryTimes));
            sb.push_back(bean.keys);
            tb.transData += joinRecord(sb);
        }
    } else if (t == TraceType::SUB_AFTER) {
        for (const TraceBean& bean : ctx->traceBeans) {
            std::vector<std::string> sb;
            sb.push_back(traceTypeName(t));
            sb.push_back(ctx->requestId);
            sb.push_back(bean.msgId);
            sb.push_back(std::to_string(ctx->costTime));
            sb.push_back(ctx->isSuccess ? "true" : "false");
            sb.push_back(bean.keys);
            sb.push_back(std::to_string(ctx->contextCode));
            // 非 CLOUD 才追加 timestamp + groupName（对齐 Java AccessChannel 判定）
            if (ctx->accessChannelOrLocal() != AccessChannel::CLOUD) {
                sb.push_back(std::to_string(ctx->timeStamp));
                sb.push_back(ctx->groupName);
            }
            tb.transData += joinRecord(sb);
        }
    } else if (t == TraceType::END_TRANSACTION) {
        if (ctx->traceBeans.empty()) return std::nullopt;
        const TraceBean& bean = ctx->traceBeans[0];
        std::vector<std::string> sb;
        sb.push_back(traceTypeName(t));
        sb.push_back(std::to_string(ctx->timeStamp));
        sb.push_back(ctx->regionId);
        sb.push_back(ctx->groupName);
        sb.push_back(bean.topic);
        sb.push_back(bean.msgId);
        sb.push_back(bean.tags);
        sb.push_back(bean.keys);
        sb.push_back(bean.storeHost);
        sb.push_back(std::to_string(bean.msgType));
        sb.push_back(bean.transactionId);
        sb.push_back(bean.transactionState);
        sb.push_back(bean.fromTransactionCheck ? "true" : "false");
        tb.transData = joinRecord(sb);
    } else if (t == TraceType::RECALL) {
        if (ctx->traceBeans.empty()) return std::nullopt;
        const TraceBean& bean = ctx->traceBeans[0];
        std::vector<std::string> sb;
        sb.push_back(traceTypeName(t));
        sb.push_back(std::to_string(ctx->timeStamp));
        sb.push_back(ctx->regionId);
        sb.push_back(ctx->groupName);
        sb.push_back(bean.topic);
        sb.push_back(bean.msgId);
        sb.push_back(ctx->isSuccess ? "true" : "false");
        tb.transData = joinRecord(sb);
    }

    // 收集 keys：msgId + 按空格拆开的业务 keys（Java split(KEY_SEPARATOR)）
    for (const TraceBean& bean : ctx->traceBeans) {
        if (!bean.msgId.empty()) tb.transKey.insert(bean.msgId);
        for (const std::string& k : splitBySpace(bean.keys)) {
            if (!k.empty()) tb.transKey.insert(k);
        }
    }
    return tb;
}

// ---------------------------------------------------------------- decoder
std::vector<TraceContext> TraceDataEncoder::decoderFromTraceDataString(
    const std::string& traceData) {
    std::vector<TraceContext> res;
    if (traceData.empty()) return res;
    // 先按记录分隔符切分（丢弃末尾空串），再按字段分隔符切分。
    for (const std::string& context : javaSplit(traceData, TraceConstants::FIELD_SPLITOR)) {
        if (context.empty()) continue;
        std::vector<std::string> line = javaSplit(context, TraceConstants::CONTENT_SPLITOR);
        if (line.empty()) continue;
        TraceContext ctx;
        try {
            if (!parseSingleRecord(line, ctx)) {
                logger_warn("trace decode: skip unknown/incomplete record: " + context);
                continue;
            }
        } catch (const std::exception& e) {
            logger_warn(std::string("trace decode: skip bad record: ") + e.what());
            continue;
        }
        res.push_back(std::move(ctx));
    }
    return res;
}

}  // namespace rocketmq
