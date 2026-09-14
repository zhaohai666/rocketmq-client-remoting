# -*- coding: utf-8 -*-
"""跨语言互操作验证：C++ 协议层 <-> Python 参考客户端 双向编解码交叉校验。

用法：python interop_check.py <path-to-rmq_interop>

验证点：
  1) C++ 编码的 JSON / ROCKETMQ 两种帧，Python 能正确解码；
  2) Python 编码的两种帧，C++ 能正确解码；
  3) 两种语言对同一"规范命令"解码出的字段完全一致；
  4) 路由(TopicRouteData) / 心跳(HeartbeatData) 两个结构体：
     - C++ 产出的 JSON 必须带 **Java 字段名**（brokerDatas/readQueueNums/clientID/...）；
     - Python 侧按 Java 形状构造的 body，C++ 解码后语义等价（用 json 规范化比对，
       以消除 Java HashSet 与 C++ std::set 的集合顺序差异）。
"""
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PY_ROOT = os.path.abspath(os.path.join(HERE, "..", "..", "python"))
sys.path.insert(0, PY_ROOT)

from rocketmq.remoting.protocol.remoting_command import RemotingCommand  # noqa: E402
from rocketmq.remoting.protocol.codes import SerializeType  # noqa: E402

TOOL = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "..", "build", "examples", "rmq_interop")

_pass = 0
_fail = 0
_warn = 0


def check(name, ok, detail=""):
    global _pass, _fail
    if ok:
        _pass += 1
    else:
        _fail += 1
        print("[FAIL] %s %s" % (name, detail))


def warn(msg):
    global _warn
    _warn += 1
    print("[WARN] %s" % msg)


def _json_default(o):
    """Python 侧 to_dict() 会把 tagsSet/codeSet 留成 set —— 顺带说明该路径确实未经测试。"""
    if isinstance(o, (set, frozenset)):
        return sorted(o)
    raise TypeError("not JSON serializable: %r" % (type(o).__name__,))


def run_tool(*args):
    return subprocess.check_output([TOOL] + list(args)).decode("utf-8").strip()


def canon(o):
    """规范化：字典按键排序、列表按内容排序，消除集合顺序差异。"""
    if isinstance(o, dict):
        return {k: canon(v) for k, v in sorted(o.items())}
    if isinstance(o, list):
        return sorted((canon(v) for v in o), key=lambda x: json.dumps(x, sort_keys=True))
    return o


def python_canonical(serialize_type):
    cmd = RemotingCommand(code=10, opaque=7)
    cmd.language = 1  # CPP
    cmd.remark = "hello"
    cmd.add_ext_field("topic", "T_TEST")
    cmd.add_ext_field("queueId", "2")
    cmd.body = b"payload"
    cmd.serialize_type_current_rpc = serialize_type
    return cmd


EXPECT = {
    "code": 10, "language": 1, "opaque": 7, "version": 0,
    "remark": "hello", "body": b"payload",
    "ext": {"topic": "T_TEST", "queueId": "2"},
}


def verify_python_decoded(tag, dec):
    check("%s code" % tag, dec.code == EXPECT["code"], "got %s" % dec.code)
    check("%s language" % tag, dec.language == EXPECT["language"], "got %s" % dec.language)
    check("%s opaque" % tag, dec.opaque == EXPECT["opaque"], "got %s" % dec.opaque)
    check("%s remark" % tag, dec.remark == EXPECT["remark"], "got %r" % dec.remark)
    body = dec.body if dec.body is not None else b""
    check("%s body" % tag, body == EXPECT["body"], "got %r" % body)
    exts = dec.ext_fields or {}
    check("%s ext" % tag, exts.get("topic") == "T_TEST" and exts.get("queueId") == "2",
          "got %s" % exts)


def verify_cpp_decoded(tag, line):
    fields = {}
    exts = {}
    for part in line.strip().split(";"):
        if part.startswith("ext."):
            k, _, v = part[len("ext."):].partition("=")
            exts[k] = v
        else:
            k, _, v = part.partition("=")
            fields[k] = v
    check("%s code" % tag, fields.get("code") == "10", line)
    check("%s language" % tag, fields.get("language") == "1", line)
    check("%s opaque" % tag, fields.get("opaque") == "7", line)
    check("%s remark" % tag, fields.get("remark") == "hello", line)
    check("%s body" % tag, fields.get("body") == "payload", line)
    check("%s ext" % tag, exts.get("topic") == "T_TEST" and exts.get("queueId") == "2", line)


# ---------------------------------------------------------------- 路由结构体
def test_route():
    # ---- C++ -> Python：必须使用 Java 字段名 ----
    cpp_route = json.loads(run_tool("--emit-route"))
    check("route cpp has brokerDatas", isinstance(cpp_route.get("brokerDatas"), list), cpp_route)
    check("route cpp has queueDatas", isinstance(cpp_route.get("queueDatas"), list), cpp_route)
    check("route cpp has filterServerTable",
          isinstance(cpp_route.get("filterServerTable"), dict), cpp_route)
    check("route cpp has orderTopicConf", "orderTopicConf" in cpp_route, cpp_route)
    check("route cpp no snake_case (broker_datas)",
          "broker_datas" not in cpp_route and "queue_datas" not in cpp_route, cpp_route)

    qd = (cpp_route.get("queueDatas") or [{}])[0]
    for k in ("brokerName", "readQueueNums", "writeQueueNums", "perm", "topicSysFlag"):
        check("route queue has Java key %s" % k, k in qd, qd)
    bd = (cpp_route.get("brokerDatas") or [{}])[0]
    for k in ("cluster", "brokerName", "brokerAddrs", "zoneName", "enableActingMaster"):
        check("route broker has Java key %s" % k, k in bd, bd)

    # ---- Python -> C++：按 Java 形状构造，验证语义等价 ----
    sent = {
        "brokerDatas": [
            {"brokerAddrs": {"0": "127.0.0.1:10911", "1": "127.0.0.1:10912"},
             "brokerName": "broker-a", "cluster": "DefaultCluster",
             "enableActingMaster": False, "zoneName": "zone-a"},
            {"brokerAddrs": {"0": "127.0.0.1:10913"}, "brokerName": "broker-b",
             "cluster": "DefaultCluster", "enableActingMaster": False, "zoneName": ""},
        ],
        "filterServerTable": {},
        "orderTopicConf": "",
        "queueDatas": [
            {"brokerName": "broker-a", "perm": 6, "readQueueNums": 4,
             "topicSysFlag": 0, "writeQueueNums": 4},
            {"brokerName": "broker-b", "perm": 4, "readQueueNums": 2,
             "topicSysFlag": 0, "writeQueueNums": 2},
        ],
    }
    body = json.dumps(sent).encode("utf-8")
    got = json.loads(run_tool("--decode-route", body.hex()))
    check("route py->cpp round-trip semantic equal", canon(got) == canon(sent),
          "got %s" % json.dumps(canon(got), sort_keys=True))

    # Java fastjson 裸数字键形式也要能解
    bare = ('{"brokerDatas":[{"brokerAddrs":{0:"127.0.0.1:10911"},"brokerName":"b",'
            '"cluster":"c","enableActingMaster":false,"zoneName":""}],'
            '"filterServerTable":{},"orderTopicConf":"",'
            '"queueDatas":[{"brokerName":"b","perm":6,"readQueueNums":1,'
            '"topicSysFlag":0,"writeQueueNums":1}]}')
    got_bare = json.loads(run_tool("--decode-route", bare.encode("utf-8").hex()))
    check("route decodes fastjson bare-numeric-key form",
          got_bare["brokerDatas"][0]["brokerAddrs"].get("0") == "127.0.0.1:10911", got_bare)


# ---------------------------------------------------------------- 心跳结构体
def test_heartbeat():
    # ---- C++ -> Python：必须使用 Java 字段名 ----
    cpp_hb = json.loads(run_tool("--emit-heartbeat"))
    check("hb cpp has clientID", cpp_hb.get("clientID") == "10.0.0.1@12345", cpp_hb)
    check("hb cpp has producerDataSet", isinstance(cpp_hb.get("producerDataSet"), list), cpp_hb)
    check("hb cpp has consumerDataSet", isinstance(cpp_hb.get("consumerDataSet"), list), cpp_hb)
    check("hb cpp has heartbeatFingerprint", "heartbeatFingerprint" in cpp_hb, cpp_hb)
    check("hb cpp uses withoutSub (Java fastjson name)", "withoutSub" in cpp_hb, cpp_hb)
    check("hb cpp no client_id snake_case", "client_id" not in cpp_hb, cpp_hb)

    cd = (cpp_hb.get("consumerDataSet") or [{}])[0]
    for k in ("groupName", "consumeType", "messageModel", "consumeFromWhere",
              "subscriptionDataSet", "unitMode"):
        check("hb consumer has Java key %s" % k, k in cd, cd)
    check("hb consumer has NO consumeTimestamp (absent in this Java version)",
          "consumeTimestamp" not in cd, cd)
    check("hb consumer has NO maxReconsumeTimes (absent in this Java version)",
          "maxReconsumeTimes" not in cd, cd)

    sd = (cd.get("subscriptionDataSet") or [{}])[0]
    for k in ("classFilterMode", "codeSet", "expressionType", "subString",
              "subVersion", "tagsSet", "topic"):
        check("hb subscription has Java key %s" % k, k in sd, sd)
    check("hb subscription has NO filterClassSource (@JSONField serialize=false)",
          "filterClassSource" not in sd, sd)
    check("hb subscription has NO snake_case (sub_string)",
          "sub_string" not in sd and "tags_set" not in sd, sd)

    # ---- Python -> C++：Java 形状的 body 必须完整还原 ----
    sent = {
        "clientID": "10.0.0.1@12345",
        "consumerDataSet": [{
            "consumeFromWhere": "CONSUME_FROM_LAST_OFFSET",
            "consumeType": "CONSUME_PASSIVELY",
            "groupName": "cg_probe",
            "messageModel": "CLUSTERING",
            "subscriptionDataSet": [{
                "classFilterMode": False, "codeSet": [11, 22],
                "expressionType": "TAG", "subString": "TagA||TagB",
                "subVersion": 1700000000000, "tagsSet": ["TagA", "TagB"],
                "topic": "TopicProbe",
            }],
            "unitMode": False,
        }],
        "heartbeatFingerprint": 0,
        "producerDataSet": [{"groupName": "pg_probe"}],
        "withoutSub": False,
    }
    body = json.dumps(sent).encode("utf-8")
    got = json.loads(run_tool("--decode-heartbeat", body.hex()))
    check("hb py->cpp round-trip semantic equal", canon(got) == canon(sent),
          "got %s" % json.dumps(canon(got), sort_keys=True))

    # 兼容 Java 字段名 isWithoutSub
    alias = json.dumps({"clientID": "c", "isWithoutSub": True, "heartbeatFingerprint": 7,
                        "consumerDataSet": [], "producerDataSet": []}).encode("utf-8")
    got_alias = json.loads(run_tool("--decode-heartbeat", alias.hex()))
    check("hb accepts isWithoutSub alias", got_alias.get("withoutSub") is True, got_alias)

    # ---- 已知差异（Python 参考客户端侧，非 C++ 问题），显式测出来避免"看着绿其实字段丢了" ----
    from rocketmq.common.subscription_data import SubscriptionData as PySub
    from rocketmq.common.subscription_data import FilterAPI as PyFilterAPI
    from rocketmq.remoting.protocol.heartbeat import ConsumerData as PyConsumerData

    # 差异 1：SubscriptionData 定义了 __eq__ 却没配 __hash__ -> 不可哈希
    try:
        set().add(PySub("T", "*"))
        check("python SubscriptionData is hashable", True)
    except TypeError as exc:
        warn("Python 参考客户端的 SubscriptionData 定义了 __eq__ 但未定义 __hash__，"
             "因此不可哈希：ConsumerData.subscription_data_set.add(...) 会抛 "
             "TypeError(%s)。同时 send_heartbeat() 在客户端里从未被调用，"
             "所以这条心跳/订阅代码路径是**未经测试的死代码**——"
             "Python 侧若要支持心跳注册，需补 __hash__（C++ 侧无此问题）。" % exc)

    # 差异 2：to_dict 用 __dict__ -> snake_case 键，broker(fastjson2) 认不出来
    py_cd = PyConsumerData("cg_x")
    # 用 list 顶替 set 以绕过上面的不可哈希问题，仅用于观察序列化字段名
    py_cd.subscription_data_set = [PyFilterAPI.build_subscription_data("TopicX", "TagA")]
    py_dict = py_cd.to_dict()
    sent_keys = set((py_dict.get("subscriptionDataSet") or [{}])[0].keys())
    snake = sorted(k for k in sent_keys if "_" in k)
    if snake:
        warn("Python 参考客户端 SubscriptionData.to_dict() 输出 snake_case 键 %s；"
             "broker 用 fastjson2 按 Java 属性名反序列化，会丢掉这些字段"
             "（C++ 侧已使用正确的 Java 字段名，可被 broker 正常解析）。" % snake)
        # 包成 HeartbeatData 的形状再喂给 C++（to_dict 产出的是 ConsumerData 一层）
        hb_dict = {
            "clientID": "cid",
            "consumerDataSet": [py_dict],
            "heartbeatFingerprint": 0,
            "producerDataSet": [],
            "withoutSub": False,
        }
        got_py = json.loads(run_tool("--decode-heartbeat",
                                     json.dumps(hb_dict, default=_json_default)
                                     .encode("utf-8").hex()))
        pcd = (got_py.get("consumerDataSet") or [{}])[0]
        psd = (pcd.get("subscriptionDataSet") or [{}])[0]
        check("hb python-client body: topic survives", psd.get("topic") == "TopicX", psd)
        check("hb python-client body: snake_case fields are NOT visible to C++",
              not psd.get("subString") and not psd.get("tagsSet"), psd)
    else:
        check("python heartbeat uses Java field names", False, sent_keys)


def main():
    # ---------- 1) C++ -> Python ----------
    out = run_tool("--emit")
    emitted = {}
    for ln in out.strip().splitlines():
        parts = ln.split()
        if len(parts) == 2:
            emitted[parts[0]] = parts[1]
    check("cpp emit JSON present", "JSON" in emitted)
    check("cpp emit ROCKETMQ present", "ROCKETMQ" in emitted)

    if "JSON" in emitted:
        dec = RemotingCommand.decode(bytes.fromhex(emitted["JSON"]))
        verify_python_decoded("py<-cpp JSON", dec)
    if "ROCKETMQ" in emitted:
        dec = RemotingCommand.decode(bytes.fromhex(emitted["ROCKETMQ"]))
        verify_python_decoded("py<-cpp ROCKETMQ", dec)

    # ---------- 2) Python -> C++ ----------
    for label, st in (("JSON", SerializeType.JSON), ("ROCKETMQ", SerializeType.ROCKETMQ)):
        wire = python_canonical(st).encode()
        hexstr = wire.hex()
        line = run_tool("--decode", hexstr)
        verify_cpp_decoded("cpp<-py %s" % label, line)

    # ---------- 3) 结构体 ----------
    test_route()
    test_heartbeat()

    print("\n===== interop summary =====")
    print("  PASS=%d FAIL=%d WARN=%d" % (_pass, _fail, _warn))
    if _fail == 0:
        print("  result: C++ <-> Python 双向编解码一致")
        return 0
    print("  result: %d 项失败" % _fail)
    return 1


if __name__ == "__main__":
    sys.exit(main())
