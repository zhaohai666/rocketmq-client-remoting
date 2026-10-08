# PHP 移植约定（所有移植子任务必须遵守）

参考蓝本：`../python/`（已与 Java 逐协议对齐并真机验证）。Java 原始语义只在
Python 侧注释不清楚时查 `D:\zhaohai666-rocketmq\rocketmq` 对应类。

## 语言与文件

- PHP **8.3**，每个文件 `declare(strict_types=1);`
- 命名空间：`RocketMQ\Common` / `RocketMQ\Remoting` / `RocketMQ\Remoting\Protocol` / `RocketMQ\Client`
- 一个 Python 模块 = 一个 PascalCase PHP 类文件；多类域文件（bodies/headers/exceptions）允许一个文件多个类。
  autoloader（`bootstrap.php`）先按 PSR-4 找同名文件，未命中则回退到一次性 token 扫描生成的
  全量 classmap —— 所以多类文件的命名空间**不必**等于目录（例如
  `src/Client/Exceptions.php` 里是 `RocketMQ\Client\Exceptions`，`src/Remoting/Protocol/Headers.php`
  里是 `RocketMQ\Remoting\Protocol`）。新增多类文件后无需改 bootstrap。
- **类名唯一性**：PHP 无模块隔离，同一 FQCN 在两个文件声明时先被 autoload 命中的胜出，
  另一个**静默失效**。因此 Python 模块名直译若与已占用的类名相同，必须改名——见
  `TraceContextPropagator`（原 `trace_context.py`，因 Java `trace.TraceContext` 已占用而改名）。
  `tests/run_all.php` 的静态守卫会扫描全量重复声明并直接判失败。
- **不引入任何 composer 依赖**，只用 ext-json/sockets/openssl/mbstring。

## 类型与结构

- Python dataclass/实体 → PHP final class + 构造器属性提升或 typed properties；
  `Optional` → `?T`；`Dict/List` → `array` + PHPDoc `array<string, X>`。
- Python Enum → PHP backed enum（`enum MessageType: string`）。
- Python 抛异常 → PHP 异常体系（全部放 `src/Client/Exceptions.php`，命名空间
  `RocketMQ\Client\Exceptions`）：
  - `MQException`（基类，RuntimeException）
  - `MQClientException` / `MQBrokerException`（带 responseCode/errMsg）
  - `RemotingConnectException` / `RemotingSendRequestException` / `RemotingTimeoutException`
    / `RequestTimeoutException` / `UnsupportedOperationException`
- Python `logging` → 轻量 `RocketMQ\Client\Logger`（静态可注入 callable，默认 stderr，
  级别 INFO 对齐 python/rocketmq_logging.py）。

## 线格式（关键，必须与 Python/Java 字节兼容）

- 帧编码：`pack('N', totalLength)` + `pack('N', headerLength|serializeType<<24)` + header + body，
  端序一律大端（`pack('N')`/`pack('n')`），参照 `remoting/protocol/serialize.py`。
- JSON header：`json_encode($v, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES)`；
  解码用 `json_decode($s, true, 512, JSON_THROW_ON_ERROR)`。
  Python 侧字段名（camelCase）原样保留，这是 broker 兼容的关键。
- properties/attributes：`PropertyMap` 语义 = `array<string,string>`（有序），JSON 序列化为对象。
- opaque 自增：静态 `AtomicInt`（PHP 单线程下就是 `static int` 自增即可）。
- LanguageCode 用 `JAVA`？**不**——对齐 Python：`LanguageCode::PYTHON` 有对应 `PHP` 值。
  Java LanguageCode 里没有 PHP，参照 Python 做法：Python 自增了 `PYTHON('PYTHON')`；
  PHP 端加 `PHP('PHP')`（broker 只做展示，不校验枚举值）。

## 异步模型

PHP 无线程：`invokeAsync` = 非阻塞写 + pending 表（opaque→[onSuccess,onFailure]），
`waitResponses(timeout)` 用 `stream_select` 泵响应分发回调。Producer 的 asyncSend 在回调注册后
**内部泵到完成**（对调用方呈现与 Python 相同的回调时序）。

**消费者的单线程适配**（`PushConsumer.php` 类头有完整说明，改动前先读）：
Python 的 6 类后台线程全部收敛为调用方驱动的 `tick()`——心跳 30s / 重平衡 20s（启动无分配 2s
快重试）/ 位点落盘 10s+5s / 顺序锁 20s / 过期清扫改为 tick 内到点检查；每队列长轮询线程改为
**短轮询**（suspend=false）+ 空结果空闲退避 `pullIntervalMillis` + 每队列每 tick 至多
`maxPullsPerQueuePerTick` 轮；顺序消费的 sleep 挂起改为 `suspendedUntil` 表；POP 用 pollTime=0
短轮询 + 批次内联执行。注意一个有意偏差：并发消费回投失败的条目 dispatchRound **同 tick 立即
重试**（Java 是 submitConsumeRequestLater 延迟），C4 位点地板的中间态因此不可观测。

## 测试与验证

- 单测：`php tests/run_all.php`（纯 PHP assert 风格 runner，禁依赖 phpunit）。
  当前：Common 205 + Remoting 196 + Client 叶子 478 + Client 轨迹 209 + Client 聚合器 149
  + Client 实例 153 + **Client 消费者 133** = **1523 项全绿**，另含「类名冲突静态守卫」。
- 单套件直接跑：`php tests/RunXxx.php`（成功打印 `ALL TESTS PASSED (N checks)`，失败非零退出）。
  消费者套件（`RunClientConsumer.php`）用子进程假 broker（`--broker` 自拉起）承载有状态剧本：
  每队列拉取计数（首拉 FOUND / OFFSET_ILLEGAL，后续 NO_NEW_MSG）、PULL/UPDATE_OFFSET/SEND_BACK/
  LOCK_BATCH_MQ 请求写 JSONL 日志供父进程断言线上行为（sysFlag suspend 位、delayLevel、mqSet）。
- 真机（macOS）：`scripts/with_cluster.sh` 是 Windows Git Bash 版，跑不了。统一入口
  `bash scripts/run_php_live.sh redelivery|admin|compression`：自起 5.5.1 集群
  （`/tmp/rmq_php_live`，ROCKETMQ_HOME + 2g 堆）→ 等端口 → **等 broker 路由注册进
  nameserver**（`php examples/wait_broker_route.php`，否则一切 createTopic/send 死于
  "No route info of default topic TBW102"）→ 跑 → jps 找 pid kill，全在同一条命令。
  - `examples/live_redelivery.php`：S1 %RETRY% 回投梯度（level3≈10s）+ topic 还原 +
    正常消息恰好 1 次；S2 死信终态两半断言（maxReconsumeTimes=2 ⇒ 客户端恰好 3 次投递
    0/1/2 且 %DLQ% 里 recon=3）；S3 顺序毒消息（本地 3 次后普通 SEND 进 %DLQ%，严格 `>`
    判定，DLQ recon=3 是**客户端侧 +1**）；S4 部分 ack（ackIndex=0 ⇒ 尾巴 2 条回投、
    已 ack 的只投 1 次、位点仍整批=3，对照组零回投）。26/26。
  - `examples/live_admin.php`：集群探活 → topic CRUD/路由/topicConfig/集群归属 →
    broker 配置/运行时 stats → KV CRUD → 订阅组 CRUD（retryMaxTimes=5 往返）→
    发消息 + topicStats（**按 SUM 断言**，消息轮询散队列，MAX 恒 1）→ searchOffset
    上下界/earliestMsgStoreTime → KEYS 索引回读 + viewMessage → deleteTopic。23/23。
  - `examples/live_compression.php`：send/recv 双模式腿（`send <topic> <group> <size>
    <ns> [codec]` / `recv ...`），载荷配方与 Java CompressProbe 逐字节一致，判定只看
    接收端 `match=1`（CRC-32 IEEE，`sprintf('%u', crc32(...))`），退出码 0/1/2/3。
    已在 `scripts/compression_matrix.sh` 注册 php_* 腿；PHP 仅 zlib，lz4/zstd → exit 2。

## 踩过的坑（移植时务必对照）

- **异常类必须 use**：异常全部在 `RocketMQ\Client\Exceptions`，源文件里 `throw/catch MQClientException`
  少一行 `use` 就会解析成不存在的 `RocketMQ\Client\MQClientException`——catch **静默不匹配**、
  throw 直接 fatal。`php -l` 和 Reflection 加载**查不出来**（不执行 throw/catch）；用
  "new/catch/instanceof/Class:: 裸类名扫描 + class_exists 探测"的脚本查，消费者三件套曾一次挖出 4 处。
- **`SubscriptionData->tagsSet` 是 list 不是 map**：`addTag` 用 `in_array` 追加，`isset($tagsSet[$tag])`
  按键查永远 miss → 整批消息被静默滤掉。判断命中一律 `in_array($tag, $tagsSet, true)`
  （与 Node 端「Set.size 标签过滤」同族坑）。
- **start() 内部调用顺序**：`requireClient()` 检查 `$started`，而 start 流程中段（LitePull 的
  路由预热、Push shutdown 的顺序解锁）在 started 置位/复位边沿上调用它 → 死锁或死代码。
  这些位置必须直取 `$this->mqClient` 并判 null。
- **订阅 tag 表达式不走构造器解析**：`new SubscriptionData('T','TagA||TagB')` 不填 tagsSet，
  必须走 `FilterAPI::buildSubscriptionData`。
- **单线程 socket 时序**：客户端建连是惰性的（首次 send 才 connect），而 `invokeSync` 会阻塞
  在读响应上。测试里若想在 server 侧 accept / 读写，必须先 `invokeOneway` 建连再 accept；
  同步用例要么预先把响应写进连接缓冲，要么改用 `invokeAsync` + `waitResponses` 交叉推进。
- **fread 语义**：对端 close 后 Windows 上 `stream_select` 仍报 readable，`fread` 返回
  `false`（不是空串）且 `feof` 为 true。收帧循环必须把 `false` 一并按 feof 判定，否则会空转到超时。
- **空串 value 的 ROCKETMQ 二进制 map 是有损的**：长度 0 编码后解码为 null 并被丢弃，
  Java/Python 同此语义，不是移植缺陷。
- **类名冲突是静默的**：`RocketMQ\Client\TraceContext` 曾被 W3C 传播助手与消息轨迹上下文
  同时声明，测试全绿但轨迹三件套在真实 autoload 路径下拿错类。改名 + 静态守卫后才真正暴露。
- **空 map 的 json_encode wire bug**：PHP 空 array 编码成 `[]`，而 Java 的 `attributes`
  等字段是 Map（默认 `{}`）——broker fastjson2 按 Map 反序列化直接抛
  `expect '{', but '['`。`SubscriptionGroupConfig`/`TopicConfig` 的 toDict 已修为
  空表输出 `new \stdClass()`。**新增任何带 Map 字段的 toDict 都要套同一招**。
- **等端口 ≠ 等路由**：broker 启动后（可达 10s+）才把路由注册进 nameserver，之前一切
  createTopic/send 死于 "No route info of default topic TBW102"——先跑
  `php examples/wait_broker_route.php`（fetchBrokerClusterInfo 探测）再开跑。
- **Admin 返回形状**：`getTopicClusterList` 等返回 **list<string>**，判定用 `in_array`，
  别按 map 键查（`$cl[$clusterName] ?? null` 永远 null，且 `implode(array_keys(...))`
  打出 "0" 极具迷惑性）；`TopicStatsTable->offsetTable` 是 list<{mq,value}>，发 N 条
  轮询散队列后**按 SUM 断言**，MAX 恒 1。
- **本机 php 进程打 `[CQ_POLLER]` 噪声**：每个 `php` 进程（含 `php -r`）输出两行，看结果先过滤。
