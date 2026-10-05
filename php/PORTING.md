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

## 测试与验证

- 单测：`php tests/run_all.php`（纯 PHP assert 风格 runner，禁依赖 phpunit）。
  当前：Common 205 + Remoting 196 + Client 叶子 478 + Client 轨迹 209 + Client 聚合器 149
  + Client 实例 153 = **1390 项全绿**，另含「类名冲突静态守卫」。
- 单套件直接跑：`php tests/RunXxx.php`（成功打印 `ALL TESTS PASSED (N checks)`，失败非零退出）。
- 真机：仓库根 `bash scripts/with_cluster.sh bash -c '... php ...'`，
  PHP 路径：`C:/Users/zhaoh/.workbuddy/binaries/php/versions/8.3/php.exe`。

## 踩过的坑（移植时务必对照）

- **单线程 socket 时序**：客户端建连是惰性的（首次 send 才 connect），而 `invokeSync` 会阻塞
  在读响应上。测试里若想在 server 侧 accept / 读写，必须先 `invokeOneway` 建连再 accept；
  同步用例要么预先把响应写进连接缓冲，要么改用 `invokeAsync` + `waitResponses` 交叉推进。
- **fread 语义**：对端 close 后 Windows 上 `stream_select` 仍报 readable，`fread` 返回
  `false`（不是空串）且 `feof` 为 true。收帧循环必须把 `false` 一并按 feof 判定，否则会空转到超时。
- **空串 value 的 ROCKETMQ 二进制 map 是有损的**：长度 0 编码后解码为 null 并被丢弃，
  Java/Python 同此语义，不是移植缺陷。
- **类名冲突是静默的**：`RocketMQ\Client\TraceContext` 曾被 W3C 传播助手与消息轨迹上下文
  同时声明，测试全绿但轨迹三件套在真实 autoload 路径下拿错类。改名 + 静态守卫后才真正暴露。
