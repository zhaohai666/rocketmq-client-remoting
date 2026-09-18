// 轻量客户端日志（对应 Java 的 rmq.client.logback.xml 与 Python 的 rocketmq/logging.py）。
//
// 设计目标：
//   - 默认级别 INFO，因此**良性事件**（如长轮询超时）走 DEBUG 会被抑制，不污染运行日志；
//   - 同时输出到 stderr 与文件，便于"日志是否干净"的可验证性；
//   - **按大小轮转**（对齐 Java logback 的 SizeBasedTriggeringPolicy + FixedWindow），
//     避免常驻进程把单个日志文件撑到无限大。
//
// 配置（环境变量）：
//   ROCKETMQ_CPP_LOG_LEVEL          = DEBUG | INFO | WARN | ERROR | OFF      （默认 INFO）
//   ROCKETMQ_CPP_LOG_FILE           = 日志文件绝对路径
//                                     （默认 $HOME/logs/rocketmqlogs/rocketmq_cpp_client.log；
//                                      设为 "OFF"/"NONE"/空串可关闭文件输出，只留 stderr）
//   ROCKETMQ_CPP_LOG_FILE_MAX_SIZE  = 单文件上限字节数（默认 67108864 = 64MB，对齐 Java
//                                     logback 的 <maxFileSize>64MB</maxFileSize>；0 = 不轮转）
//   ROCKETMQ_CPP_LOG_FILE_MAX_INDEX = 保留的备份份数（默认 10，对齐 Java
//                                     rocketmq.log.file.maxIndex；0 = 不保留备份）
//
// ⚠ 级别与文件路径在**首次写日志时**求值并缓存，因此必须在第一次日志输出前设置环境变量。
//   程序内可用 SetLogLevel() / SetLogFile() 直接改（后者会重新打开文件）。
//
// 日志行格式（对齐 Java 的 `%d{...SSS} %-5p [%pid] [%t] [%logger#%M:%L] - %m`）：
//   2026-09-14 16:57:21.123 INFO  [54606] [ConsumeMessageThread_0] [consumer.cpp:289] - ...
//   Java 的 %logger#%M:%L 是"日志器名#方法:行"，本实现没有日志器层级，用**文件名:行号**替代，
//   信息量等价且更直观。文件名取调用点路径的基名（Path.GetFileName，对应 C++ 的 logBaseName）。
//   线程名：主线程落 [main]（对齐 Java），工作线程由客户端内部命名
//   （ConsumeMessageThread_N / AsyncSenderThread_N / RemotingClientReader-<addr>），
//   未命名线程回落 OS 线程名，再回落 [tid-xxxx]。
//
// 与 Java 的已知差异（在此显式记录，避免被当成 bug）：
//   1. 备份文件**不压缩**（Java 会 gzip 成 other_days/rocketmq_client-%i.log.gz）；
//      备份名是 `<file>.1` … `<file>.N`（FixedWindow 语义），仍是滚动窗口而不是按天归档。
//   2. **同步写**（Java 走 AsyncAppender）。每行之后 Flush，所以 `tail -f` 实时可见。
//   3. 连接关闭记为 DEBUG。"对端关闭连接"在 Java 里走 Netty 的 channelInactive 会打一行
//      INFO/WARN，但正常 shutdown 也会命中同一条路径，在默认 INFO 级别下会变成退出时的
//      假异常噪声。真正的协议异常（帧长非法、解码失败）仍按 WARN 记录。
//
// C# 移植面的非显然判断（与 C++ 的差异）：
//   - C++ 用 __FILE__/__LINE__ 宏捕获调用点；C# 用 [CallerFilePath]/[CallerLineNumber]
//     特性做到等价效果，因此 Debug/Info/Warn/Error 是带可选 caller 参数的公开方法。
//   - C++ 的 thread_local 显式名缓存在 C# 天然成立：SetThreadName 直接设置
//     Thread.CurrentThread.Name（每个托管线程各自持有），同时缓存到 [ThreadStatic] 字段。
//   - .NET BCL 没有可移植的"OS 线程名"读取 API，故 C++ 的 "OS 线程名" 这一档在 C# 中
//     即等价于托管线程名（已由 SetThreadName 设置）；未命名线程回落 "main" / "tid-xxxx"。
//   - 主线程判定：在类型静态初始化时捕获 Environment.CurrentManagedThreadId（那一刻
//     通常是进程主线程），后续比对即可，对应 C++ 在 main 之前动态初始化阶段捕获主线程 id。
//   - 级别/路径/轮转参数均用 Lazy 在首次使用时求值并缓存，之后不再读环境变量。
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace RocketMQ.Common;

// 日志级别（数值对齐 C++ 的 enum LogLevel）。
public enum LogLevel
{
    Debug = 0,
    Info = 1,
    Warn = 2,
    Error = 3,
    Off = 4,
}

// 轻量客户端日志。静态类，进程内单例；只依赖 BCL。
public static class ClientLog
{
    // ---------------------------------------------------------------- 内部状态
    // 日志文件输出状态（对应 C++ 的 LogFileSink）。
    private sealed class LogFileState
    {
        public string Path = "";            // 空 = 关闭文件输出
        public StreamWriter? Writer;        // null = 尚未打开
        public bool ExplicitPath;           // 是否由 SetLogFile() 指定
        public bool OpenFailed;             // 打开失败则静默降级为仅 stderr
        public long Size;                   // 当前文件已写字节数（本进程视角）
    }

    private static readonly LogFileState Sink = new();
    private static readonly object FileLock = new();

    // 文件写入统一用无 BOM 的 UTF-8，字节数与 Size 统计一致。
    private static readonly Encoding Utf8NoBom = new UTF8Encoding(false);

    // ---------------------------------------------------------------- 级别
    // 环境变量原始值，首次访问时读取并缓存（之后不再读环境变量）。
    private static readonly Lazy<string?> RawLevelEnv =
        new(() => Environment.GetEnvironmentVariable("ROCKETMQ_CPP_LOG_LEVEL"));

    private static readonly Lazy<int> DefaultLevel =
        new(() => ParseLevel(RawLevelEnv.Value));

    // -1 = 尚未解析（首次使用时按环境变量/默认填充）；>=0 表示已确定的级别。
    // SetLogLevel 通过 Interlocked 覆盖该值，等价于 C++ 的 std::atomic<int>。
    private static int _level = -1;

    private static int ResolveLevelOnce()
    {
        int resolved = DefaultLevel.Value;
        Interlocked.CompareExchange(ref _level, resolved, -1);
        return _level;
    }

    // 环境变量是否**显式**指定了级别。宿主程序若要"默认 INFO、但尊重外部显式配置"，
    // 写成 `if (!ClientLog.LogLevelSetFromEnv()) ClientLog.SetLogLevel(LogLevel.Info);`，
    // 这样真机排查时直接 `ROCKETMQ_CPP_LOG_LEVEL=DEBUG ./app` 就能提高日志级别而不必改代码。
    public static bool LogLevelSetFromEnv() => RawLevelEnv.Value is not null;

    public static LogLevel LogLevel
    {
        get
        {
            int v = _level;
            if (v < 0) v = ResolveLevelOnce();
            return (LogLevel)v;
        }
        set => Interlocked.Exchange(ref _level, (int)value);
    }

    public static void SetLogLevel(LogLevel level) => LogLevel = level;

    // ---------------------------------------------------------------- 轮转参数
    // Java logback: <maxFileSize>64MB</maxFileSize> + maxIndex 默认 10
    private static readonly Lazy<long> MaxSize =
        new(() => ResolveMaxSize());

    private static readonly Lazy<int> MaxIndex =
        new(() => ResolveMaxIndex());

    // 运行时覆盖（优先于环境变量）：宿主程序/测试可在启动后注入阈值。
    private static long? _maxSizeOverride;
    private static int? _maxIndexOverride;

    /// <summary>
    /// 程序内设定轮转阈值（优先于环境变量）。maxSize&lt;=0 关闭按大小轮转；
    /// maxIndex&lt;=0 不保留备份。对应 C++ 只能通过环境变量在启动前设置。
    /// </summary>
    public static void SetLogFileLimits(long maxSize, int maxIndex)
    {
        _maxSizeOverride = maxSize;
        _maxIndexOverride = maxIndex;
    }

    public static long LogFileMaxSize => _maxSizeOverride ?? MaxSize.Value;
    public static int LogFileMaxIndex => _maxIndexOverride ?? MaxIndex.Value;

    // ---------------------------------------------------------------- 日志文件
    // 默认路径，首次访问时读取环境变量并缓存。$HOME 为空时回落 USERPROFILE（Windows）。
    private static readonly Lazy<string> DefaultFilePath =
        new(() => ResolveDefaultFilePath());

    // 文件名刻意与 Java 的 rocketmq_client.log 区分：同机同时跑 Java 客户端时
    // 两边轮转策略不同，写同一文件会互相插行、互相截断。
    private static string ResolveDefaultFilePath()
    {
        string? env = Environment.GetEnvironmentVariable("ROCKETMQ_CPP_LOG_FILE");
        if (env is not null)
        {
            if (env.Length == 0 || env == "OFF" || env == "NONE") return "";
            return env;
        }

        string home = UtilAll.UserHome();
        if (string.IsNullOrEmpty(home)) return "";
        return Path.Combine(home, "logs", "rocketmqlogs", "rocketmq_cpp_client.log");
    }

    // 宿主程序可直接指定日志文件（空串 = 关闭文件输出）。
    public static void SetLogFile(string path)
    {
        lock (FileLock)
        {
            CloseSinkLocked();
            Sink.Path = path ?? "";
            Sink.ExplicitPath = true;
            Sink.OpenFailed = false;
        }
    }

    public static void FlushLogFile()
    {
        lock (FileLock)
        {
            Sink.Writer?.Flush();
        }
    }

    // ---------------------------------------------------------------- 线程名
    // 取值优先级：本线程设置的名称（thread_local，天然按线程持有）-> "main"（进程主线程）
    //   -> OS 线程名（C# 无独立可移植 API，等价于托管线程名）-> tid 短哈希。
    // 结果按线程缓存（[ThreadStatic]），线程改名请用 SetThreadName()。
    [ThreadStatic]
    private static string? _threadNameResolved;

    // 在类型静态初始化阶段捕获主线程 id（那一刻通常是进程主线程，对应 C++ 在 main 前
    // 动态初始化阶段捕获主线程 id），日志启动阶段显示 [main]，与 Java 客户端一致。
    private static readonly int MainThreadId = Environment.CurrentManagedThreadId;

    // 命名当前线程：更新日志用名，并尽力设置框架线程名（Thread.CurrentThread.Name 每个线程
    // 仅能赋值一次，已设置则仅更新缓存；对应 Java 的 ThreadFactory 命名）。
    public static void SetThreadName(string name)
    {
        _threadNameResolved = name;
        try
        {
            if (string.IsNullOrEmpty(Thread.CurrentThread.Name))
            {
                Thread.CurrentThread.Name = name;
            }
        }
        catch (InvalidOperationException)
        {
            // 线程名只能赋值一次；缓存已设置，忽略框架侧的冲突。
        }
    }

    public static string CurrentThreadName()
    {
        if (_threadNameResolved is not null) return _threadNameResolved;

        string? name = Thread.CurrentThread.Name;
        if (!string.IsNullOrEmpty(name))
        {
            _threadNameResolved = name;
            return name;
        }

        if (Environment.CurrentManagedThreadId == MainThreadId)
        {
            _threadNameResolved = "main";
            return "main";
        }

        // 未命名线程回落到托管线程 id 的短哈希：稳定、可区分，但不可读（Java 侧总是有线程名）。
        int tid = Environment.CurrentManagedThreadId;
        string fallback = "tid-" + (tid & 0xffff).ToString("x4", CultureInfo.InvariantCulture);
        _threadNameResolved = fallback;
        return fallback;
    }

    // ---------------------------------------------------------------- 公开入口
    // 以下四个方法对应 C++ 的 logger_debug/info/warn/error 宏。[CallerFilePath]/
    // [CallerLineNumber] 在**调用点**展开，等价于宏捕获的 __FILE__/__LINE__。
    public static void Debug(string msg,
        [CallerFilePath] string file = "", [CallerLineNumber] int line = 0)
        => WriteLine(LogLevel.Debug, msg, file, line);

    public static void Info(string msg,
        [CallerFilePath] string file = "", [CallerLineNumber] int line = 0)
        => WriteLine(LogLevel.Info, msg, file, line);

    public static void Warn(string msg,
        [CallerFilePath] string file = "", [CallerLineNumber] int line = 0)
        => WriteLine(LogLevel.Warn, msg, file, line);

    public static void Error(string msg,
        [CallerFilePath] string file = "", [CallerLineNumber] int line = 0)
        => WriteLine(LogLevel.Error, msg, file, line);

    // ---------------------------------------------------------------- 内部实现
    private static string LevelName(LogLevel level) => level switch
    {
        LogLevel.Debug => "DEBUG",
        LogLevel.Info => "INFO",
        LogLevel.Warn => "WARN",
        LogLevel.Error => "ERROR",
        _ => "OFF",
    };

    private static int ParseLevel(string? raw)
    {
        if (raw is null) return (int)LogLevel.Info;
        string s = raw.Trim().ToUpperInvariant();
        return s switch
        {
            "DEBUG" => (int)LogLevel.Debug,
            "INFO" => (int)LogLevel.Info,
            "WARN" or "WARNING" => (int)LogLevel.Warn,
            "ERROR" => (int)LogLevel.Error,
            "OFF" or "NONE" => (int)LogLevel.Off,
            _ => (int)LogLevel.Info,
        };
    }

    private static long ResolveMaxSize()
    {
        const long kDefault = 64L * 1024 * 1024; // 64MB
        string? raw = Environment.GetEnvironmentVariable("ROCKETMQ_CPP_LOG_FILE_MAX_SIZE");
        if (raw is null) return kDefault;
        return long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out long v) && v > 0
            ? v
            : kDefault;
    }

    private static int ResolveMaxIndex()
    {
        string? raw = Environment.GetEnvironmentVariable("ROCKETMQ_CPP_LOG_FILE_MAX_INDEX");
        if (raw is null) return 10;
        return int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out int v) && v > 0
            ? v
            : 10;
    }

    private static void CloseSinkLocked()
    {
        Sink.Writer?.Dispose();
        Sink.Writer = null;
        Sink.Size = 0;
    }

    private static bool OpenSinkLocked()
    {
        if (Sink.Path.Length == 0 || Sink.OpenFailed) return false;
        try
        {
            string? dir = Path.GetDirectoryName(Sink.Path);
            if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);

            // FileShare.ReadWrite：Windows 是强制锁，默认的 FileShare.Read 会让 tail 式并发读
            // 直接抛 IOException（POSIX 下无强制锁所以只在 Windows 暴露）。
            Sink.Writer = new StreamWriter(
                new FileStream(Sink.Path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite),
                Utf8NoBom);
            long existing = 0;
            try
            {
                existing = new FileInfo(Sink.Path).Length;
            }
            catch (IOException)
            {
                existing = 0;
            }
            Sink.Size = existing;
            return true;
        }
        catch (Exception)
        {
            // 静默降级为仅 stderr（不阻断客户端），但只提示一次。
            Sink.OpenFailed = true;
            try
            {
                Console.Error.WriteLine("[rocketmq] client file log disabled: cannot open " + Sink.Path);
            }
            catch (Exception)
            {
                // 控制台也不可用时忽略
            }
            return false;
        }
    }

    // FixedWindow 滚动：<file>.N 最旧，先删；然后 .N-1 -> .N …… .1 -> .2；最后 base -> .1
    private static void RollLocked(string basePath, int maxIndex)
    {
        string top = basePath + "." + maxIndex.ToString(CultureInfo.InvariantCulture);
        if (File.Exists(top)) File.Delete(top);

        for (int i = maxIndex - 1; i >= 1; i--)
        {
            string src = basePath + "." + i.ToString(CultureInfo.InvariantCulture);
            if (!File.Exists(src)) continue;
            string dst = basePath + "." + (i + 1).ToString(CultureInfo.InvariantCulture);
            if (File.Exists(dst)) File.Delete(dst);
            File.Move(src, dst);
        }

        if (File.Exists(basePath))
        {
            File.Move(basePath, basePath + ".1");
        }
    }

    // file/line 是**调用点**（由 [CallerFilePath]/[CallerLineNumber] 传入），对应 Java 的 %M:%L。
    private static void WriteLine(LogLevel level, string msg, string file, int line)
    {
        if ((int)level < (int)LogLevel) return;

        DateTime now = DateTime.Now; // 本地时间，对应 C++ 的 localtime_r
        string ts = now.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        string ms = now.Millisecond.ToString("D3", CultureInfo.InvariantCulture);
        string levelName = LevelName(level);
        string fileName = string.IsNullOrEmpty(file) ? "?" : Path.GetFileName(file);
        string threadName = CurrentThreadName();
        string pid = Environment.ProcessId.ToString(CultureInfo.InvariantCulture);

        // %-5s 左对齐 5 位宽（"INFO " 带一个尾随空格，再接 " ["），与 Java %-5p 对齐。
        string head = string.Format(
            CultureInfo.InvariantCulture,
            "{0}.{1} {2,-5} [{3}] [{4}] [{5}:{6}] - ",
            ts, ms, levelName, pid, threadName, fileName, line);
        string output = head + (msg ?? "") + "\n";

        // stderr（每次 Write 自身原子，保证 tail -f 实时可见）。
        Console.Error.Write(output);
        Console.Error.Flush();

        // 文件：在锁内保证行原子性，并按大小轮转。
        lock (FileLock)
        {
            if (!Sink.ExplicitPath) Sink.Path = DefaultFilePath.Value;
            if (Sink.Path.Length == 0 || Sink.OpenFailed) return;
            if (Sink.Writer is null && !OpenSinkLocked()) return;

            long maxSize = LogFileMaxSize;
            int maxIndex = LogFileMaxIndex;
            if (maxSize > 0 && maxIndex > 0)
            {
                long lineBytes = Utf8NoBom.GetByteCount(output);
                if (Sink.Size + lineBytes > maxSize)
                {
                    try
                    {
                        CloseSinkLocked();
                        RollLocked(Sink.Path, maxIndex);
                    }
                    catch (IOException)
                    {
                        // 轮转失败则降级为直接写新文件，不阻断客户端（对齐 C++ 忽略 error_code）。
                    }
                    if (!OpenSinkLocked()) return;
                }
            }

            StreamWriter writer = Sink.Writer!;
            writer.Write(output);
            writer.Flush(); // 保证 tail -f 实时可见
            Sink.Size += Utf8NoBom.GetByteCount(output);
        }
    }
}
