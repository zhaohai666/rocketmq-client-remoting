// WARNING: Level and file path are evaluated and cached on the first log write, so the environment
//   variables must be set before the first log output.
//   Inside the program you can change them directly via SetLogLevel() / SetLogFile() (the latter reopens the file).
//
//
//
// Non-obvious implementation notes:
//   - The call site is expanded at the call location via the [CallerFilePath]/[CallerLineNumber] attributes,
//     so Debug/Info/Warn/Error are public methods that take optional caller parameters.
//   - Explicit thread name caching is naturally satisfied: SetThreadName directly sets
//     Thread.CurrentThread.Name (each managed thread holds its own), and also caches it into a [ThreadStatic] field.
//   - There is no portable "OS thread name" read API, so this tier is simply
//     the managed thread name (already set by SetThreadName); unnamed threads fall back to "main" / "tid-xxxx".
//   - Main-thread detection: capture Environment.CurrentManagedThreadId during the type's static initialization
//     (at that moment it is usually the process main thread), then compare later.
//   - Level/path/rotation parameters are all evaluated and cached on first use via Lazy, after which env vars are no longer read.
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace RocketMQ.Common;

// Log level (values follow the common convention used by client logging).
public enum LogLevel
{
    Debug = 0,
    Info = 1,
    Warn = 2,
    Error = 3,
    Off = 4,
}

// Lightweight client logger. Static class, process-wide singleton; depends only on the BCL.
public static class ClientLog
{
    // ---------------------------------------------------------------- Internal state
    // Log file output state (file sink).
    private sealed class LogFileState
    {
        public string Path = "";            // empty = file output disabled
        public StreamWriter? Writer;        // null = not yet opened
        public bool ExplicitPath;           // whether set via SetLogFile()
        public bool OpenFailed;             // if open failed, silently degrade to stderr only
        public long Size;                   // bytes written to the current file so far (from this process view)
    }

    private static readonly LogFileState Sink = new();
    private static readonly object FileLock = new();

    // File writes uniformly use BOM-less UTF-8, consistent with the Size accounting in bytes.
    private static readonly Encoding Utf8NoBom = new UTF8Encoding(false);

    // ---------------------------------------------------------------- Level
    // Raw environment variable value, read and cached on first access (env vars are not read afterwards).
    private static readonly Lazy<string?> RawLevelEnv =
        new(() => Environment.GetEnvironmentVariable("ROCKETMQ_CLIENT_LOG_LEVEL"));

    private static readonly Lazy<int> DefaultLevel =
        new(() => ParseLevel(RawLevelEnv.Value));

    // -1 = not yet resolved (filled from env var/default on first use); >=0 means a determined level.
    // SetLogLevel overrides this value via Interlocked to guarantee atomic read/write.
    private static int _level = -1;

    private static int ResolveLevelOnce()
    {
        int resolved = DefaultLevel.Value;
        Interlocked.CompareExchange(ref _level, resolved, -1);
        return _level;
    }

    // Whether the environment variable **explicitly** specified the level. If a host program wants
    // "default INFO, but respect explicit external config", write `if (!ClientLog.LogLevelSetFromEnv()) ClientLog.SetLogLevel(LogLevel.Info);`,
    // so that in real-device troubleshooting you can simply `ROCKETMQ_CLIENT_LOG_LEVEL=DEBUG ./app` to raise the level without changing code.
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

    private static readonly Lazy<long> MaxSize =
        new(() => ResolveMaxSize());

    private static readonly Lazy<int> MaxIndex =
        new(() => ResolveMaxIndex());

    // Runtime override (takes precedence over env var): host program/tests can inject thresholds after startup.
    private static long? _maxSizeOverride;
    private static int? _maxIndexOverride;

    /// <summary>
    /// Set the rotation thresholds in-process (takes precedence over env vars). maxSize&lt;=0 disables size-based rotation;
    /// maxIndex&lt;=0 keeps no backups (env vars can only be set before startup, whereas this method overrides at runtime).
    /// </summary>
    public static void SetLogFileLimits(long maxSize, int maxIndex)
    {
        _maxSizeOverride = maxSize;
        _maxIndexOverride = maxIndex;
    }

    public static long LogFileMaxSize => _maxSizeOverride ?? MaxSize.Value;
    public static int LogFileMaxIndex => _maxIndexOverride ?? MaxIndex.Value;

    // ---------------------------------------------------------------- Log file
    // Default path, read from env var and cached on first access. Falls back to USERPROFILE (Windows) when $HOME is empty.
    private static readonly Lazy<string> DefaultFilePath =
        new(() => ResolveDefaultFilePath());

    private static string ResolveDefaultFilePath()
    {
        string? env = Environment.GetEnvironmentVariable("ROCKETMQ_CLIENT_LOG_FILE");
        if (env is not null)
        {
            if (env.Length == 0 || env == "OFF" || env == "NONE") return "";
            return env;
        }

        string home = UtilAll.UserHome();
        if (string.IsNullOrEmpty(home)) return "";
        return Path.Combine(home, "logs", "rocketmqlogs", "rocketmq_csharp_client.log");
    }

    // Host program can specify the log file directly (empty string = disable file output).
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

    // ---------------------------------------------------------------- Thread name
    // Resolution priority: name set by this thread (thread_local, naturally thread-held) -> "main" (process main thread)
    //   -> OS thread name (C# has no separate portable API, equivalent to managed thread name) -> tid short hash.
    // Result is cached per thread ([ThreadStatic]); to rename a thread use SetThreadName().
    [ThreadStatic]
    private static string? _threadNameResolved;

    private static readonly int MainThreadId = Environment.CurrentManagedThreadId;

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
            // A thread name can only be assigned once; the cache is already set, so ignore the framework-side conflict.
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

        int tid = Environment.CurrentManagedThreadId;
        string fallback = "tid-" + (tid & 0xffff).ToString("x4", CultureInfo.InvariantCulture);
        _threadNameResolved = fallback;
        return fallback;
    }

    // ---------------------------------------------------------------- Public entry points
    // The four methods below are the public entry points for log output. [CallerFilePath]/
    // [CallerLineNumber] are expanded at the **call site**, capturing the caller's file and line number.
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

    // ---------------------------------------------------------------- Internal implementation
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
        string? raw = Environment.GetEnvironmentVariable("ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE");
        if (raw is null) return kDefault;
        return long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out long v) && v > 0
            ? v
            : kDefault;
    }

    private static int ResolveMaxIndex()
    {
        string? raw = Environment.GetEnvironmentVariable("ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX");
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

            // FileShare.ReadWrite: on Windows this is a mandatory lock; the default FileShare.Read would make a tail-style
            // concurrent reader throw IOException directly (no mandatory lock under POSIX, so only exposed on Windows).
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
            // Silently degrade to stderr only (do not block the client), but only warn once.
            Sink.OpenFailed = true;
            try
            {
                Console.Error.WriteLine("[rocketmq] client file log disabled: cannot open " + Sink.Path);
            }
            catch (Exception)
            {
                // Ignore when the console is also unavailable
            }
            return false;
        }
    }

    // FixedWindow rotation: <file>.N is oldest, delete first; then .N-1 -> .N ... .1 -> .2; finally base -> .1
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

    private static void WriteLine(LogLevel level, string msg, string file, int line)
    {
        if ((int)level < (int)LogLevel) return;

        DateTime now = DateTime.Now; // local time
        string ts = now.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        string ms = now.Millisecond.ToString("D3", CultureInfo.InvariantCulture);
        string levelName = LevelName(level);
        string fileName = string.IsNullOrEmpty(file) ? "?" : Path.GetFileName(file);
        string threadName = CurrentThreadName();
        string pid = Environment.ProcessId.ToString(CultureInfo.InvariantCulture);

        string head = string.Format(
            CultureInfo.InvariantCulture,
            "{0}.{1} {2,-5} [{3}] [{4}] [{5}:{6}] - ",
            ts, ms, levelName, pid, threadName, fileName, line);
        string output = head + (msg ?? "") + "\n";

        // stderr (each Write is atomic on its own, guaranteeing tail -f visibility in real time).
        Console.Error.Write(output);
        Console.Error.Flush();

        // File: guarantee line atomicity inside the lock, and rotate by size.
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
                        // On rotation failure, degrade to writing a fresh file directly, without blocking the client (error code intentionally ignored).
                    }
                    if (!OpenSinkLocked()) return;
                }
            }

            StreamWriter writer = Sink.Writer!;
            writer.Write(output);
            writer.Flush(); // guarantee tail -f visibility in real time
            Sink.Size += Utf8NoBom.GetByteCount(output);
        }
    }
}
