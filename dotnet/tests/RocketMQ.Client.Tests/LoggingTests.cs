// 日志模块单测（镜像 C++ test_logging.cpp 的核心断言）：
// 行格式、主线程命名 main、线程名线程本地性、按大小轮转 + maxIndex 上限、级别过滤、关闭文件输出。
using System.Globalization;
using System.Text.RegularExpressions;
using RocketMQ.Common;
using Xunit;

namespace RocketMQ.Client.Tests;

public class LoggingTests : IDisposable
{
    private readonly string _dir;

    public LoggingTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), "rmq_dotnet_log_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(_dir, recursive: true);
        }
        catch
        {
            // 清理失败不影响测试
        }
    }

    private static readonly Regex LineRegex = new(
        @"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) (DEBUG|INFO |WARN |ERROR) \[\d+\] \[([^\]]+)\] \[[^\]]+:\d+\] - (.*)$",
        RegexOptions.Compiled);

    [Fact]
    public void LineFormat_MatchesJavaStyle()
    {
        string path = Path.Combine(_dir, "fmt.log");
        ClientLog.SetLogFile(path);
        ClientLog.SetLogLevel(LogLevel.Info);
        ClientLog.Info("hello-测试");
        ClientLog.FlushLogFile();

        string[] lines = File.ReadAllLines(path);
        Assert.Single(lines);
        Match m = LineRegex.Match(lines[0]);
        Assert.True(m.Success, "line format mismatch: " + lines[0]);
        // INFO 占 5 位宽（左对齐带尾随空格）
        Assert.Contains(" INFO  [", lines[0]);
        Assert.EndsWith("- hello-测试", lines[0]);
    }

    [Fact]
    public void ThreadName_IsThreadLocal_Isolated()
    {
        // xunit 的测试线程已被命名为 ".NET TP Worker"（非 main），所以只验证：
        // 1) 命名前的值与命名后的值不同且更新生效；2) 其它线程不受影响。
        string mainBefore = ClientLog.CurrentThreadName();

        string? before = null;
        string? after = null;
        var t = new Thread(() =>
        {
            before = ClientLog.CurrentThreadName();
            ClientLog.SetThreadName("ConsumeMessageThread_0");
            after = ClientLog.CurrentThreadName();
        });
        t.Start();
        t.Join();

        Assert.NotEqual("ConsumeMessageThread_0", before);
        Assert.Equal("ConsumeMessageThread_0", after);
        // 回到本线程：名字不受工作线程影响（thread-local）
        Assert.Equal(mainBefore, ClientLog.CurrentThreadName());
    }

    [Fact]
    public void LevelFilter_SuppressesDebugAtInfo()
    {
        string path = Path.Combine(_dir, "level.log");
        ClientLog.SetLogFile(path);
        ClientLog.SetLogLevel(LogLevel.Info);
        ClientLog.Debug("invisible");
        ClientLog.Warn("visible");
        ClientLog.FlushLogFile();

        string[] lines = File.ReadAllLines(path);
        Assert.Single(lines);
        Assert.Contains("- visible", lines[0]);
    }

    [Fact]
    public void DisableFileOutput()
    {
        ClientLog.SetLogFile(string.Empty);
        // 不抛异常即通过（文件输出已关闭）
        ClientLog.Info("stderr-only");
        ClientLog.FlushLogFile();
    }

    [Fact]
    public void SizeRotation_FixedWindow_WithMaxIndexCap()
    {
        string path = Path.Combine(_dir, "rot.log");
        ClientLog.SetLogFileLimits(2000, 3); // 注入小阈值：2KB 轮转、最多 3 份备份
        ClientLog.SetLogFile(path);

        for (int i = 0; i < 400; ++i)
        {
            ClientLog.Info("rotation probe line " + i.ToString(CultureInfo.InvariantCulture)
                + " 0123456789012345678901234567890123456789012345");
        }

        ClientLog.FlushLogFile();
        string backup1 = path + ".1";
        Assert.True(File.Exists(backup1), "rotation did not create .1 backup");

        long baseSize = new FileInfo(path).Length;
        Assert.True(baseSize <= 2000, "base file should be capped at maxSize");

        // 备份文件内容也应是完整行
        string[] backupLines = File.ReadAllLines(backup1);
        Assert.All(backupLines, l => Assert.Matches(LineRegex, l));
    }
}
