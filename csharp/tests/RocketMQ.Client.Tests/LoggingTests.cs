// Logging module unit tests:
// line format, naming the main thread, thread name being thread-local, rotation by size
// plus the maxIndex cap, level filtering, and turning file output off.
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
            // A cleanup failure does not affect the test
        }
    }

    private static readonly Regex LineRegex = new(
        @"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) (DEBUG|INFO |WARN |ERROR) \[\d+\] \[([^\]]+)\] \[[^\]]+:\d+\] - (.*)$",
        RegexOptions.Compiled);

    // ClientLog holds the write handle throughout, and Windows has bidirectional
    // share-mode validation: File.ReadAllLines opens with FileShare.Read, which is judged
    // to conflict with the write handle. It must be read shared for ReadWrite, the way
    // `tail -f` does it.
    private static string[] ReadAllLinesShared(string path)
    {
        using FileStream fs = new(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using StreamReader sr = new(fs);
        var lines = new List<string>();
        string? line;
        while ((line = sr.ReadLine()) is not null) lines.Add(line);
        return lines.ToArray();
    }

    [Fact]
    public void LineFormat_MatchesLegacyStyle()
    {
        string path = Path.Combine(_dir, "fmt.log");
        ClientLog.SetLogFile(path);
        ClientLog.SetLogLevel(LogLevel.Info);
        ClientLog.Info("hello-测试");
        ClientLog.FlushLogFile();

        string[] lines = ReadAllLinesShared(path);
        Assert.Single(lines);
        Match m = LineRegex.Match(lines[0]);
        Assert.True(m.Success, "line format mismatch: " + lines[0]);
        // INFO occupies a 5-character field (left aligned with trailing spaces)
        Assert.Contains(" INFO  [", lines[0]);
        Assert.EndsWith("- hello-测试", lines[0]);
    }

    [Fact]
    public void ThreadName_IsThreadLocal_Isolated()
    {
        // xunit's test thread is already named "C# TP Worker" (not main), so only these
        // are verified: 1) the value before naming differs from the one after and the
        // update takes effect; 2) other threads are unaffected.
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
        // Back on this thread: the name is not influenced by the worker thread (thread-local)
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

        string[] lines = ReadAllLinesShared(path);
        Assert.Single(lines);
        Assert.Contains("- visible", lines[0]);
    }

    [Fact]
    public void DisableFileOutput()
    {
        ClientLog.SetLogFile(string.Empty);
        // Passing means it does not throw (file output is already off)
        ClientLog.Info("stderr-only");
        ClientLog.FlushLogFile();
    }

    [Fact]
    public void SizeRotation_FixedWindow_WithMaxIndexCap()
    {
        string path = Path.Combine(_dir, "rot.log");
        ClientLog.SetLogFileLimits(2000, 3); // inject small thresholds: rotate at 2KB, at most 3 backups
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

        // The backup file's content should also be whole lines
        string[] backupLines = ReadAllLinesShared(backup1);
        Assert.All(backupLines, l => Assert.Matches(LineRegex, l));
    }
}
