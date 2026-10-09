package common

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// 日志落盘与轮转的离线单测。
//
// 为什么必须单测：Java 客户端会生成 rocketmq_client.log，本仓库的对齐口径是「每个端都
// 要真的写出文件、并且按大小轮转」。这两条在真机脚本里最多验证「有没有文件」，轮转窗口
// 要写满 64MB 才看得见 —— 只能离线用小上限造出来。
//
// 三条判据：
//  1. 默认文件名是本端专属的 rocketmq_go_client.log，**不是** Java 的 rocketmq_client.log
//     （同机跑 Java 客户端时两边会互相插行、并把对方的文件轮走，之后各自写进已 unlink 的 inode）；
//  2. OFF / NONE / 空串关掉文件落盘（只剩 stderr），USE_STDOUT 优先；
//  3. 到大小就轮转，备份是固定窗口 <file>.1 … <file>.N，最老的一份被丢掉。

// resetSink 把包级日志状态恢复成「还没开过文件」，并让环境变量重新生效。
// 同包测试可以直接改这些内部状态，无需为此导出任何测试专用 API。
func resetSink(t *testing.T, dir, name string, maxSize, maxIndex int) string {
	t.Helper()
	logMu.Lock()
	defer logMu.Unlock()
	if f, ok := logWriter.(*os.File); ok {
		_ = f.Close()
	}
	logWriter = os.Stderr
	logFilePath = ""
	logSize = 0
	logInit = &sync.Once{}
	if dir != "" {
		t.Setenv("ROCKETMQ_CLIENT_LOG_DIR", dir)
	} else {
		t.Setenv("ROCKETMQ_CLIENT_LOG_DIR", "")
		os.Unsetenv("ROCKETMQ_CLIENT_LOG_DIR")
	}
	// t.Setenv 而非 os.Setenv：清空也要保持「变量存在但取值为空」这一状态，
	// 端口间的约定是空串 = 关闭文件落盘，与「未设置 = 用默认文件名」是两回事。
	t.Setenv("ROCKETMQ_CLIENT_LOG_FILE", name)
	os.Unsetenv("ROCKETMQ_CLIENT_LOG_USE_STDOUT")
	t.Setenv("ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE", fmt.Sprintf("%d", maxSize))
	t.Setenv("ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX", fmt.Sprintf("%d", maxIndex))
	// 包级 var 在进程启动时就读过一次环境变量，这里按同一口径重算。
	logMaxSize = int64(maxSize)
	logMaxIndex = maxIndex
	return filepath.Join(dir, defaultLogFile)
}

func TestLogDefaultsToPortSpecificFileName(t *testing.T) {
	dir := t.TempDir()
	resetSink(t, dir, "anything", 0, 0)
	// 「未设置」与「设为空串」是两种状态：只有前者才落到本端默认文件名
	os.Unsetenv("ROCKETMQ_CLIENT_LOG_FILE")

	LogInfof("hello %s", "default")

	got := LogFilePath()
	if filepath.Base(got) != defaultLogFile {
		t.Fatalf("default log file = %q, want %q", got, defaultLogFile)
	}
	if filepath.Base(got) == "rocketmq_client.log" {
		t.Fatal("must not share Java's rocketmq_client.log")
	}
	data, err := os.ReadFile(got)
	if err != nil {
		t.Fatalf("log file not written: %v", err)
	}
	if !strings.Contains(string(data), "hello default") {
		t.Fatalf("log line missing from %q: %q", got, data)
	}
	// 行格式：时间戳 + 级别 + 消息（与其余端口同一口径）
	if !strings.Contains(string(data), " INFO ") {
		t.Fatalf("level token missing: %q", data)
	}
}

func TestLogFileDisabledValues(t *testing.T) {
	for _, name := range []string{"", "OFF", "NONE", "off"} {
		dir := t.TempDir()
		resetSink(t, dir, name, 0, 0)
		LogInfof("should-not-land-in-a-file")
		if got := LogFilePath(); got != "" {
			t.Fatalf("name=%q: file sink still active at %q", name, got)
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("name=%q: unexpected files %v", name, entries)
		}
	}
}

func TestLogUseStdoutWinsOverFile(t *testing.T) {
	dir := t.TempDir()
	resetSink(t, dir, "client.log", 0, 0)
	t.Setenv("ROCKETMQ_CLIENT_LOG_USE_STDOUT", "1")

	LogInfof("stdout only")

	if got := LogFilePath(); got != "" {
		t.Fatalf("USE_STDOUT must skip the file sink, got %q", got)
	}
}

func TestLogFilePathWithSeparatorIsFullPath(t *testing.T) {
	dir := t.TempDir()
	// 带路径分隔符的 ROCKETMQ_CLIENT_LOG_FILE 当整路径处理（同 php/csharp 口径），
	// 否则用户没法把日志放到 LOG_DIR 之外。
	full := filepath.Join(dir, "nested", "deep", "rmq.log")
	resetSink(t, dir, full, 0, 0)

	LogInfof("full path")

	if LogFilePath() != full {
		t.Fatalf("LogFilePath = %q, want %q", LogFilePath(), full)
	}
	if _, err := os.ReadFile(full); err != nil {
		t.Fatalf("nested path not created: %v", err)
	}
}

func TestLogRotatesWithFixedBackupWindow(t *testing.T) {
	dir := t.TempDir()
	// 每行约 40B，上限 100B ⇒ 约每 2~3 行轮一次
	resetSink(t, dir, "rotate.log", 100, 3)
	path := filepath.Join(dir, "rotate.log")

	for i := 0; i < 12; i++ {
		LogInfof("line-%02d-padding-padding", i)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("current log file must exist after rotation: %v", err)
	}
	for i := 1; i <= 3; i++ {
		if _, err := os.Stat(fmt.Sprintf("%s.%d", path, i)); err != nil {
			t.Fatalf("backup .%d missing: %v", i, err)
		}
	}
	if _, err := os.Stat(path + ".4"); !os.IsNotExist(err) {
		t.Fatalf("window must drop the oldest backup, .4 exists")
	}

	// 窗口里最新的一份（.1）必须比当前文件「老」：当前文件只剩最后几行
	cur, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(path + ".1")
	if err != nil {
		t.Fatal(err)
	}
	if len(first) < len(cur) {
		t.Fatalf("backup .1 (%d B) should hold more history than the current file (%d B)", len(first), len(cur))
	}
}

func TestLogMaxIndexZeroTruncatesInPlace(t *testing.T) {
	dir := t.TempDir()
	resetSink(t, dir, "trunc.log", 60, 0)
	path := filepath.Join(dir, "trunc.log")

	for i := 0; i < 6; i++ {
		LogInfof("line-%d-padding-padding", i)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "trunc.log.") {
			t.Fatalf("MAX_INDEX=0 must not keep backups, found %q", e.Name())
		}
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// 文件仍在写，但大小被压回上限附近（原地截断）
	if st.Size() > 200 {
		t.Fatalf("file grew past the cap without rotation: %d B", st.Size())
	}
}

func TestLogSizeIsResumedFromExistingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "resume.log")
	seed := strings.Repeat("x", 90) + "\n"
	if err := os.WriteFile(path, []byte(seed), 0o644); err != nil {
		t.Fatal(err)
	}
	resetSink(t, dir, "resume.log", 100, 2)

	LogInfof("append")

	if _, err := os.Stat(path + ".1"); err != nil {
		t.Fatalf("pre-existing 90B must count toward the cap so the next line rolls the file: %v", err)
	}
}

func TestEnvIntFallsBackOnBadValue(t *testing.T) {
	t.Setenv("ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE", "-5")
	if got := envIntDefault("ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE", defaultLogMaxSize); got != defaultLogMaxSize {
		t.Fatalf("negative value must fall back to the default, got %d", got)
	}
	t.Setenv("ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE", "abc")
	if got := envIntDefault("ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE", defaultLogMaxSize); got != defaultLogMaxSize {
		t.Fatalf("unparsable value must fall back to the default, got %d", got)
	}
	t.Setenv("ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE", "0")
	if got := envIntDefault("ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE", defaultLogMaxSize); got != 0 {
		t.Fatalf("0 is a legal 'no rotation' value, got %d", got)
	}
}
