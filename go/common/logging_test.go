package common

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// Offline unit tests for log persistence and rotation.
//
// Why they must be unit-tested: the file must actually be written and rotated by
// size. A live script can at most verify "a file exists"; the rotation window
// only becomes visible after filling 64MB — so it can only be reproduced offline
// with a small cap.
//
// Three criteria:
//  1. The default file name is this-client-specific: rocketmq_go_client.log, and
//     does **not** use the generic rocketmq_client.log (two client processes on
//     the same machine sharing one file would interleave their lines and rotate
//     each other's file away, after which each writes into an already-unlinked
//     inode);
//  2. OFF / NONE / empty string turn off file persistence (only stderr remains),
//     with USE_STDOUT taking precedence;
//  3. rotation happens at the size cap; backups use a fixed window <file>.1 …
//     <file>.N, dropping the oldest.

// resetSink restores the package-level logging state to "never opened a file"
// and makes the environment variables take effect again.
// Tests in the same package can mutate these internal states directly, without
// exporting any test-only API for it.
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
	// t.Setenv instead of os.Setenv: clearing must also keep the "variable
	// exists but is empty" state, because the cross-port contract is that an
	// empty string = file persistence off, which differs from "unset = use the
	// default file name".
	t.Setenv("ROCKETMQ_CLIENT_LOG_FILE", name)
	os.Unsetenv("ROCKETMQ_CLIENT_LOG_USE_STDOUT")
	t.Setenv("ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE", fmt.Sprintf("%d", maxSize))
	t.Setenv("ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX", fmt.Sprintf("%d", maxIndex))
	// Package-level vars are read once at process start; recompute them on the same basis here.
	logMaxSize = int64(maxSize)
	logMaxIndex = maxIndex
	return filepath.Join(dir, defaultLogFile)
}

func TestLogDefaultsToPortSpecificFileName(t *testing.T) {
	dir := t.TempDir()
	resetSink(t, dir, "anything", 0, 0)
	// "unset" and "set to empty string" are two different states: only the former falls back to the client-specific default file name
	os.Unsetenv("ROCKETMQ_CLIENT_LOG_FILE")

	LogInfof("hello %s", "default")

	got := LogFilePath()
	if filepath.Base(got) != defaultLogFile {
		t.Fatalf("default log file = %q, want %q", got, defaultLogFile)
	}
	if filepath.Base(got) == "rocketmq_client.log" {
		t.Fatal("must not share the language-neutral rocketmq_client.log")
	}
	data, err := os.ReadFile(got)
	if err != nil {
		t.Fatalf("log file not written: %v", err)
	}
	if !strings.Contains(string(data), "hello default") {
		t.Fatalf("log line missing from %q: %q", got, data)
	}
	// Line format: timestamp + level + message
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
	// A ROCKETMQ_CLIENT_LOG_FILE containing a path separator is treated as a full
	// path, otherwise the user cannot place the log outside LOG_DIR.
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
	// ~40B per line and a 100B cap ⇒ a rotation roughly every 2~3 lines
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

	// The newest in the window (.1) must be "older" than the current file: the current file holds only the last few lines
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
	// The file is still being written, but its size is clamped near the cap (truncated in place)
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
