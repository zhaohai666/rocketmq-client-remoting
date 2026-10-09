package common

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Client logging, configured from the environment:
//
//	ROCKETMQ_CLIENT_LOG_LEVEL         TRACE/DEBUG/INFO/WARN/ERROR (default INFO)
//	ROCKETMQ_CLIENT_LOG_DIR           log directory (default $HOME/logs/rocketmqlogs)
//	ROCKETMQ_CLIENT_LOG_FILE          file name (default rocketmq_go_client.log;
//	                                  '' / OFF / NONE disables the file sink; a value
//	                                  containing a path separator is used as a full path)
//	ROCKETMQ_CLIENT_LOG_USE_STDOUT    any non-empty value -> stdout instead of file
//	ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE    per-file cap in bytes (default 64MB, Java's
//	                                  logback <maxFileSize>64MB</maxFileSize>; 0 disables rotation)
//	ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX  backups kept (default 10, Java's
//	                                  rocketmq.log.file.maxIndex; 0 keeps none)
//
// Two deliberate choices, both shared with the C#/C++/Node/PHP ports:
//   - the file name is NOT Java's `rocketmq_client.log`: a JVM client on the same
//     machine would interleave its lines into the same file, and whichever side
//     rolls first renames the file out from under the other's open fd, after which
//     the other writes silently into an unlinked inode;
//   - rotation is SIZE based with a fixed backup window (`<file>.1` … `<file>.N`),
//     because an unbounded single log file in a long-lived producer is a disk-full
//     hazard. Backups are not gzipped (Java archives into other_days/*.gz).
type LogLevel int

const (
	LogTrace LogLevel = iota
	LogDebug
	LogInfo
	LogWarn
	LogError
)

func (l LogLevel) String() string {
	switch l {
	case LogTrace:
		return "TRACE"
	case LogDebug:
		return "DEBUG"
	case LogInfo:
		return "INFO"
	case LogWarn:
		return "WARN"
	case LogError:
		return "ERROR"
	}
	return "INFO"
}

const LoggerName = "rocketmq.client"

const (
	defaultLogFile     = "rocketmq_go_client.log"
	defaultLogMaxSize  = 64 * 1024 * 1024 // Java logback <maxFileSize>64MB</maxFileSize>
	defaultLogMaxIndex = 10               // Java rocketmq.log.file.maxIndex
)

var (
	logMu     sync.Mutex
	logLevel            = LogLevelFromName(envDefault("ROCKETMQ_CLIENT_LOG_LEVEL", "INFO"))
	logWriter io.Writer = os.Stderr
	logInit             = &sync.Once{}

	// File-sink state, resolved once by InitLogging.
	logFilePath string // "" = no file sink (disabled, stdout, or open failure)
	logSize     int64
	logMaxSize  = int64(envIntDefault("ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE", defaultLogMaxSize))
	logMaxIndex = envIntDefault("ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX", defaultLogMaxIndex)
)

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envIntDefault reads a non-negative integer knob; a bad or negative value
// falls back to the default instead of disabling rotation by accident.
func envIntDefault(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return def
	}
	return n
}

// InitLogging opens the log sink on first use (stdout when requested, else the
// log file); failures fall back to stderr silently.
func InitLogging() {
	logInit.Do(func() {
		if os.Getenv("ROCKETMQ_CLIENT_LOG_USE_STDOUT") != "" {
			logWriter = os.Stdout
			return
		}
		// LookupEnv (not envDefault) so an **explicitly empty** value disables the
		// file sink — "" / OFF / NONE is the cross-port rule; only an *unset*
		// variable falls back to the default name.
		name, nameSet := os.LookupEnv("ROCKETMQ_CLIENT_LOG_FILE")
		if !nameSet {
			name = defaultLogFile
		}
		switch strings.ToUpper(name) {
		case "", "OFF", "NONE":
			return // file sink disabled: stderr only
		}
		path := name
		if !strings.ContainsRune(name, filepath.Separator) {
			dir := envDefault("ROCKETMQ_CLIENT_LOG_DIR", filepath.Join(UserHome(), "logs", "rocketmqlogs"))
			if dir == "" {
				return
			}
			path = filepath.Join(dir, name)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return
		}
		if st, statErr := f.Stat(); statErr == nil {
			logSize = st.Size()
		}
		logFilePath = path
		logWriter = f
	})
}

// rotateLocked rolls the current file into the fixed backup window
// (csharp ClientLog.RollLocked / nodeJs roll): delete <file>.N, shift .N-1→.N …
// .1→.2, then base→.1 and reopen. Callers hold logMu.
func rotateLocked() {
	f, ok := logWriter.(*os.File)
	if logFilePath == "" || !ok {
		return
	}
	_ = f.Close()
	if logMaxIndex > 0 {
		_ = os.Remove(fmt.Sprintf("%s.%d", logFilePath, logMaxIndex))
		for i := logMaxIndex - 1; i >= 1; i-- {
			_ = os.Rename(fmt.Sprintf("%s.%d", logFilePath, i), fmt.Sprintf("%s.%d", logFilePath, i+1))
		}
	}
	renamed := false
	if logMaxIndex > 0 {
		renamed = os.Rename(logFilePath, logFilePath+".1") == nil
	} else {
		// No backups requested: truncate the file in place instead of rolling it.
		renamed = os.Truncate(logFilePath, 0) == nil
	}
	if !renamed {
		// Roll-over failed (permissions, file vanished): keep appending to the
		// same file rather than losing the log entirely.
		if nf, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
			logWriter = nf
			if st, statErr := nf.Stat(); statErr == nil {
				logSize = st.Size()
			}
		} else {
			logWriter = os.Stderr
			logSize = 0
		}
		return
	}
	nf, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		logWriter = os.Stderr
		logSize = 0
		return
	}
	logWriter = nf
	logSize = 0
}

// LogFilePath returns the file the client logs to, or "" when the file sink is
// off. Exposed for diagnostics and the live scripts' "did it write a file" check.
func LogFilePath() string {
	InitLogging()
	logMu.Lock()
	defer logMu.Unlock()
	return logFilePath
}

func LogLevelFromName(name string) LogLevel {
	switch strings.ToUpper(name) {
	case "TRACE":
		return LogTrace
	case "DEBUG":
		return LogDebug
	case "WARN", "WARNING":
		return LogWarn
	case "ERROR":
		return LogError
	default:
		return LogInfo
	}
}

func SetLogLevel(level LogLevel) { logLevel = level }

func IsLogEnabled(level LogLevel) bool { return level >= logLevel }

func Log(level LogLevel, format string, args ...any) {
	if !IsLogEnabled(level) {
		return
	}
	InitLogging()
	line := fmt.Sprintf("%s %s %s\n",
		time.Now().Format("2006-01-02 15:04:05.000"), level, fmt.Sprintf(format, args...))
	logMu.Lock()
	defer logMu.Unlock()
	if logFilePath != "" && logMaxSize > 0 && logSize+int64(len(line)) > logMaxSize {
		rotateLocked()
	}
	n, err := fmt.Fprint(logWriter, line)
	if err != nil && logFilePath != "" {
		// A rotated-away inode (external `rm`, backup-window race) makes every later
		// write fail silently; reopen once so the log does not vanish.
		if f, ok := logWriter.(*os.File); ok {
			_ = f.Close()
		}
		logSize = 0
		if nf, openErr := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); openErr == nil {
			logWriter = nf
			n, _ = fmt.Fprint(logWriter, line)
		}
		return
	}
	logSize += int64(n)
}

func LogTracef(format string, args ...any) { Log(LogTrace, format, args...) }
func LogDebugf(format string, args ...any) { Log(LogDebug, format, args...) }
func LogInfof(format string, args ...any)  { Log(LogInfo, format, args...) }
func LogWarnf(format string, args ...any)  { Log(LogWarn, format, args...) }
func LogErrorf(format string, args ...any) { Log(LogError, format, args...) }
