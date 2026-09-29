package common

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Client logging, configured from the environment:
//
//	ROCKETMQ_CLIENT_LOG_LEVEL      TRACE/DEBUG/INFO/WARN/ERROR (default INFO)
//	ROCKETMQ_CLIENT_LOG_DIR        log directory (default $HOME/logs/rocketmqlogs)
//	ROCKETMQ_CLIENT_LOG_FILE       file name (default rocketmq_client.log)
//	ROCKETMQ_CLIENT_LOG_USE_STDOUT any non-empty value -> stdout instead of file

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

var (
	logMu     sync.Mutex
	logLevel  = LogLevelFromName(envDefault("ROCKETMQ_CLIENT_LOG_LEVEL", "INFO"))
	logWriter = os.Stderr
	logInit   sync.Once
)

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// InitLogging opens the log sink on first use (stdout when requested, else the
// log file); failures fall back to stderr silently.
func InitLogging() {
	logInit.Do(func() {
		if os.Getenv("ROCKETMQ_CLIENT_LOG_USE_STDOUT") != "" {
			logWriter = os.Stdout
			return
		}
		dir := envDefault("ROCKETMQ_CLIENT_LOG_DIR", filepath.Join(UserHome(), "logs", "rocketmqlogs"))
		if dir == "" {
			return
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return
		}
		f, err := os.OpenFile(filepath.Join(dir, envDefault("ROCKETMQ_CLIENT_LOG_FILE", "rocketmq_client.log")),
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return
		}
		logWriter = f
	})
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
	logMu.Lock()
	defer logMu.Unlock()
	fmt.Fprintf(logWriter, "%s %s %s\n",
		time.Now().Format("2006-01-02 15:04:05.000"), level, fmt.Sprintf(format, args...))
}

func LogTracef(format string, args ...any) { Log(LogTrace, format, args...) }
func LogDebugf(format string, args ...any) { Log(LogDebug, format, args...) }
func LogInfof(format string, args ...any)  { Log(LogInfo, format, args...) }
func LogWarnf(format string, args ...any)  { Log(LogWarn, format, args...) }
func LogErrorf(format string, args ...any) { Log(LogError, format, args...) }
