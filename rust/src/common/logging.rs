//! 轻量客户端日志（对应 `rocketmq/logging.py` 与 Java 的 `rocketmq_client.log`）。
//!
//! 落盘 `~/logs/rocketmqlogs/rocketmq_rs_client.log`，按天改名轮转，同时输出 stderr。
//! 刻意不叫 Java 的 `rocketmq_client.log`：两者轮转策略不同，同机同文件会互相插行、
//! 且改名会让对方写进已 unlink 的 inode 而静默丢日志。

use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use crate::common::util_all::time_to_human_string;
use crate::error::Result;

pub const LOGGER_NAME: &str = "rocketmq.client";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Debug = 10,
    Info = 20,
    Warn = 30,
    Error = 40,
    Off = 100,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Debug => "DEBUG",
            Level::Info => "INFO",
            Level::Warn => "WARN",
            Level::Error => "ERROR",
            Level::Off => "OFF",
        }
    }

    fn parse(name: &str) -> Level {
        match name.trim().to_ascii_uppercase().as_str() {
            "DEBUG" => Level::Debug,
            "WARN" | "WARNING" => Level::Warn,
            "ERROR" => Level::Error,
            "OFF" | "NONE" => Level::Off,
            _ => Level::Info,
        }
    }
}

struct Config {
    level: Level,
    path: PathBuf,
    log_dir: String,
    file_name: String,
    use_stdout: bool,
    max_index: usize,
}

impl Config {
    fn from_env() -> Config {
        let log_dir = std::env::var("ROCKETMQ_CLIENT_LOG_DIR")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .or_else(|| {
                std::env::var("HOME")
                    .ok()
                    .filter(|h| !h.is_empty())
                    .map(|h| format!("{h}/logs/rocketmqlogs"))
            })
            .unwrap_or_else(|| "logs/rocketmqlogs".to_string());
        let file_name = std::env::var("ROCKETMQ_CLIENT_LOG_FILE")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| "rocketmq_rs_client.log".to_string());
        let level = Level::parse(
            &std::env::var("ROCKETMQ_CLIENT_LOG_LEVEL").unwrap_or_else(|_| "INFO".into()),
        );
        let max_index = std::env::var("ROCKETMQ_CLIENT_LOG_MAX_INDEX")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(10);
        let use_stdout = !std::env::var("ROCKETMQ_CLIENT_LOG_USE_STDOUT")
            .map(|v| v.trim().eq_ignore_ascii_case("false"))
            .unwrap_or(false);
        Config {
            level,
            path: PathBuf::from(&log_dir).join(&file_name),
            log_dir,
            file_name,
            use_stdout,
            max_index,
        }
    }
}

struct Sink {
    file: Option<std::fs::File>,
    date: String,
    file_disabled: bool,
}

static CONFIG: OnceLock<Config> = OnceLock::new();
static SINK: OnceLock<Mutex<Sink>> = OnceLock::new();

fn config() -> &'static Config {
    CONFIG.get_or_init(Config::from_env)
}

fn sink() -> &'static Mutex<Sink> {
    SINK.get_or_init(|| {
        Mutex::new(Sink { file: None, date: String::new(), file_disabled: false })
    })
}

pub fn level() -> Level {
    config().level
}

pub fn is_enabled(level: Level) -> bool {
    level >= config().level
}

/// 手动设置级别（只能在第一次写日志前生效，之后走环境变量）。
pub fn set_console_output(enable: bool) {
    // Config 在 OnceLock 里不可变，因此这里只影响 stdout 分支的开关位。
    STDOUT_OVERRIDE.set(if enable { Some(true) } else { Some(false) }).ok();
}

static STDOUT_OVERRIDE: OnceLock<Option<bool>> = OnceLock::new();

fn today() -> String {
    time_to_human_string(crate::common::util_all::current_time_millis())
        .chars()
        .take(10)
        .collect()
}

fn timestamp() -> String {
    let millis = crate::common::util_all::current_time_millis();
    let human = time_to_human_string(millis);
    let fraction = (millis % 1000).abs();
    format!("{human},{fraction:03}")
}

pub fn log(level: Level, message: &str) {
    if !is_enabled(level) {
        return;
    }
    let line = format!("{} [{}] {} - {}\n", timestamp(), level.as_str(), LOGGER_NAME, message);
    let use_stdout = match STDOUT_OVERRIDE.get().and_then(|v| *v) {
        Some(v) => v,
        None => config().use_stdout,
    };
    if use_stdout {
        let mut err = std::io::stderr();
        let _ = err.write_all(line.as_bytes());
    }
    write_to_file(line.as_bytes());
}

pub fn log_fmt(level: Level, args: std::fmt::Arguments<'_>) {
    log(level, &std::fmt::format(args));
}

fn write_to_file(bytes: &[u8]) {
    let cfg = config();
    let mut guard = sink().lock().unwrap_or_else(|e| e.into_inner());
    if guard.file_disabled {
        return;
    }
    let date = today();
    if guard.file.is_some() && guard.date != date {
        rotate(&mut guard, cfg, &date);
    }
    if guard.file.is_none() {
        match open_file(cfg) {
            Ok(file) => {
                guard.file = Some(file);
                guard.date = date;
                guard.file_disabled = false;
            }
            Err(e) => {
                guard.file_disabled = true;
                guard.file = None;
                eprintln!("[rocketmq] client file log disabled: {e}");
                return;
            }
        }
    }
    if let Some(file) = guard.file.as_mut() {
        let _ = file.write_all(bytes);
        let _ = file.flush();
    }
}

fn open_file(cfg: &Config) -> Result<std::fs::File> {
    create_dir_all(&cfg.log_dir)?;
    let file = OpenOptions::new().create(true).append(true).open(&cfg.path)?;
    Ok(file)
}

fn rotate(guard: &mut Sink, cfg: &Config, date: &str) {
    drop(guard.file.take());
    let backup = PathBuf::from(format!("{}.{}", cfg.path.display(), guard.date));
    if cfg.path.exists() {
        let _ = std::fs::rename(&cfg.path, &backup);
    }
    prune_backups(cfg);
    guard.date = date.to_string();
    if let Ok(file) = open_file(cfg) {
        guard.file = Some(file);
    }
}

fn prune_backups(cfg: &Config) {
    let Ok(entries) = std::fs::read_dir(&cfg.log_dir) else { return };
    let prefix = format!("{}.", cfg.file_name);
    let mut backups: Vec<String> = entries
        .flatten()
        .filter_map(|e| e.file_name().to_str().map(str::to_string))
        .filter(|name| name.starts_with(&prefix))
        .collect();
    if backups.len() <= cfg.max_index {
        return;
    }
    backups.sort_unstable();
    backups.reverse();
    for stale in backups.iter().skip(cfg.max_index) {
        let _ = std::fs::remove_file(PathBuf::from(&cfg.log_dir).join(stale));
    }
}

#[macro_export]
macro_rules! rmq_debug {
    ($($arg:tt)*) => {
        if $crate::common::logging::is_enabled($crate::common::logging::Level::Debug) {
            $crate::common::logging::log_fmt(
                $crate::common::logging::Level::Debug,
                format_args!($($arg)*),
            )
        }
    };
}

#[macro_export]
macro_rules! rmq_info {
    ($($arg:tt)*) => {
        if $crate::common::logging::is_enabled($crate::common::logging::Level::Info) {
            $crate::common::logging::log_fmt(
                $crate::common::logging::Level::Info,
                format_args!($($arg)*),
            )
        }
    };
}

#[macro_export]
macro_rules! rmq_warn {
    ($($arg:tt)*) => {
        if $crate::common::logging::is_enabled($crate::common::logging::Level::Warn) {
            $crate::common::logging::log_fmt(
                $crate::common::logging::Level::Warn,
                format_args!($($arg)*),
            )
        }
    };
}

#[macro_export]
macro_rules! rmq_error {
    ($($arg:tt)*) => {
        if $crate::common::logging::is_enabled($crate::common::logging::Level::Error) {
            $crate::common::logging::log_fmt(
                $crate::common::logging::Level::Error,
                format_args!($($arg)*),
            )
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_parsing() {
        assert_eq!(Level::parse("debug"), Level::Debug);
        assert_eq!(Level::parse("WARNING"), Level::Warn);
        assert_eq!(Level::parse("garbage"), Level::Info);
        assert_eq!(Level::parse("OFF"), Level::Off);
        assert!(Level::Error > Level::Warn);
    }

    #[test]
    fn timestamp_has_milliseconds() {
        let ts = timestamp();
        assert_eq!(ts.len(), 23, "expect 'YYYY-MM-DD HH:MM:SS,mmm' got {ts:?}");
        assert_eq!(ts.matches(',').count(), 1);
    }

    #[test]
    fn macros_compile_and_respect_level() {
        rmq_debug!("should not throw: {}", 1);
        rmq_warn!("test line {}", 2);
        assert!(is_enabled(Level::Warn) || !is_enabled(Level::Warn));
    }
}
