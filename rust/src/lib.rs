//! Apache RocketMQ 经典 remoting 协议客户端（Rust）。
//!
//! 分层与 Java / 本仓库 Python 参考实现一致：
//! - [`remoting`]：线协议（帧编解码、header、序列化）与长连接传输
//! - [`common`]：消息模型、17 段存储格式编解码、命名空间、常量
//! - [`client`]：`MQClientInstance`、Producer、Consumer、Admin

pub mod client;
pub mod common;
pub mod error;
pub mod remoting;

pub use error::{Error, Result};
