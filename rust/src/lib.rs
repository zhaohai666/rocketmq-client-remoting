//! Apache RocketMQ 经典 remoting 协议客户端（Rust）。

pub mod client;
pub mod common;
pub mod error;
pub mod remoting;

pub use error::{Error, Result};
