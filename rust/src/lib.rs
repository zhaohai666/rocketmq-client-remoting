//! Apache RocketMQ classic remoting protocol client (Rust).

pub mod client;
pub mod common;
pub mod error;
pub mod remoting;

pub use error::{Error, Result};
