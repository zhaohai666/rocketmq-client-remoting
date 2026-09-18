//! RPC 钩子（对应 `org.apache.rocketmq.remoting.RPCHook`）。
//!
//! [`AclClientRPCHook`] 的签名逐字节对齐 Java：
//! `AclClientRPCHook#doBeforeRequest` / `AclUtils#combineRequestContent` / `AclSigner#calSignature`。

use base64::Engine;
use hmac::{Hmac, Mac};
use sha1::Sha1;

use crate::remoting::protocol::remoting_command::RemotingCommand;

/// 对应 `org.apache.rocketmq.remoting.RPCHook`。
pub trait RPCHook: Send + Sync {
    fn do_before_request(&self, remote_addr: &str, request: &mut RemotingCommand);

    fn do_after_response(
        &self,
        remote_addr: &str,
        request: Option<&RemotingCommand>,
        response: Option<&RemotingCommand>,
    ) {
        let _ = (remote_addr, request, response);
    }
}

/// 对应 `org.apache.rocketmq.acl.common.SessionCredentials`。
#[derive(Debug, Clone, Default)]
pub struct SessionCredentials {
    pub access_key: String,
    pub secret_key: String,
    pub security_token: String,
}

impl SessionCredentials {
    pub const ACCESS_KEY: &'static str = "AccessKey";
    pub const SECRET_KEY: &'static str = "SecretKey";
    pub const SIGNATURE: &'static str = "Signature";
    pub const SECURITY_TOKEN: &'static str = "SecurityToken";

    pub fn new(access_key: &str, secret_key: &str) -> SessionCredentials {
        SessionCredentials {
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            security_token: String::new(),
        }
    }

    pub fn with_token(
        access_key: &str,
        secret_key: &str,
        security_token: &str,
    ) -> SessionCredentials {
        SessionCredentials {
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            security_token: security_token.to_string(),
        }
    }
}

/// 对应 `org.apache.rocketmq.acl.common.AclClientRPCHook`。
#[derive(Debug, Clone)]
pub struct AclClientRPCHook {
    pub credentials: SessionCredentials,
}

impl AclClientRPCHook {
    pub fn new(credentials: SessionCredentials) -> AclClientRPCHook {
        AclClientRPCHook { credentials }
    }

    /// 对应 Java `AclUtils.combineRequestContent`：先落 customHeader，再按 key 字典序
    /// 拼接全部 value（跳过 Signature 自身），最后拼 body 原始字节。
    pub fn build_request_content(request: &mut RemotingCommand) -> Vec<u8> {
        request.make_custom_header_to_net();
        let mut buf: Vec<u8> = Vec::new();
        for (key, value) in request.ext_fields().sorted() {
            if key == SessionCredentials::SIGNATURE {
                continue;
            }
            buf.extend_from_slice(value.as_bytes());
        }
        if let Some(body) = request.body() {
            buf.extend_from_slice(body);
        }
        buf
    }

    /// 对应 Java `AclSigner.calSignature`：Base64(HmacSHA1(secretKey, content))。
    pub fn calc_signature(secret_key: &str, request: &mut RemotingCommand) -> String {
        let content = Self::build_request_content(request);
        let mut mac = Hmac::<Sha1>::new_from_slice(secret_key.as_bytes())
            .expect("HMAC accepts any key length");
        mac.update(&content);
        base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
    }
}

impl RPCHook for AclClientRPCHook {
    /// 顺序必须与 Java 一致：AccessKey / SecurityToken 先写入（参与签名），
    /// Signature 最后写入（自身不参与签名）。
    fn do_before_request(&self, _remote_addr: &str, request: &mut RemotingCommand) {
        request.add_ext_field(SessionCredentials::ACCESS_KEY, &self.credentials.access_key);
        if !self.credentials.security_token.is_empty() {
            request.add_ext_field(
                SessionCredentials::SECURITY_TOKEN,
                &self.credentials.security_token,
            );
        }
        let signature = Self::calc_signature(&self.credentials.secret_key, request);
        request.add_ext_field(SessionCredentials::SIGNATURE, &signature);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command() -> RemotingCommand {
        let mut cmd = RemotingCommand::create_request_command(
            crate::remoting::protocol::codes::request_code::SEND_MESSAGE_V2,
            None,
        );
        cmd.ext_fields_mut().insert("producerGroup", "pg");
        cmd.ext_fields_mut().insert("topic", "T");
        cmd.ext_fields_mut().insert("defaultTopic", "TBW102");
        cmd.set_body(Some(b"hello body".to_vec()));
        cmd
    }

    #[test]
    fn content_sorts_keys_and_appends_body() {
        let content = AclClientRPCHook::build_request_content(&mut command());
        assert_eq!(
            content,
            b"TBW102pgThello body".to_vec(),
            "value 按 key 字典序（defaultTopic < producerGroup < topic）后接 body"
        );
    }

    #[test]
    fn signature_skips_signature_key() {
        let mut cmd = command();
        cmd.add_ext_field("Signature", "SHOULD_NOT_APPEAR");
        let content = AclClientRPCHook::build_request_content(&mut cmd);
        assert!(!String::from_utf8_lossy(&content).contains("SHOULD_NOT_APPEAR"));
    }

    #[test]
    fn signature_matches_known_hmac_vectors() {
        // openssl dgst -sha1 -hmac "key" -binary | base64
        let mut cmd = RemotingCommand::create_request_command(10, None);
        cmd.set_body(Some(b"hello".to_vec()));
        assert_eq!(
            AclClientRPCHook::calc_signature("key", &mut cmd),
            "s0zqxFFv8joUPmHXnQ+npPvl8mY="
        );
    }

    #[test]
    fn hook_injects_access_key_and_signature() {
        let hook = AclClientRPCHook::new(SessionCredentials::new("AK", "SK"));
        let mut cmd = command();
        hook.do_before_request("127.0.0.1:10911", &mut cmd);
        assert_eq!(cmd.get_ext_field("AccessKey"), Some("AK"));
        assert!(cmd.get_ext_field("Signature").is_some());
        assert_eq!(cmd.get_ext_field("SecurityToken"), None);

        let with_token = AclClientRPCHook::new(SessionCredentials::with_token("AK", "SK", "TOKEN"));
        let mut cmd2 = command();
        with_token.do_before_request("127.0.0.1:10911", &mut cmd2);
        assert_eq!(cmd2.get_ext_field("SecurityToken"), Some("TOKEN"));
        // Signature 不参与签名，所以两份内容的签名只对其它字段敏感
        assert_ne!(
            cmd.get_ext_field("Signature"),
            cmd2.get_ext_field("Signature")
        );
    }
}
