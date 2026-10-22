//! Auth user / ACL management payloads: the JSON bodies and responses of requests 3001-3010.
//!
//! An unset field is **omitted** rather than written as `null`: absence means "not specified"
//! for the broker, `null` means "clear this value", and the two are not interchangeable.
//! Decoding is the tolerant direction — every optional key may be missing from a response.

use serde_json::Value;

use super::admin_body::{expect_object, jarray, json_object, jstring};
use super::serialize::RemotingSerializable;
use crate::error::{Error, Result};

// The wire spelling of each enumeration, case included. The four families do not share a case
// convention, so these literals are part of the contract and must stay byte-exact.

/// `userType` of a superuser.
pub const USER_TYPE_SUPER: &str = "Super";
/// `userType` of an ordinary user.
pub const USER_TYPE_NORMAL: &str = "Normal";
/// `userStatus` of an enabled user.
pub const USER_STATUS_ENABLE: &str = "enable";
/// `userStatus` of a disabled user.
pub const USER_STATUS_DISABLE: &str = "disable";
/// `policyType` of a user-defined policy.
pub const POLICY_TYPE_CUSTOM: &str = "Custom";
/// `policyType` of a broker-side built-in policy.
pub const POLICY_TYPE_DEFAULT: &str = "Default";
/// `decision` granting the listed actions.
pub const DECISION_ALLOW: &str = "Allow";
/// `decision` refusing the listed actions.
pub const DECISION_DENY: &str = "Deny";

/// A user record: the body of 3001/3002 and the response of 3004/3005.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthUser {
    pub username: String,
    pub password: Option<String>,
    pub user_type: Option<String>,
    pub user_status: Option<String>,
}

impl AuthUser {
    pub fn new(username: impl Into<String>) -> AuthUser {
        AuthUser {
            username: username.into(),
            password: None,
            user_type: None,
            user_status: None,
        }
    }

    pub fn to_json_value(&self) -> Value {
        let mut pairs: Vec<(&'static str, Value)> =
            vec![("username", Value::String(self.username.clone()))];
        push_string(&mut pairs, "password", self.password.as_deref());
        push_string(&mut pairs, "userType", self.user_type.as_deref());
        push_string(&mut pairs, "userStatus", self.user_status.as_deref());
        json_object(pairs)
    }

    pub fn from_json_value(value: &Value) -> Result<AuthUser> {
        expect_object(value, "AuthUser")?;
        Ok(AuthUser {
            username: jstring(value, "username").unwrap_or_default(),
            password: jstring(value, "password"),
            user_type: jstring(value, "userType"),
            user_status: jstring(value, "userStatus"),
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<AuthUser> {
        AuthUser::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

/// One grant inside a policy: the resource, the actions on it, where the request must come from,
/// and whether that combination is allowed or refused.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AclEntry {
    pub resource: String,
    pub actions: Vec<String>,
    pub source_ips: Vec<String>,
    pub decision: Option<String>,
}

impl AclEntry {
    pub fn to_json_value(&self) -> Value {
        let mut pairs: Vec<(&'static str, Value)> = vec![
            ("resource", Value::String(self.resource.clone())),
            ("actions", string_array(&self.actions)),
            ("sourceIps", string_array(&self.source_ips)),
        ];
        push_string(&mut pairs, "decision", self.decision.as_deref());
        json_object(pairs)
    }

    pub fn from_json_value(value: &Value) -> Result<AclEntry> {
        expect_object(value, "AclEntry")?;
        Ok(AclEntry {
            resource: jstring(value, "resource").unwrap_or_default(),
            actions: string_list(value, "actions")?,
            source_ips: string_list(value, "sourceIps")?,
            decision: jstring(value, "decision"),
        })
    }
}

/// A group of entries. `policy_type` is `None` for a policy assembled from per-resource grants,
/// and that key is then absent from the JSON.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AclPolicy {
    pub policy_type: Option<String>,
    pub entries: Vec<AclEntry>,
}

impl AclPolicy {
    pub fn to_json_value(&self) -> Value {
        let mut pairs: Vec<(&'static str, Value)> = Vec::new();
        push_string(&mut pairs, "policyType", self.policy_type.as_deref());
        pairs.push((
            "entries",
            Value::Array(self.entries.iter().map(|e| e.to_json_value()).collect()),
        ));
        json_object(pairs)
    }

    pub fn from_json_value(value: &Value) -> Result<AclPolicy> {
        expect_object(value, "AclPolicy")?;
        Ok(AclPolicy {
            policy_type: jstring(value, "policyType"),
            entries: jarray(value, "entries")?
                .iter()
                .map(|item| AclEntry::from_json_value(item))
                .collect::<Result<Vec<AclEntry>>>()?,
        })
    }
}

/// An ACL record: the body of 3006/3007 and the response of 3009/3010.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Acl {
    pub subject: String,
    pub policies: Vec<AclPolicy>,
}

impl Acl {
    pub fn new(subject: impl Into<String>) -> Acl {
        Acl {
            subject: subject.into(),
            policies: Vec::new(),
        }
    }

    /// Assembles the shape the per-resource convenience path sends: one policy carrying no
    /// `policyType`, and one entry per resource repeating the same actions / sourceIps / decision.
    pub fn for_resources(
        subject: impl Into<String>,
        resources: &[&str],
        actions: &[&str],
        source_ips: &[&str],
        decision: &str,
    ) -> Acl {
        let actions = owned(actions);
        let source_ips = owned(source_ips);
        let entries = resources
            .iter()
            .map(|resource| AclEntry {
                resource: (*resource).to_string(),
                actions: actions.clone(),
                source_ips: source_ips.clone(),
                decision: Some(decision.to_string()),
            })
            .collect();
        Acl {
            subject: subject.into(),
            policies: vec![AclPolicy {
                policy_type: None,
                entries,
            }],
        }
    }

    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            ("subject", Value::String(self.subject.clone())),
            (
                "policies",
                Value::Array(self.policies.iter().map(|p| p.to_json_value()).collect()),
            ),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<Acl> {
        expect_object(value, "Acl")?;
        Ok(Acl {
            subject: jstring(value, "subject").unwrap_or_default(),
            policies: jarray(value, "policies")?
                .iter()
                .map(|item| AclPolicy::from_json_value(item))
                .collect::<Result<Vec<AclPolicy>>>()?,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<Acl> {
        Acl::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

// ---------------------------------------------------------------- response bodies

/// A missing, empty or `null` body all mean "no such record".
pub fn user_from_body(body: Option<&[u8]>) -> Result<Option<AuthUser>> {
    match value_from_body(body)? {
        None => Ok(None),
        Some(value) => Ok(Some(AuthUser::from_json_value(&value)?)),
    }
}

/// A missing, empty or `null` list body all mean "no record at all".
pub fn users_from_body(body: Option<&[u8]>) -> Result<Vec<AuthUser>> {
    let value = match value_from_body(body)? {
        None => return Ok(Vec::new()),
        Some(value) => value,
    };
    array_items(&value, "user list")?
        .iter()
        .map(AuthUser::from_json_value)
        .collect::<Result<Vec<AuthUser>>>()
}

pub fn acl_from_body(body: Option<&[u8]>) -> Result<Option<Acl>> {
    match value_from_body(body)? {
        None => Ok(None),
        Some(value) => Ok(Some(Acl::from_json_value(&value)?)),
    }
}

pub fn acls_from_body(body: Option<&[u8]>) -> Result<Vec<Acl>> {
    let value = match value_from_body(body)? {
        None => return Ok(Vec::new()),
        Some(value) => value,
    };
    array_items(&value, "acl list")?
        .iter()
        .map(Acl::from_json_value)
        .collect::<Result<Vec<Acl>>>()
}

fn array_items<'a>(value: &'a Value, what: &str) -> Result<&'a [Value]> {
    value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| Error::Decode(format!("{what} body is not a json array")))
}

fn value_from_body(body: Option<&[u8]>) -> Result<Option<Value>> {
    let data = match body {
        Some(b) if !b.is_empty() => b,
        _ => return Ok(None),
    };
    let value = RemotingSerializable::decode(data)?;
    if value.is_null() {
        return Ok(None);
    }
    Ok(Some(value))
}

// ---------------------------------------------------------------- json field helpers

/// Appends a string field only when set; `None` leaves the key out entirely.
fn push_string(pairs: &mut Vec<(&'static str, Value)>, key: &'static str, value: Option<&str>) {
    if let Some(v) = value {
        pairs.push((key, Value::String(v.to_string())));
    }
}

fn string_array(items: &[String]) -> Value {
    Value::Array(items.iter().map(|i| Value::String(i.clone())).collect())
}

fn owned(items: &[&str]) -> Vec<String> {
    items.iter().map(|i| (*i).to_string()).collect()
}

/// Element list of string values: non-strings are skipped rather than fatal, as elsewhere here.
fn string_list(value: &Value, key: &str) -> Result<Vec<String>> {
    Ok(jarray(value, key)?
        .into_iter()
        .filter_map(|item| item.as_str().map(str::to_string))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(json: &str) -> Vec<u8> {
        json.as_bytes().to_vec()
    }

    #[test]
    fn enum_wire_spellings_are_byte_exact() {
        // The mixed casing across the four families is the contract, not a typo to fix.
        assert_eq!(USER_TYPE_SUPER, "Super");
        assert_eq!(USER_TYPE_NORMAL, "Normal");
        assert_eq!(USER_STATUS_ENABLE, "enable");
        assert_eq!(USER_STATUS_DISABLE, "disable");
        assert_eq!(POLICY_TYPE_CUSTOM, "Custom");
        assert_eq!(POLICY_TYPE_DEFAULT, "Default");
        assert_eq!(DECISION_ALLOW, "Allow");
        assert_eq!(DECISION_DENY, "Deny");
    }

    #[test]
    fn full_user_keeps_all_four_keys_in_order() {
        let user = AuthUser {
            username: "rocketmq".to_string(),
            password: Some("12345678".to_string()),
            user_type: Some(USER_TYPE_SUPER.to_string()),
            user_status: Some(USER_STATUS_ENABLE.to_string()),
        };
        assert_eq!(
            String::from_utf8(user.encode()).unwrap(),
            r#"{"username":"rocketmq","password":"12345678","userType":"Super","userStatus":"enable"}"#
        );
        let object = user.to_json_value();
        let keys: Vec<&str> = object
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            vec!["username", "password", "userType", "userStatus"],
            "keys keep contract order instead of being sorted away"
        );
    }

    /// The convenience shape never writes `userStatus`: a null there would ask the broker to
    /// clear a status the caller did not mention.
    #[test]
    fn user_without_status_omits_the_key_entirely() {
        let user = AuthUser {
            username: "rocketmq".to_string(),
            password: Some("12345678".to_string()),
            user_type: Some(USER_TYPE_NORMAL.to_string()),
            user_status: None,
        };
        let encoded = String::from_utf8(user.encode()).unwrap();
        assert_eq!(
            encoded,
            r#"{"username":"rocketmq","password":"12345678","userType":"Normal"}"#
        );
        assert!(
            !encoded.contains("userStatus"),
            "an unset field must not appear as a null key: {encoded}"
        );
        assert!(
            !encoded.contains("null"),
            "no null-valued key in a request body: {encoded}"
        );
    }

    #[test]
    fn user_read_accepts_a_body_with_missing_optional_keys() {
        let user = AuthUser::decode(&bytes(r#"{"username":"only-name"}"#)).unwrap();
        assert_eq!(
            user,
            AuthUser {
                username: "only-name".to_string(),
                password: None,
                user_type: None,
                user_status: None,
            }
        );
        // An explicit null is read as unset, exactly like a missing key
        let nulled = AuthUser::decode(&bytes(
            r#"{"username":"a","password":null,"userType":null,"userStatus":null}"#,
        ))
        .unwrap();
        assert_eq!(nulled.user_status, None);
        assert_eq!(nulled.password, None);
    }

    /// One policy, no `policyType` key, one entry per resource repeating the shared values.
    #[test]
    fn resource_short_form_builds_one_policy_and_repeats_entry_values() {
        let acl = Acl::for_resources(
            "user:alice",
            &["topic:T1", "topic:T2"],
            &["PUB", "SUB"],
            &["192.168.0.1"],
            DECISION_ALLOW,
        );
        assert_eq!(
            String::from_utf8(acl.encode()).unwrap(),
            concat!(
                r#"{"subject":"user:alice","policies":[{"entries":["#,
                r#"{"resource":"topic:T1","actions":["PUB","SUB"],"sourceIps":["192.168.0.1"],"decision":"Allow"},"#,
                r#"{"resource":"topic:T2","actions":["PUB","SUB"],"sourceIps":["192.168.0.1"],"decision":"Allow"}"#,
                r#"]}]}"#
            )
        );
        assert_eq!(acl.policies.len(), 1);
        assert_eq!(acl.policies[0].policy_type, None);
        assert_eq!(acl.policies[0].entries.len(), 2);
    }

    #[test]
    fn policy_type_written_only_when_set() {
        let mut acl = Acl::new("user:alice");
        acl.policies.push(AclPolicy {
            policy_type: Some(POLICY_TYPE_CUSTOM.to_string()),
            entries: vec![AclEntry {
                resource: "group:G1".to_string(),
                actions: vec!["ANY".to_string()],
                source_ips: vec![],
                decision: Some(DECISION_DENY.to_string()),
            }],
        });
        assert_eq!(
            String::from_utf8(acl.encode()).unwrap(),
            concat!(
                r#"{"subject":"user:alice","policies":[{"policyType":"Custom","entries":["#,
                r#"{"resource":"group:G1","actions":["ANY"],"sourceIps":[],"decision":"Deny"}]}]}"#
            )
        );
    }

    #[test]
    fn acl_read_round_trips_and_tolerates_missing_keys() {
        let wire = concat!(
            r#"{"subject":"user:bob","policies":[{"policyType":"Default","entries":["#,
            r#"{"resource":"topic:T","actions":["PUB"],"sourceIps":["*"],"decision":"Allow"},"#,
            r#"{"resource":"topic:U"}]}]}"#
        );
        let acl = Acl::decode(&bytes(wire)).unwrap();
        assert_eq!(acl.subject, "user:bob");
        assert_eq!(acl.policies.len(), 1);
        assert_eq!(acl.policies[0].policy_type.as_deref(), Some("Default"));
        assert_eq!(acl.policies[0].entries[0].actions, vec!["PUB".to_string()]);
        assert_eq!(acl.policies[0].entries[0].source_ips, vec!["*".to_string()]);
        // An entry the broker sent without the optional keys reads back as unset / empty lists
        assert_eq!(acl.policies[0].entries[1].resource, "topic:U");
        assert!(acl.policies[0].entries[1].actions.is_empty());
        assert_eq!(acl.policies[0].entries[1].decision, None);
    }

    #[test]
    fn list_bodies_read_empty_and_missing_as_no_records() {
        assert_eq!(users_from_body(None).unwrap(), Vec::<AuthUser>::new());
        assert_eq!(
            users_from_body(Some(&bytes(""))).unwrap(),
            Vec::<AuthUser>::new()
        );
        assert_eq!(
            users_from_body(Some(&bytes("null"))).unwrap(),
            Vec::<AuthUser>::new()
        );
        assert!(acls_from_body(Some(&bytes("[]"))).unwrap().is_empty());

        let users = users_from_body(Some(&bytes(
            r#"[{"username":"a"},{"username":"b","userStatus":"disable"}]"#,
        )))
        .unwrap();
        assert_eq!(users.len(), 2);
        assert_eq!(users[0].user_status, None);
        assert_eq!(users[1].user_status.as_deref(), Some("disable"));
    }

    #[test]
    fn single_record_bodies_read_missing_as_none() {
        assert_eq!(user_from_body(None).unwrap(), None);
        assert_eq!(user_from_body(Some(&bytes(""))).unwrap(), None);
        assert_eq!(acl_from_body(Some(&bytes("null"))).unwrap(), None);

        let user = user_from_body(Some(&bytes(r#"{"username":"a","userType":"Super"}"#)))
            .unwrap()
            .expect("present record");
        assert_eq!(user.user_type.as_deref(), Some(USER_TYPE_SUPER));
        let acl = acl_from_body(Some(&bytes(r#"{"subject":"user:x"}"#)))
            .unwrap()
            .expect("present acl");
        assert!(acl.policies.is_empty());
    }

    #[test]
    fn wrong_shape_bodies_are_rejected_not_guessed() {
        // A list response that is really an object cannot be read as a list
        let err = users_from_body(Some(&bytes(r#"{"username":"a"}"#))).unwrap_err();
        assert!(matches!(err, Error::Decode(_)), "{err}");
        // a non-object element is dirty data too
        assert!(matches!(
            users_from_body(Some(&bytes("[42]"))).unwrap_err(),
            Error::Decode(_)
        ));
        // and a scalar single record is not a user object
        assert!(matches!(
            user_from_body(Some(&bytes("7"))).unwrap_err(),
            Error::Decode(_)
        ));
        // a policy list written as an object instead of an array
        assert!(matches!(
            Acl::decode(&bytes(r#"{"subject":"s","policies":{}}"#)).unwrap_err(),
            Error::Decode(_)
        ));
    }

    #[test]
    fn push_string_leaves_unset_keys_out() {
        let mut pairs: Vec<(&'static str, Value)> = Vec::new();
        push_string(&mut pairs, "z", None);
        push_string(&mut pairs, "y", Some("1"));
        push_string(&mut pairs, "x", None);
        let object = json_object(pairs);
        assert_eq!(object.as_object().unwrap().len(), 1);
        assert_eq!(object["y"], "1");
    }
}
