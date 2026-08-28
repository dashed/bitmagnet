use std::borrow::Cow;
use std::collections::BTreeMap;

use bendy::decoding::{Decoder, FromBencode};
use bendy::encoding::Encoder;
use bendy::value::Value;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::compact::{
    decode_nodes, decode_samples, encode_nodes, encode_samples, CompactAddr, CompactCodecError,
    CompactNode, Id20,
};
use crate::scrape::{ScrapeBloomError, ScrapeBloomFilter};

type OwnedValue = Value<'static>;
type OwnedDict = BTreeMap<Cow<'static, [u8]>, OwnedValue>;

/// An opaque KRPC byte string rendered as lowercase hex in JSON fixtures.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct ByteString(Vec<u8>);

impl ByteString {
    #[must_use]
    pub fn new(value: impl Into<Vec<u8>>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Serialize for ByteString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(&self.0))
    }
}

impl<'de> Deserialize<'de> for ByteString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value.bytes().any(|byte| byte.is_ascii_uppercase()) {
            return Err(serde::de::Error::custom(
                "byte string hex must be lowercase",
            ));
        }
        hex::decode(value)
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageArgs {
    pub id: Id20,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub info_hash: Option<Id20>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<Id20>,
    /// Go's non-pointer string cannot distinguish omission from empty.
    #[serde(default, skip_serializing_if = "ByteString::is_empty")]
    pub token: ByteString,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<i64>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub implied_port: bool,
    /// Go's slice `omitempty` distinguishes nil from non-nil empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub want: Option<Vec<ByteString>>,
    /// Go's non-pointer `int` cannot distinguish omission from an explicit zero.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub no_seed: i64,
    /// Go's non-pointer `int` cannot distinguish omission from an explicit zero.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub scrape: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageReturn {
    pub id: Id20,
    /// Go's slice `omitempty` distinguishes nil from non-nil empty when encoding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nodes: Option<Vec<CompactNode>>,
    /// Go's slice `omitempty` distinguishes nil from non-nil empty when encoding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nodes6: Option<Vec<CompactNode>>,
    /// Pointer in Go: explicit empty and omission remain distinct.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<ByteString>,
    /// Go's slice `omitempty` distinguishes nil from non-nil empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<CompactAddr>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub num: Option<i64>,
    /// Pointer in Go: `Some(empty)` advertises BEP-51 support.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub samples: Option<Vec<Id20>>,
    /// BEP-33 seeder filter (`BFsd` on the wire); pointer presence is retained.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seeders_bloom: Option<ScrapeBloomFilter>,
    /// BEP-33 peer filter (`BFpe` on the wire); pointer presence is retained.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peers_bloom: Option<ScrapeBloomFilter>,
}

/// Go accepts both the canonical `[code,message]` list and a legacy bare
/// message string; both decode to this projection and encode canonically as a
/// two-element list.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KrpcError {
    pub code: i64,
    pub message: ByteString,
}

/// The Go `dht.Msg` wire projection. `message_type`, `query`, transaction IDs,
/// tokens, client IDs, and error messages are bytes, never assumed UTF-8.
///
/// This codec deliberately does not validate protocol semantics. Go's bencode
/// decoder accepts empty/unknown `y` and `q`, mixed envelope fields, and unknown
/// dictionary keys; later server logic owns those decisions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KrpcMessage {
    pub transaction_id: ByteString,
    pub message_type: ByteString,
    /// Go's non-pointer string cannot distinguish omission from empty.
    #[serde(default, skip_serializing_if = "ByteString::is_empty")]
    pub query: ByteString,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<MessageArgs>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<MessageReturn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<KrpcError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_addr: Option<CompactAddr>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool,
    /// Go's non-pointer string cannot distinguish omission from empty.
    #[serde(default, skip_serializing_if = "ByteString::is_empty")]
    pub client_id: ByteString,
}

impl KrpcMessage {
    pub fn encode(&self) -> Result<Vec<u8>, WireError> {
        let mut message = OwnedDict::new();
        insert_bytes(&mut message, b"t", self.transaction_id.as_bytes());
        insert_bytes(&mut message, b"y", self.message_type.as_bytes());
        if !self.query.is_empty() {
            insert_bytes(&mut message, b"q", self.query.as_bytes());
        }
        if let Some(args) = &self.args {
            message.insert(key(b"a"), encode_args(args));
        }
        if let Some(response) = &self.response {
            message.insert(key(b"r"), encode_return(response)?);
        }
        if let Some(error) = &self.error {
            message.insert(key(b"e"), encode_error(error));
        }
        if let Some(addr) = self.observed_addr {
            insert_bytes(&mut message, b"ip", &addr.encode());
        }
        if self.read_only {
            message.insert(key(b"ro"), Value::Integer(1));
        }
        if !self.client_id.is_empty() {
            insert_bytes(&mut message, b"v", self.client_id.as_bytes());
        }
        let value = Value::Dict(message);
        let mut encoder = Encoder::new().with_max_depth(3);
        encoder
            .emit(&value)
            .and_then(|()| encoder.get_output())
            .map_err(|error| WireError::Bencode(error.to_string()))
    }

    pub fn decode(input: &[u8]) -> Result<Self, WireError> {
        let mut decoder = Decoder::new(input).with_max_depth(3);
        let object = decoder
            .next_object()
            .map_err(|error| WireError::Bencode(error.to_string()))?
            .ok_or_else(|| WireError::Invalid("empty KRPC datagram".into()))?;
        let value = Value::decode_bencode_object(object)
            .map_err(|error| WireError::Bencode(error.to_string()))?
            .into_owned();
        if decoder
            .next_object()
            .map_err(|error| WireError::Bencode(error.to_string()))?
            .is_some()
        {
            return Err(WireError::Invalid(
                "trailing bencode object after KRPC message".into(),
            ));
        }
        let mut dict = into_dict(value, "KRPC message")?;
        let transaction_id = take_optional_bytes(&mut dict, b"t")?
            .map(ByteString::new)
            .unwrap_or_default();
        let message_type = take_optional_bytes(&mut dict, b"y")?
            .map(ByteString::new)
            .unwrap_or_default();
        let query = take_optional_bytes(&mut dict, b"q")?
            .map(ByteString::new)
            .unwrap_or_default();
        let args = take_optional(&mut dict, b"a")
            .map(decode_args)
            .transpose()?;
        let response = take_optional(&mut dict, b"r")
            .map(decode_return)
            .transpose()?;
        let error = take_optional(&mut dict, b"e")
            .map(decode_error)
            .transpose()?;
        let observed_addr = take_optional_bytes(&mut dict, b"ip")?
            .map(|value| CompactAddr::decode(&value))
            .transpose()?;
        let read_only = take_optional_bool(&mut dict, b"ro")?.unwrap_or_default();
        let client_id = take_optional_bytes(&mut dict, b"v")?
            .map(ByteString::new)
            .unwrap_or_default();
        // Go ignores unknown fields at every dictionary level. Preserve that
        // forward-compatible acceptance even though they are not re-emitted.
        Ok(Self {
            transaction_id,
            message_type,
            query,
            args,
            response,
            error,
            observed_addr,
            read_only,
            client_id,
        })
    }
}

fn encode_args(args: &MessageArgs) -> OwnedValue {
    let mut dict = OwnedDict::new();
    insert_bytes(&mut dict, b"id", args.id.as_bytes());
    if let Some(value) = args.info_hash.filter(|value| *value != Id20::ZERO) {
        insert_bytes(&mut dict, b"info_hash", value.as_bytes());
    }
    if let Some(value) = args.target.filter(|value| *value != Id20::ZERO) {
        insert_bytes(&mut dict, b"target", value.as_bytes());
    }
    if !args.token.is_empty() {
        insert_bytes(&mut dict, b"token", args.token.as_bytes());
    }
    if let Some(value) = args.port {
        dict.insert(key(b"port"), Value::Integer(value));
    }
    if args.implied_port {
        dict.insert(key(b"implied_port"), Value::Integer(1));
    }
    if let Some(want) = &args.want {
        dict.insert(
            key(b"want"),
            Value::List(want.iter().map(|want| bytes(want.as_bytes())).collect()),
        );
    }
    if args.no_seed != 0 {
        dict.insert(key(b"noseed"), Value::Integer(args.no_seed));
    }
    if args.scrape != 0 {
        dict.insert(key(b"scrape"), Value::Integer(args.scrape));
    }
    Value::Dict(dict)
}

fn decode_args(value: OwnedValue) -> Result<MessageArgs, WireError> {
    let mut dict = into_dict(value, "query arguments")?;
    let id = take_optional_bytes(&mut dict, b"id")?
        .map(|value| Id20::from_slice(&value))
        .transpose()?
        .unwrap_or(Id20::ZERO);
    let info_hash = take_optional_id(&mut dict, b"info_hash")?.filter(|value| *value != Id20::ZERO);
    let target = take_optional_id(&mut dict, b"target")?.filter(|value| *value != Id20::ZERO);
    let token = take_optional_bytes(&mut dict, b"token")?
        .map(ByteString::new)
        .unwrap_or_default();
    let port = take_optional_integer(&mut dict, b"port")?;
    let implied_port = take_optional_bool(&mut dict, b"implied_port")?.unwrap_or_default();
    let want = take_optional(&mut dict, b"want")
        .map(|value| {
            into_list(value, "want")?
                .into_iter()
                .map(|value| into_bytes(value, "want value").map(ByteString::new))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;
    let no_seed = take_optional_integer(&mut dict, b"noseed")?.unwrap_or_default();
    let scrape = take_optional_integer(&mut dict, b"scrape")?.unwrap_or_default();
    Ok(MessageArgs {
        id,
        info_hash,
        target,
        token,
        port,
        implied_port,
        want,
        no_seed,
        scrape,
    })
}

fn encode_return(value: &MessageReturn) -> Result<OwnedValue, WireError> {
    let mut dict = OwnedDict::new();
    insert_bytes(&mut dict, b"id", value.id.as_bytes());
    if let Some(filter) = value.seeders_bloom {
        insert_bytes(&mut dict, b"BFsd", filter.as_bytes());
    }
    if let Some(filter) = value.peers_bloom {
        insert_bytes(&mut dict, b"BFpe", filter.as_bytes());
    }
    if let Some(nodes) = &value.nodes {
        insert_bytes(&mut dict, b"nodes", &encode_nodes(nodes, false)?);
    }
    if let Some(nodes6) = &value.nodes6 {
        insert_bytes(&mut dict, b"nodes6", &encode_nodes(nodes6, true)?);
    }
    if let Some(token) = &value.token {
        insert_bytes(&mut dict, b"token", token.as_bytes());
    }
    if let Some(values) = &value.values {
        dict.insert(
            key(b"values"),
            Value::List(values.iter().map(|addr| bytes(&addr.encode())).collect()),
        );
    }
    if let Some(interval) = value.interval {
        dict.insert(key(b"interval"), Value::Integer(interval));
    }
    if let Some(num) = value.num {
        dict.insert(key(b"num"), Value::Integer(num));
    }
    if let Some(samples) = &value.samples {
        insert_bytes(&mut dict, b"samples", &encode_samples(samples));
    }
    Ok(Value::Dict(dict))
}

fn decode_return(value: OwnedValue) -> Result<MessageReturn, WireError> {
    let mut dict = into_dict(value, "response")?;
    let id = take_optional_bytes(&mut dict, b"id")?
        .map(|value| Id20::from_slice(&value))
        .transpose()?
        .unwrap_or(Id20::ZERO);
    // Go's custom compact-node unmarshaler collapses an empty byte string back
    // to a nil slice, even though its encoder can emit a non-nil empty slice.
    let nodes = take_optional_bytes(&mut dict, b"nodes")?
        .filter(|value| !value.is_empty())
        .map(|value| decode_nodes(&value, false))
        .transpose()?;
    let nodes6 = take_optional_bytes(&mut dict, b"nodes6")?
        .filter(|value| !value.is_empty())
        .map(|value| decode_nodes(&value, true))
        .transpose()?;
    let token = take_optional_bytes(&mut dict, b"token")?.map(ByteString::new);
    let values = take_optional(&mut dict, b"values")
        .map(|value| {
            into_list(value, "peer values")?
                .into_iter()
                .map(|value| -> Result<CompactAddr, WireError> {
                    Ok(CompactAddr::decode(&into_bytes(value, "peer value")?)?)
                })
                .collect::<Result<Vec<_>, WireError>>()
        })
        .transpose()?;
    let interval = take_optional_integer(&mut dict, b"interval")?;
    let num = take_optional_integer(&mut dict, b"num")?;
    let samples = take_optional_bytes(&mut dict, b"samples")?
        .map(|value| decode_samples(&value))
        .transpose()?;
    let seeders_bloom = take_optional_bytes(&mut dict, b"BFsd")?
        .map(|value| ScrapeBloomFilter::from_slice(&value))
        .transpose()?;
    let peers_bloom = take_optional_bytes(&mut dict, b"BFpe")?
        .map(|value| ScrapeBloomFilter::from_slice(&value))
        .transpose()?;
    Ok(MessageReturn {
        id,
        nodes,
        nodes6,
        token,
        values,
        interval,
        num,
        samples,
        seeders_bloom,
        peers_bloom,
    })
}

fn encode_error(error: &KrpcError) -> OwnedValue {
    Value::List(vec![
        Value::Integer(error.code),
        bytes(error.message.as_bytes()),
    ])
}

fn decode_error(value: OwnedValue) -> Result<KrpcError, WireError> {
    if let Value::Bytes(message) = value {
        return Ok(KrpcError {
            code: 0,
            message: ByteString::new(message.into_owned()),
        });
    }
    let mut values = into_list(value, "error")?;
    if values.len() != 2 {
        return Err(WireError::Invalid(
            "KRPC error must be a message or exactly [code,message]".into(),
        ));
    }
    let message = ByteString::new(into_bytes(
        values.pop().expect("length checked"),
        "error message",
    )?);
    let code = into_integer(values.pop().expect("length checked"), "error code")?;
    Ok(KrpcError { code, message })
}

const fn is_zero(value: &i64) -> bool {
    *value == 0
}

fn key(value: &[u8]) -> Cow<'static, [u8]> {
    Cow::Owned(value.to_vec())
}

fn bytes(value: &[u8]) -> OwnedValue {
    Value::Bytes(Cow::Owned(value.to_vec()))
}

fn insert_bytes(dict: &mut OwnedDict, name: &[u8], value: &[u8]) {
    dict.insert(key(name), bytes(value));
}

fn take_optional(dict: &mut OwnedDict, name: &[u8]) -> Option<OwnedValue> {
    dict.remove::<[u8]>(name)
}

fn take_optional_bytes(dict: &mut OwnedDict, name: &[u8]) -> Result<Option<Vec<u8>>, WireError> {
    take_optional(dict, name)
        .map(|value| into_bytes(value, "byte string"))
        .transpose()
}

fn take_optional_integer(dict: &mut OwnedDict, name: &[u8]) -> Result<Option<i64>, WireError> {
    take_optional(dict, name)
        .map(|value| into_integer(value, "integer"))
        .transpose()
}

fn take_optional_bool(dict: &mut OwnedDict, name: &[u8]) -> Result<Option<bool>, WireError> {
    take_optional(dict, name)
        .map(|value| into_go_bool(value, "boolean"))
        .transpose()
}

fn take_optional_id(dict: &mut OwnedDict, name: &[u8]) -> Result<Option<Id20>, WireError> {
    take_optional_bytes(dict, name)?
        .map(|value| Id20::from_slice(&value))
        .transpose()
        .map_err(WireError::from)
}

fn into_dict(value: OwnedValue, description: &str) -> Result<OwnedDict, WireError> {
    match value {
        Value::Dict(value) => Ok(value),
        _ => Err(WireError::Invalid(format!(
            "{description} must be a dictionary"
        ))),
    }
}

fn into_list(value: OwnedValue, description: &str) -> Result<Vec<OwnedValue>, WireError> {
    match value {
        Value::List(value) => Ok(value),
        _ => Err(WireError::Invalid(format!("{description} must be a list"))),
    }
}

fn into_bytes(value: OwnedValue, description: &str) -> Result<Vec<u8>, WireError> {
    match value {
        Value::Bytes(value) => Ok(value.into_owned()),
        _ => Err(WireError::Invalid(format!(
            "{description} must be a byte string"
        ))),
    }
}

fn into_integer(value: OwnedValue, description: &str) -> Result<i64, WireError> {
    match value {
        Value::Integer(value) => Ok(value),
        _ => Err(WireError::Invalid(format!(
            "{description} must be an integer"
        ))),
    }
}

fn into_go_bool(value: OwnedValue, description: &str) -> Result<bool, WireError> {
    match value {
        Value::Integer(value) => Ok(value != 0),
        Value::Bytes(value) => match value.as_ref() {
            b"1" | b"t" | b"T" | b"true" | b"TRUE" | b"True" => Ok(true),
            b"0" | b"f" | b"F" | b"false" | b"FALSE" | b"False" => Ok(false),
            other => Ok(!other.is_empty()),
        },
        Value::List(mut values) if values.len() == 1 => {
            into_go_bool(values.pop().expect("length checked"), description)
        }
        _ => Err(WireError::Invalid(format!(
            "{description} must be an integer, byte string, or singleton list"
        ))),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("invalid bencode: {0}")]
    Bencode(String),
    #[error("invalid KRPC value: {0}")]
    Invalid(String),
    #[error(transparent)]
    Compact(#[from] CompactCodecError),
    #[error(transparent)]
    ScrapeBloom(#[from] ScrapeBloomError),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wire(value: OwnedValue) -> Vec<u8> {
        let mut encoder = Encoder::new().with_max_depth(5);
        encoder.emit(&value).unwrap();
        encoder.get_output().unwrap()
    }

    fn envelope_field(name: &[u8], value: OwnedValue) -> Vec<u8> {
        let mut dict = OwnedDict::new();
        dict.insert(key(name), value);
        if name != b"t" {
            insert_bytes(&mut dict, b"t", b"");
        }
        if name != b"y" {
            insert_bytes(&mut dict, b"y", b"");
        }
        wire(Value::Dict(dict))
    }

    fn nested_field(envelope: &[u8], name: &[u8], value: OwnedValue) -> Vec<u8> {
        let mut nested = OwnedDict::new();
        insert_bytes(&mut nested, b"id", &[0; 20]);
        nested.insert(key(name), value);
        envelope_field(envelope, Value::Dict(nested))
    }

    fn empty() -> KrpcMessage {
        KrpcMessage {
            transaction_id: ByteString::default(),
            message_type: ByteString::default(),
            query: ByteString::default(),
            args: None,
            response: None,
            error: None,
            observed_addr: None,
            read_only: false,
            client_id: ByteString::default(),
        }
    }

    #[test]
    fn go_empty_message_and_raw_fields_round_trip() {
        assert_eq!(empty().encode().unwrap(), b"d1:t0:1:y0:e");
        assert_eq!(KrpcMessage::decode(b"d1:t0:1:y0:e").unwrap(), empty());
        let raw = KrpcMessage {
            transaction_id: ByteString::new([0xff, 0]),
            message_type: ByteString::new([0x80]),
            query: ByteString::new([0, 0xfe]),
            ..empty()
        };
        assert_eq!(KrpcMessage::decode(&raw.encode().unwrap()).unwrap(), raw);
    }

    #[test]
    fn go_compatibility_ro_unknown_and_legacy_error() {
        assert!(
            !KrpcMessage::decode(b"d2:roi0e1:t0:1:y0:e")
                .unwrap()
                .read_only
        );
        assert_eq!(
            KrpcMessage::decode(b"d1:t0:1:y0:1:z1:xe")
                .unwrap()
                .encode()
                .unwrap(),
            b"d1:t0:1:y0:e"
        );
        let legacy = KrpcMessage::decode(b"d1:e4:oops1:t0:1:y1:ee").unwrap();
        assert_eq!(legacy.error.unwrap().message.as_bytes(), b"oops");
    }

    #[test]
    fn strict_syntax_and_compact_shapes_fail_closed() {
        for input in [
            b"".as_slice(),
            b"de0:",
            b"d1:ti00e1:y0:e",
            b"d1:t0:1:t0:1:y0:e",
            b"d1:y0:1:t0:e",
        ] {
            assert!(KrpcMessage::decode(input).is_err(), "accepted {input:?}");
        }
        let short_nodes = b"d1:rd2:id20:000000000000000000005:nodes1:xe1:t1:a1:y1:re";
        assert!(matches!(
            KrpcMessage::decode(short_nodes),
            Err(WireError::Compact(
                CompactCodecError::MisalignedNodeList { .. }
            ))
        ));
    }

    #[test]
    fn malformed_wire_shapes_return_errors_without_panics() {
        let cases = [
            ("top list", wire(Value::List(vec![]))),
            ("top integer", wire(Value::Integer(1))),
            ("t integer", envelope_field(b"t", Value::Integer(1))),
            ("y integer", envelope_field(b"y", Value::Integer(1))),
            ("q integer", envelope_field(b"q", Value::Integer(1))),
            ("ip integer", envelope_field(b"ip", Value::Integer(1))),
            ("a not dict", envelope_field(b"a", bytes(b"x"))),
            ("r not dict", envelope_field(b"r", bytes(b"x"))),
            ("args id", nested_field(b"a", b"id", Value::Integer(1))),
            (
                "args info_hash",
                nested_field(b"a", b"info_hash", Value::Integer(1)),
            ),
            (
                "args target",
                nested_field(b"a", b"target", Value::Integer(1)),
            ),
            (
                "args token",
                nested_field(b"a", b"token", Value::Integer(1)),
            ),
            ("args port", nested_field(b"a", b"port", bytes(b"1"))),
            (
                "args implied_port",
                nested_field(b"a", b"implied_port", Value::Dict(OwnedDict::new())),
            ),
            ("args want", nested_field(b"a", b"want", bytes(b"n4"))),
            (
                "args want item",
                nested_field(b"a", b"want", Value::List(vec![Value::Integer(1)])),
            ),
            ("args noseed", nested_field(b"a", b"noseed", bytes(b"1"))),
            ("args scrape", nested_field(b"a", b"scrape", bytes(b"1"))),
            ("return id", nested_field(b"r", b"id", Value::Integer(1))),
            (
                "return nodes",
                nested_field(b"r", b"nodes", Value::Integer(1)),
            ),
            (
                "return nodes6",
                nested_field(b"r", b"nodes6", Value::List(vec![])),
            ),
            (
                "return token",
                nested_field(b"r", b"token", Value::Integer(1)),
            ),
            ("return values", nested_field(b"r", b"values", bytes(b"x"))),
            (
                "return values item",
                nested_field(b"r", b"values", Value::List(vec![Value::Integer(1)])),
            ),
            (
                "return interval",
                nested_field(b"r", b"interval", bytes(b"1")),
            ),
            ("return num", nested_field(b"r", b"num", bytes(b"1"))),
            (
                "return samples",
                nested_field(b"r", b"samples", Value::Integer(1)),
            ),
            (
                "return BFsd type",
                nested_field(b"r", b"BFsd", Value::Integer(1)),
            ),
            (
                "return BFpe type",
                nested_field(b"r", b"BFpe", Value::List(vec![])),
            ),
            (
                "return BFsd short",
                nested_field(b"r", b"BFsd", bytes(&vec![0; 255])),
            ),
            (
                "return BFpe long",
                nested_field(b"r", b"BFpe", bytes(&vec![0; 257])),
            ),
            ("error empty", envelope_field(b"e", Value::List(vec![]))),
            (
                "error one",
                envelope_field(b"e", Value::List(vec![Value::Integer(1)])),
            ),
            (
                "error three",
                envelope_field(
                    b"e",
                    Value::List(vec![Value::Integer(1), bytes(b"x"), bytes(b"y")]),
                ),
            ),
            (
                "error code",
                envelope_field(b"e", Value::List(vec![bytes(b"1"), bytes(b"x")])),
            ),
            (
                "error message",
                envelope_field(
                    b"e",
                    Value::List(vec![Value::Integer(1), Value::Integer(2)]),
                ),
            ),
            ("truncated", b"d1:t1:x1:y1:q".to_vec()),
        ];
        for (name, input) in cases {
            assert!(KrpcMessage::decode(&input).is_err(), "accepted {name}");
        }
    }

    #[test]
    fn nested_unknown_dictionary_keys_are_ignored() {
        let mut unknown = OwnedDict::new();
        insert_bytes(&mut unknown, b"nested", b"value");
        for envelope in [b"a".as_slice(), b"r".as_slice()] {
            let input = nested_field(envelope, b"future", Value::Dict(unknown.clone()));
            assert!(KrpcMessage::decode(&input).is_ok());
        }
    }
}
