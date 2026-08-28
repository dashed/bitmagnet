use std::cmp::Ordering;

use crate::compact::{decode_nodes, decode_samples};
use crate::{
    ByteString, CompactAddr, CompactCodecError, Id20, KrpcError, KrpcMessage, MessageArgs,
    MessageReturn, ScrapeBloomError, ScrapeBloomFilter,
};

/// The exact maximum payload accepted by the production Go UDP receive loop.
pub const MAX_INBOUND_DATAGRAM_BYTES: usize = 65_507;
/// A conservative, non-configurable nesting ceiling for untrusted bencode.
pub const MAX_INBOUND_NESTING_DEPTH: usize = 8;
/// A non-configurable ceiling on values visited by one decode.
pub const MAX_INBOUND_VALUES: usize = 32_768;

impl KrpcMessage {
    /// Decode the permissive-but-bounded production inbound KRPC projection.
    ///
    /// Unlike [`KrpcMessage::decode`], typed dictionaries accept unsorted and
    /// duplicate fields like Go's struct decoder. Every occurrence is decoded
    /// and the last successful one wins. Unknown values are validated and
    /// discarded without constructing a generic value tree.
    pub fn decode_inbound(input: &[u8]) -> Result<Self, InboundError> {
        Cursor::new(input)?.parse_message()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InboundLimitKind {
    DatagramBytes,
    NestingDepth,
    ValueCount,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InboundSyntaxKind {
    UnexpectedEnd,
    UnexpectedValueType,
    InvalidByteStringLength,
    InvalidInteger,
    MissingDictionaryValue,
    NonCanonicalUnknownDictionary,
    TrailingData,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InboundShapeKind {
    Dictionary,
    ByteString,
    Integer,
    List,
    Boolean,
    ErrorValue,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum InboundError {
    #[error("inbound KRPC {kind:?} limit exceeded at byte {offset}: {actual} > {limit}")]
    Limit {
        kind: InboundLimitKind,
        offset: usize,
        actual: usize,
        limit: usize,
    },
    #[error("inbound KRPC syntax error {kind:?} at byte {offset}")]
    Syntax {
        kind: InboundSyntaxKind,
        offset: usize,
    },
    #[error("inbound KRPC field {field} must have shape {expected:?} at byte {offset}")]
    Shape {
        field: &'static str,
        expected: InboundShapeKind,
        offset: usize,
    },
    #[error("invalid compact KRPC field at byte {offset}: {source}")]
    Compact {
        offset: usize,
        #[source]
        source: CompactCodecError,
    },
    #[error("invalid BEP-33 KRPC field at byte {offset}: {source}")]
    ScrapeBloom {
        offset: usize,
        #[source]
        source: ScrapeBloomError,
    },
    #[error("inbound KRPC field {field} is outside the admitted projection at byte {offset}")]
    Unsupported { field: &'static str, offset: usize },
}

struct Cursor<'a> {
    input: &'a [u8],
    offset: usize,
    values: usize,
}

impl<'a> Cursor<'a> {
    fn new(input: &'a [u8]) -> Result<Self, InboundError> {
        if input.len() > MAX_INBOUND_DATAGRAM_BYTES {
            return Err(InboundError::Limit {
                kind: InboundLimitKind::DatagramBytes,
                offset: 0,
                actual: input.len(),
                limit: MAX_INBOUND_DATAGRAM_BYTES,
            });
        }
        Ok(Self {
            input,
            offset: 0,
            values: 0,
        })
    }

    fn parse_message(mut self) -> Result<KrpcMessage, InboundError> {
        self.start_dictionary("KRPC message", 1)?;
        let mut message = KrpcMessage {
            transaction_id: ByteString::default(),
            message_type: ByteString::default(),
            query: ByteString::default(),
            args: None,
            response: None,
            error: None,
            observed_addr: None,
            read_only: false,
            client_id: ByteString::default(),
        };
        while !self.end_container()? {
            let (_, key) = self.parse_scalar_bytes("KRPC dictionary key", 2)?;
            self.require_dictionary_value()?;
            match key {
                b"t" => {
                    message.transaction_id = ByteString::new(self.parse_scalar_bytes("t", 2)?.1)
                }
                b"y" => message.message_type = ByteString::new(self.parse_scalar_bytes("y", 2)?.1),
                b"q" => message.query = ByteString::new(self.parse_scalar_bytes("q", 2)?.1),
                b"a" => message.args = Some(self.parse_args(2)?),
                b"r" => message.response = Some(self.parse_return(2)?),
                b"e" => message.error = Some(self.parse_error(2)?),
                b"ip" => {
                    let (offset, value) = self.parse_bytes("ip")?;
                    message.observed_addr = Some(
                        CompactAddr::decode(value)
                            .map_err(|source| InboundError::Compact { offset, source })?,
                    );
                }
                b"ro" => message.read_only = self.parse_bool("ro", 2)?,
                b"v" => message.client_id = ByteString::new(self.parse_scalar_bytes("v", 2)?.1),
                _ => self.skip_generic(2)?,
            }
        }
        if self.offset != self.input.len() {
            return Err(InboundError::Syntax {
                kind: InboundSyntaxKind::TrailingData,
                offset: self.offset,
            });
        }
        Ok(message)
    }

    fn parse_args(&mut self, depth: usize) -> Result<MessageArgs, InboundError> {
        self.start_dictionary("a", depth)?;
        let mut args = MessageArgs {
            id: Id20::ZERO,
            info_hash: None,
            target: None,
            token: ByteString::default(),
            port: None,
            implied_port: false,
            want: None,
            no_seed: 0,
            scrape: 0,
        };
        while !self.end_container()? {
            let (_, key) = self.parse_scalar_bytes("a dictionary key", depth + 1)?;
            self.require_dictionary_value()?;
            match key {
                b"id" => args.id = self.parse_id("a.id", depth + 1)?,
                b"info_hash" => {
                    args.info_hash = Some(self.parse_id("a.info_hash", depth + 1)?)
                        .filter(|value| *value != Id20::ZERO);
                }
                b"target" => {
                    args.target = Some(self.parse_id("a.target", depth + 1)?)
                        .filter(|value| *value != Id20::ZERO);
                }
                b"token" => {
                    args.token = ByteString::new(self.parse_scalar_bytes("a.token", depth + 1)?.1)
                }
                b"port" => args.port = Some(self.parse_scalar_integer("a.port", depth + 1)?.1),
                b"implied_port" => {
                    args.implied_port = self.parse_bool("a.implied_port", depth + 1)?
                }
                b"want" => args.want = Some(self.parse_byte_list("a.want", depth + 1)?),
                b"noseed" => args.no_seed = self.parse_scalar_integer("a.noseed", depth + 1)?.1,
                b"scrape" => args.scrape = self.parse_scalar_integer("a.scrape", depth + 1)?.1,
                // BEP-44 fields are outside this projection but are typed by
                // Go. Validate their wire shape before discarding them.
                b"seq" | b"cas" => {
                    self.parse_scalar_integer("BEP-44 integer", depth + 1)?;
                }
                b"k" | b"salt" | b"sig" => {
                    self.parse_bytes("BEP-44 byte string")?;
                }
                b"v" => self.reject_bep44_value("a.v", depth + 1, false)?,
                _ => self.skip_generic(depth + 1)?,
            }
        }
        Ok(args)
    }

    fn parse_return(&mut self, depth: usize) -> Result<MessageReturn, InboundError> {
        self.start_dictionary("r", depth)?;
        let mut response = MessageReturn {
            id: Id20::ZERO,
            nodes: None,
            nodes6: None,
            token: None,
            values: None,
            interval: None,
            num: None,
            samples: None,
            seeders_bloom: None,
            peers_bloom: None,
        };
        while !self.end_container()? {
            let (_, key) = self.parse_scalar_bytes("r dictionary key", depth + 1)?;
            self.require_dictionary_value()?;
            match key {
                b"id" => response.id = self.parse_id("r.id", depth + 1)?,
                b"nodes" => {
                    let (offset, value) = self.parse_scalar_bytes("r.nodes", depth + 1)?;
                    response.nodes = if value.is_empty() {
                        None
                    } else {
                        Some(
                            decode_nodes(value, false)
                                .map_err(|source| InboundError::Compact { offset, source })?,
                        )
                    };
                }
                b"nodes6" => {
                    let (offset, value) = self.parse_scalar_bytes("r.nodes6", depth + 1)?;
                    response.nodes6 = if value.is_empty() {
                        None
                    } else {
                        Some(
                            decode_nodes(value, true)
                                .map_err(|source| InboundError::Compact { offset, source })?,
                        )
                    };
                }
                b"token" => {
                    response.token = Some(ByteString::new(
                        self.parse_scalar_bytes("r.token", depth + 1)?.1,
                    ))
                }
                b"values" => response.values = Some(self.parse_addresses(depth + 1)?),
                b"interval" => {
                    response.interval = Some(self.parse_scalar_integer("r.interval", depth + 1)?.1)
                }
                b"num" => response.num = Some(self.parse_scalar_integer("r.num", depth + 1)?.1),
                b"samples" => {
                    let (offset, value) = self.parse_scalar_bytes("r.samples", depth + 1)?;
                    response.samples = Some(
                        decode_samples(value)
                            .map_err(|source| InboundError::Compact { offset, source })?,
                    );
                }
                b"BFsd" => {
                    let (offset, value) = self.parse_bytes("r.BFsd")?;
                    response.seeders_bloom = Some(
                        ScrapeBloomFilter::from_slice(value)
                            .map_err(|source| InboundError::ScrapeBloom { offset, source })?,
                    );
                }
                b"BFpe" => {
                    let (offset, value) = self.parse_bytes("r.BFpe")?;
                    response.peers_bloom = Some(
                        ScrapeBloomFilter::from_slice(value)
                            .map_err(|source| InboundError::ScrapeBloom { offset, source })?,
                    );
                }
                b"seq" => {
                    self.parse_scalar_integer("BEP-44 seq", depth + 1)?;
                }
                b"k" | b"sig" => {
                    self.parse_bytes("BEP-44 byte string")?;
                }
                b"v" => self.reject_bep44_value("r.v", depth + 1, true)?,
                _ => self.skip_generic(depth + 1)?,
            }
        }
        Ok(response)
    }

    fn parse_error(&mut self, depth: usize) -> Result<KrpcError, InboundError> {
        match self.peek()? {
            b'0'..=b'9' => Ok(KrpcError {
                code: 0,
                message: ByteString::new(self.parse_bytes("e")?.1),
            }),
            b'l' => {
                self.start_list("e", depth)?;
                if self.end_container()? {
                    return Err(self.shape("e", InboundShapeKind::ErrorValue, self.offset - 1));
                }
                let code = self.parse_integer("e.code")?.1;
                if self.end_container()? {
                    return Err(self.shape("e", InboundShapeKind::ErrorValue, self.offset - 1));
                }
                let message = ByteString::new(self.parse_bytes("e.message")?.1);
                while !self.end_container()? {
                    self.skip_generic(depth + 1)?;
                }
                Ok(KrpcError { code, message })
            }
            _ => Err(self.shape("e", InboundShapeKind::ErrorValue, self.offset)),
        }
    }

    fn parse_id(&mut self, field: &'static str, depth: usize) -> Result<Id20, InboundError> {
        let (offset, value) = self.parse_scalar_bytes(field, depth)?;
        Id20::from_slice(value).map_err(|source| InboundError::Compact { offset, source })
    }

    /// Go's decoder recursively unwraps a singleton list when decoding a list
    /// into a scalar target. It validates every list entry before rejecting a
    /// non-singleton list, so this cursor does the same.
    fn parse_scalar_bytes(
        &mut self,
        field: &'static str,
        depth: usize,
    ) -> Result<(usize, &'a [u8]), InboundError> {
        if self.peek()? != b'l' {
            return self.parse_bytes(field);
        }
        let offset = self.offset;
        self.start_list(field, depth)?;
        let mut value = None;
        let mut count = 0;
        while !self.end_container()? {
            let parsed = self.parse_scalar_bytes(field, depth + 1)?;
            if count == 0 {
                value = Some(parsed);
            }
            count += 1;
        }
        if count == 1 {
            Ok(value.expect("one scalar list element was parsed"))
        } else {
            Err(self.shape(field, InboundShapeKind::ByteString, offset))
        }
    }

    fn parse_scalar_integer(
        &mut self,
        field: &'static str,
        depth: usize,
    ) -> Result<(usize, i64), InboundError> {
        if self.peek()? != b'l' {
            return self.parse_integer(field);
        }
        let offset = self.offset;
        self.start_list(field, depth)?;
        let mut value = None;
        let mut count = 0;
        while !self.end_container()? {
            let parsed = self.parse_scalar_integer(field, depth + 1)?;
            if count == 0 {
                value = Some(parsed);
            }
            count += 1;
        }
        if count == 1 {
            Ok(value.expect("one scalar list element was parsed"))
        } else {
            Err(self.shape(field, InboundShapeKind::Integer, offset))
        }
    }

    fn reject_bep44_value(
        &mut self,
        field: &'static str,
        depth: usize,
        raw: bool,
    ) -> Result<(), InboundError> {
        let offset = self.offset;
        if raw {
            self.skip_raw(depth)?;
        } else {
            self.skip_generic(depth)?;
        }
        Err(InboundError::Unsupported { field, offset })
    }

    fn parse_byte_list(
        &mut self,
        field: &'static str,
        depth: usize,
    ) -> Result<Vec<ByteString>, InboundError> {
        self.start_list(field, depth)?;
        let mut values = Vec::new();
        while !self.end_container()? {
            values.push(ByteString::new(
                self.parse_scalar_bytes(field, depth + 1)?.1,
            ));
        }
        Ok(values)
    }

    fn parse_addresses(&mut self, depth: usize) -> Result<Vec<CompactAddr>, InboundError> {
        self.start_list("r.values", depth)?;
        let mut values = Vec::new();
        while !self.end_container()? {
            let (offset, value) = self.parse_bytes("r.values entry")?;
            values.push(
                CompactAddr::decode(value)
                    .map_err(|source| InboundError::Compact { offset, source })?,
            );
        }
        Ok(values)
    }

    fn parse_bool(&mut self, field: &'static str, depth: usize) -> Result<bool, InboundError> {
        match self.peek()? {
            b'i' => {
                let (_, raw) = self.parse_integer_raw()?;
                Ok(raw != b"0")
            }
            b'0'..=b'9' => {
                let (_, raw) = self.parse_bytes(field)?;
                Ok(match raw {
                    b"1" | b"t" | b"T" | b"true" | b"TRUE" | b"True" => true,
                    b"0" | b"f" | b"F" | b"false" | b"FALSE" | b"False" => false,
                    other => !other.is_empty(),
                })
            }
            b'l' => {
                self.start_list(field, depth)?;
                let mut count = 0;
                let mut first = false;
                while !self.end_container()? {
                    let value = self.parse_bool(field, depth + 1)?;
                    if count == 0 {
                        first = value;
                    }
                    count += 1;
                }
                if count == 1 {
                    Ok(first)
                } else {
                    Err(self.shape(field, InboundShapeKind::Boolean, self.offset - 1))
                }
            }
            _ => Err(self.shape(field, InboundShapeKind::Boolean, self.offset)),
        }
    }

    /// Validate and discard one generic interface value. Generic dictionaries
    /// intentionally require strictly increasing byte-string keys, matching
    /// anacrolix's interface decoder rather than its typed struct decoder.
    fn skip_generic(&mut self, depth: usize) -> Result<(), InboundError> {
        match self.peek()? {
            b'0'..=b'9' => {
                self.parse_bytes("unknown value")?;
            }
            b'i' => {
                let (offset, raw) = self.parse_integer_raw()?;
                if !valid_generic_integer(raw) {
                    return Err(InboundError::Syntax {
                        kind: InboundSyntaxKind::InvalidInteger,
                        offset,
                    });
                }
            }
            b'l' => {
                self.start_list("unknown value", depth)?;
                while !self.end_container()? {
                    self.skip_generic(depth + 1)?;
                }
            }
            b'd' => {
                self.start_dictionary("unknown value", depth)?;
                let mut previous: Option<&[u8]> = None;
                while !self.end_container()? {
                    let (offset, key) = self.parse_bytes("unknown dictionary key")?;
                    if previous.is_some_and(|last| last.cmp(key) != Ordering::Less) {
                        return Err(InboundError::Syntax {
                            kind: InboundSyntaxKind::NonCanonicalUnknownDictionary,
                            offset,
                        });
                    }
                    previous = Some(key);
                    self.require_dictionary_value()?;
                    self.skip_generic(depth + 1)?;
                }
            }
            _ => {
                return Err(InboundError::Syntax {
                    kind: InboundSyntaxKind::UnexpectedValueType,
                    offset: self.offset,
                });
            }
        }
        Ok(())
    }

    /// Validate and discard one raw bencoded value with the same structural
    /// rules as anacrolix `bencode.Bytes`. In particular, raw dictionaries do
    /// not interpret key/value pairs or impose key ordering, and raw integers
    /// are only required to have a terminating `e`.
    fn skip_raw(&mut self, depth: usize) -> Result<(), InboundError> {
        match self.peek()? {
            b'0'..=b'9' => {
                self.parse_raw_bytes()?;
            }
            b'i' => {
                self.begin_value()?;
                self.offset += 1;
                while self
                    .input
                    .get(self.offset)
                    .is_some_and(|byte| *byte != b'e')
                {
                    self.offset += 1;
                }
                if self.input.get(self.offset) != Some(&b'e') {
                    return Err(self.unexpected_end());
                }
                self.offset += 1;
            }
            b'l' | b'd' => {
                let token = self.peek()?;
                self.start_container(
                    "raw BEP-44 value",
                    if token == b'l' {
                        InboundShapeKind::List
                    } else {
                        InboundShapeKind::Dictionary
                    },
                    token,
                    depth,
                )?;
                while !self.end_container()? {
                    self.skip_raw(depth + 1)?;
                }
            }
            _ => {
                return Err(InboundError::Syntax {
                    kind: InboundSyntaxKind::UnexpectedValueType,
                    offset: self.offset,
                });
            }
        }
        Ok(())
    }

    fn parse_raw_bytes(&mut self) -> Result<&'a [u8], InboundError> {
        let offset = self.begin_value()?;
        let length_start = self.offset;
        while self.input.get(self.offset).is_some_and(u8::is_ascii_digit) {
            self.offset += 1;
        }
        if self.input.get(self.offset) != Some(&b':') {
            return Err(InboundError::Syntax {
                kind: InboundSyntaxKind::InvalidByteStringLength,
                offset: length_start,
            });
        }
        let length = parse_decimal_usize(&self.input[length_start..self.offset]).ok_or(
            InboundError::Syntax {
                kind: InboundSyntaxKind::InvalidByteStringLength,
                offset,
            },
        )?;
        self.offset += 1;
        let end = self
            .offset
            .checked_add(length)
            .ok_or(InboundError::Syntax {
                kind: InboundSyntaxKind::InvalidByteStringLength,
                offset,
            })?;
        let value = self
            .input
            .get(self.offset..end)
            .ok_or_else(|| self.unexpected_end())?;
        self.offset = end;
        Ok(value)
    }

    fn parse_bytes(&mut self, field: &'static str) -> Result<(usize, &'a [u8]), InboundError> {
        let offset = self.begin_value()?;
        let Some(first) = self.input.get(self.offset).copied() else {
            return Err(self.unexpected_end());
        };
        if !first.is_ascii_digit() {
            return Err(self.shape(field, InboundShapeKind::ByteString, offset));
        }
        let length_start = self.offset;
        while self.input.get(self.offset).is_some_and(u8::is_ascii_digit) {
            self.offset += 1;
        }
        if self.input.get(self.offset) != Some(&b':') {
            return Err(InboundError::Syntax {
                kind: InboundSyntaxKind::InvalidByteStringLength,
                offset: length_start,
            });
        }
        let length_bytes = &self.input[length_start..self.offset];
        if length_bytes.len() > 1 && length_bytes[0] == b'0' {
            return Err(InboundError::Syntax {
                kind: InboundSyntaxKind::InvalidByteStringLength,
                offset: length_start,
            });
        }
        let length = parse_decimal_usize(length_bytes).ok_or(InboundError::Syntax {
            kind: InboundSyntaxKind::InvalidByteStringLength,
            offset: length_start,
        })?;
        self.offset += 1;
        let end = self
            .offset
            .checked_add(length)
            .ok_or(InboundError::Syntax {
                kind: InboundSyntaxKind::InvalidByteStringLength,
                offset: length_start,
            })?;
        let value = self
            .input
            .get(self.offset..end)
            .ok_or_else(|| self.unexpected_end())?;
        self.offset = end;
        Ok((offset, value))
    }

    fn parse_integer(&mut self, field: &'static str) -> Result<(usize, i64), InboundError> {
        let (offset, raw) = self.parse_integer_raw()?;
        if !valid_generic_integer(raw) {
            return Err(InboundError::Syntax {
                kind: InboundSyntaxKind::InvalidInteger,
                offset,
            });
        }
        let value = std::str::from_utf8(raw)
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .ok_or_else(|| self.shape(field, InboundShapeKind::Integer, offset))?;
        Ok((offset, value))
    }

    fn parse_integer_raw(&mut self) -> Result<(usize, &'a [u8]), InboundError> {
        let offset = self.begin_value()?;
        if self.input.get(self.offset) != Some(&b'i') {
            return Err(self.shape("integer", InboundShapeKind::Integer, offset));
        }
        self.offset += 1;
        let start = self.offset;
        while self
            .input
            .get(self.offset)
            .is_some_and(|byte| *byte != b'e')
        {
            self.offset += 1;
        }
        if self.input.get(self.offset) != Some(&b'e') {
            return Err(self.unexpected_end());
        }
        let raw = &self.input[start..self.offset];
        self.offset += 1;
        if raw.len() > 1 {
            let digits = raw.strip_prefix(b"-").unwrap_or(raw);
            if digits
                .first()
                .is_none_or(|first| !matches!(first, b'1'..=b'9'))
            {
                return Err(InboundError::Syntax {
                    kind: InboundSyntaxKind::InvalidInteger,
                    offset,
                });
            }
        }
        Ok((offset, raw))
    }

    fn start_dictionary(&mut self, field: &'static str, depth: usize) -> Result<(), InboundError> {
        self.start_container(field, InboundShapeKind::Dictionary, b'd', depth)
    }

    fn start_list(&mut self, field: &'static str, depth: usize) -> Result<(), InboundError> {
        self.start_container(field, InboundShapeKind::List, b'l', depth)
    }

    fn start_container(
        &mut self,
        field: &'static str,
        expected: InboundShapeKind,
        token: u8,
        depth: usize,
    ) -> Result<(), InboundError> {
        let offset = self.begin_value()?;
        if depth > MAX_INBOUND_NESTING_DEPTH {
            return Err(InboundError::Limit {
                kind: InboundLimitKind::NestingDepth,
                offset,
                actual: depth,
                limit: MAX_INBOUND_NESTING_DEPTH,
            });
        }
        if self.input.get(self.offset) != Some(&token) {
            return Err(self.shape(field, expected, offset));
        }
        self.offset += 1;
        Ok(())
    }

    fn end_container(&mut self) -> Result<bool, InboundError> {
        match self.input.get(self.offset) {
            Some(b'e') => {
                self.offset += 1;
                Ok(true)
            }
            Some(_) => Ok(false),
            None => Err(self.unexpected_end()),
        }
    }

    fn require_dictionary_value(&self) -> Result<(), InboundError> {
        match self.input.get(self.offset) {
            Some(b'e') => Err(InboundError::Syntax {
                kind: InboundSyntaxKind::MissingDictionaryValue,
                offset: self.offset,
            }),
            Some(_) => Ok(()),
            None => Err(self.unexpected_end()),
        }
    }

    fn begin_value(&mut self) -> Result<usize, InboundError> {
        let offset = self.offset;
        if self.offset >= self.input.len() {
            return Err(self.unexpected_end());
        }
        self.values += 1;
        if self.values > MAX_INBOUND_VALUES {
            return Err(InboundError::Limit {
                kind: InboundLimitKind::ValueCount,
                offset,
                actual: self.values,
                limit: MAX_INBOUND_VALUES,
            });
        }
        Ok(offset)
    }

    fn peek(&self) -> Result<u8, InboundError> {
        self.input
            .get(self.offset)
            .copied()
            .ok_or_else(|| self.unexpected_end())
    }

    fn shape(
        &self,
        field: &'static str,
        expected: InboundShapeKind,
        offset: usize,
    ) -> InboundError {
        InboundError::Shape {
            field,
            expected,
            offset,
        }
    }

    fn unexpected_end(&self) -> InboundError {
        InboundError::Syntax {
            kind: InboundSyntaxKind::UnexpectedEnd,
            offset: self.offset,
        }
    }
}

fn parse_decimal_usize(raw: &[u8]) -> Option<usize> {
    raw.iter().try_fold(0usize, |value, byte| {
        value
            .checked_mul(10)?
            .checked_add(usize::from(byte.checked_sub(b'0')?))
    })
}

fn valid_generic_integer(raw: &[u8]) -> bool {
    let digits = raw.strip_prefix(b"-").unwrap_or(raw);
    !digits.is_empty()
        && digits.iter().all(u8::is_ascii_digit)
        && (digits.len() == 1 || digits[0] != b'0')
        && !(raw.starts_with(b"-") && digits == b"0")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_dictionaries_are_unsorted_last_wins() {
        let wire = b"d1:y1:r1:t2:aa1:rd2:id20:000000000000000000002:id20:11111111111111111100e1:t2:bb1:y1:re";
        let decoded = KrpcMessage::decode_inbound(wire).unwrap();
        assert_eq!(decoded.transaction_id.as_bytes(), b"bb");
        assert_eq!(
            decoded.response.unwrap().id.as_bytes(),
            b"11111111111111111100"
        );
    }

    #[test]
    fn unknown_dictionaries_are_strict_and_do_not_allocate_a_tree() {
        assert!(KrpcMessage::decode_inbound(b"d1:t0:1:y0:1:zd1:a0:1:b0:ee").is_ok());
        assert!(matches!(
            KrpcMessage::decode_inbound(b"d1:t0:1:y0:1:zd1:b0:1:a0:ee"),
            Err(InboundError::Syntax {
                kind: InboundSyntaxKind::NonCanonicalUnknownDictionary,
                ..
            })
        ));
    }

    #[test]
    fn fixed_limits_and_trailing_data_fail_closed() {
        assert!(matches!(
            KrpcMessage::decode_inbound(&vec![b'x'; MAX_INBOUND_DATAGRAM_BYTES + 1]),
            Err(InboundError::Limit {
                kind: InboundLimitKind::DatagramBytes,
                ..
            })
        ));
        assert!(matches!(
            KrpcMessage::decode_inbound(b"d1:t0:1:y0:e0:"),
            Err(InboundError::Syntax {
                kind: InboundSyntaxKind::TrailingData,
                ..
            })
        ));
        assert!(KrpcMessage::decode_inbound(b"d1:t0:1:y0:1:zlllllll0:eeeeeeee").is_ok());
        assert!(matches!(
            KrpcMessage::decode_inbound(b"d1:t0:1:y0:1:zllllllll0:eeeeeeeee"),
            Err(InboundError::Limit {
                kind: InboundLimitKind::NestingDepth,
                ..
            })
        ));
    }

    #[test]
    fn go_boolean_edges_and_error_extras_match() {
        assert!(
            KrpcMessage::decode_inbound(b"d2:roie1:t0:1:y0:e")
                .unwrap()
                .read_only
        );
        assert!(
            KrpcMessage::decode_inbound(b"d2:roi-e1:t0:1:y0:e")
                .unwrap()
                .read_only
        );
        let error = KrpcMessage::decode_inbound(b"d1:eli201e4:oopsd1:a0:ee1:t0:1:y1:ee")
            .unwrap()
            .error
            .unwrap();
        assert_eq!(error.code, 201);
        assert_eq!(error.message.as_bytes(), b"oops");
    }
}
