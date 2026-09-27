// paho-mqtt-codec/src/properties.rs
//
// This file is part of the Eclipse Paho MQTT Rust Codec library.

/*******************************************************************************
 * Copyright (c) 2026 Frank Pagliughi <fpagliughi@mindspring.com>
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Frank Pagliughi - initial implementation and documentation
 *******************************************************************************/

//! MQTT v5 properties.
//!
//! Properties are key/value pairs carried by MQTT v5 packets. Each property
//! has a fixed wire type determined by its identifier. The [`Properties`]
//! collection encodes and decodes the length-prefixed property list that
//! appears in the variable header of every v5 packet type that supports them.
//!
//! Properties exist only in v5. On a v3 connection nothing is read from the
//! wire and an empty list writes nothing; encoding a non-empty list for v3
//! is an error rather than a silent drop.

use crate::{
    Error, MqttVersion, PacketType, Result, String, Vec, varlen,
    wire::{get_binary, get_utf8, put_binary, put_utf8},
};
use bytes::{Buf, BufMut, Bytes};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/////////////////////////////////////////////////////////////////////////////

/// The wire encoding type of an MQTT v5 property value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum PropertyType {
    /// Single byte.
    Byte,
    /// Two-byte big-endian unsigned integer.
    U16,
    /// Four-byte big-endian unsigned integer.
    U32,
    /// Variable-byte integer (1–4 bytes).
    VarInt,
    /// Two-byte length-prefixed binary blob.
    Binary,
    /// Two-byte length-prefixed UTF-8 string.
    String,
    /// Two UTF-8 strings (key then value), each length-prefixed.
    StringPair,
}

///////////////////////////////////////////////////////////////////

/// MQTT v5 property identifiers.
///
/// Each identifier has a fixed associated wire type (see [`PropertyId::value_type`]).
/// The numeric discriminants match the values on the wire.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum PropertyId {
    /// Payload is unspecified bytes (0) or UTF-8 text (1). PUBLISH, Will.
    PayloadFormatIndicator = 0x01,
    /// Message lifetime in seconds. PUBLISH, Will.
    MessageExpiryInterval = 0x02,
    /// Description of the payload content, e.g. a MIME type. PUBLISH, Will.
    ContentType = 0x03,
    /// Topic for a response in a request/response exchange. PUBLISH, Will.
    ResponseTopic = 0x08,
    /// Opaque data correlating a response with its request. PUBLISH, Will.
    CorrelationData = 0x09,
    /// Identifier tying a delivered PUBLISH to the subscription that matched
    /// it. SUBSCRIBE (once), PUBLISH (may repeat).
    SubscriptionIdentifier = 0x0B,
    /// How long the session survives after disconnect, in seconds.
    /// CONNECT, CONNACK, DISCONNECT.
    SessionExpiryInterval = 0x11,
    /// Client identifier the server assigned when the client sent none.
    /// CONNACK.
    AssignedClientIdentifier = 0x12,
    /// Keep-alive the server requires, overriding the client's. CONNACK.
    ServerKeepAlive = 0x13,
    /// Name of the extended authentication method. CONNECT, CONNACK, AUTH.
    AuthenticationMethod = 0x15,
    /// Method-specific authentication data. CONNECT, CONNACK, AUTH.
    AuthenticationData = 0x16,
    /// Whether the server may send Reason Strings and User Properties on
    /// failures other than PUBLISH, CONNACK, and DISCONNECT. CONNECT.
    RequestProblemInformation = 0x17,
    /// Delay before the server publishes the Will, in seconds. Will.
    WillDelayInterval = 0x18,
    /// Whether the client wants Response Information in CONNACK. CONNECT.
    RequestResponseInformation = 0x19,
    /// Basis for building response topics. CONNACK.
    ResponseInformation = 0x1A,
    /// Another server the client should use. CONNACK, DISCONNECT.
    ServerReference = 0x1C,
    /// Human-readable explanation of a reason code. Any acknowledgement,
    /// DISCONNECT, AUTH.
    ReasonString = 0x1F,
    /// Most QoS 1 and 2 PUBLISH packets the sender will accept in flight.
    /// CONNECT, CONNACK.
    ReceiveMaximum = 0x21,
    /// Highest topic alias the sender will accept. CONNECT, CONNACK.
    TopicAliasMaximum = 0x22,
    /// Integer standing in for the topic name. PUBLISH.
    TopicAlias = 0x23,
    /// Highest QoS the server supports (0 or 1). CONNACK.
    MaximumQoS = 0x24,
    /// Whether the server supports retained messages. CONNACK.
    RetainAvailable = 0x25,
    /// Application-defined name/value pair. Every packet except PINGREQ
    /// and PINGRESP; may repeat.
    UserProperty = 0x26,
    /// Largest packet the sender will accept. CONNECT, CONNACK.
    MaximumPacketSize = 0x27,
    /// Whether the server supports wildcard subscriptions. CONNACK.
    WildcardSubscriptionAvailable = 0x28,
    /// Whether the server supports subscription identifiers. CONNACK.
    SubscriptionIdentifierAvailable = 0x29,
    /// Whether the server supports shared subscriptions. CONNACK.
    SharedSubscriptionAvailable = 0x2A,
}

impl PropertyId {
    /// Returns the wire encoding type for the value of this property.
    pub fn value_type(self) -> PropertyType {
        use PropertyId::*;
        use PropertyType::*;
        match self {
            PayloadFormatIndicator
            | RequestProblemInformation
            | RequestResponseInformation
            | MaximumQoS
            | RetainAvailable
            | WildcardSubscriptionAvailable
            | SubscriptionIdentifierAvailable
            | SharedSubscriptionAvailable => Byte,

            ServerKeepAlive | ReceiveMaximum | TopicAliasMaximum | TopicAlias => U16,

            MessageExpiryInterval
            | SessionExpiryInterval
            | WillDelayInterval
            | MaximumPacketSize => U32,

            SubscriptionIdentifier => VarInt,

            CorrelationData | AuthenticationData => Binary,

            ContentType
            | ResponseTopic
            | AssignedClientIdentifier
            | AuthenticationMethod
            | ResponseInformation
            | ServerReference
            | ReasonString => String,

            UserProperty => StringPair,
        }
    }

    /// Returns `true` if this property may appear more than once in some
    /// packet.
    ///
    /// Only `UserProperty` and `SubscriptionIdentifier` ever repeat; the
    /// specification makes a second occurrence of any other property a
    /// Protocol Error. `SubscriptionIdentifier` repeats only in PUBLISH —
    /// see [`is_repeatable_in`](Self::is_repeatable_in) for the
    /// context-aware form.
    pub fn is_repeatable(self) -> bool {
        matches!(self, Self::UserProperty | Self::SubscriptionIdentifier)
    }

    /// Returns `true` if this property may appear more than once in `ctx`.
    ///
    /// `UserProperty` may repeat anywhere it is allowed. A PUBLISH may carry
    /// several `SubscriptionIdentifier`s (one per matching subscription,
    /// §3.3.2.3.8) but a SUBSCRIBE may carry only one (§3.8.2.1.2).
    pub fn is_repeatable_in(self, ctx: impl Into<PropertyContext>) -> bool {
        match self {
            Self::UserProperty => true,
            Self::SubscriptionIdentifier => {
                ctx.into() == PropertyContext::Packet(PacketType::Publish)
            }
            _ => false,
        }
    }
}

/// Where a property appears: in a packet of a given type, or in the Will
/// section of a CONNECT, which has its own allowed set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PropertyContext {
    /// The properties of a packet of this type.
    Packet(PacketType),
    /// The Will properties inside a CONNECT.
    Will,
}

impl From<PacketType> for PropertyContext {
    fn from(ty: PacketType) -> Self {
        Self::Packet(ty)
    }
}

/// Bitmask of property identifiers (bit `n` = identifier `n`).
const fn ids(list: &[PropertyId]) -> u64 {
    let mut mask = 0u64;
    let mut i = 0;
    while i < list.len() {
        mask |= 1 << (list[i] as u8);
        i += 1;
    }
    mask
}

impl PropertyContext {
    /// The set of property identifiers the specification allows here
    /// (MQTT v5 §2.2.2.2, Table 2-4), as a bitmask over identifier values.
    fn allowed_ids(self) -> u64 {
        use PacketType as T;
        use PropertyId::*;
        match self {
            Self::Packet(T::Connect) => {
                const {
                    ids(&[
                        SessionExpiryInterval,
                        AuthenticationMethod,
                        AuthenticationData,
                        RequestProblemInformation,
                        RequestResponseInformation,
                        ReceiveMaximum,
                        TopicAliasMaximum,
                        UserProperty,
                        MaximumPacketSize,
                    ])
                }
            }
            Self::Packet(T::ConnAck) => {
                const {
                    ids(&[
                        SessionExpiryInterval,
                        AssignedClientIdentifier,
                        ServerKeepAlive,
                        AuthenticationMethod,
                        AuthenticationData,
                        ResponseInformation,
                        ServerReference,
                        ReasonString,
                        ReceiveMaximum,
                        TopicAliasMaximum,
                        MaximumQoS,
                        RetainAvailable,
                        UserProperty,
                        MaximumPacketSize,
                        WildcardSubscriptionAvailable,
                        SubscriptionIdentifierAvailable,
                        SharedSubscriptionAvailable,
                    ])
                }
            }
            Self::Packet(T::Publish) => {
                const {
                    ids(&[
                        PayloadFormatIndicator,
                        MessageExpiryInterval,
                        ContentType,
                        ResponseTopic,
                        CorrelationData,
                        SubscriptionIdentifier,
                        TopicAlias,
                        UserProperty,
                    ])
                }
            }
            Self::Will => {
                const {
                    ids(&[
                        PayloadFormatIndicator,
                        MessageExpiryInterval,
                        ContentType,
                        ResponseTopic,
                        CorrelationData,
                        WillDelayInterval,
                        UserProperty,
                    ])
                }
            }
            Self::Packet(T::PubAck | T::PubRec | T::PubRel | T::PubComp) => {
                const { ids(&[ReasonString, UserProperty]) }
            }
            Self::Packet(T::Subscribe) => const { ids(&[SubscriptionIdentifier, UserProperty]) },
            Self::Packet(T::SubAck) => const { ids(&[ReasonString, UserProperty]) },
            Self::Packet(T::Unsubscribe) => const { ids(&[UserProperty]) },
            Self::Packet(T::UnsubAck) => const { ids(&[ReasonString, UserProperty]) },
            Self::Packet(T::Disconnect) => {
                const {
                    ids(&[
                        SessionExpiryInterval,
                        ServerReference,
                        ReasonString,
                        UserProperty,
                    ])
                }
            }
            Self::Packet(T::Auth) => {
                const {
                    ids(&[
                        AuthenticationMethod,
                        AuthenticationData,
                        ReasonString,
                        UserProperty,
                    ])
                }
            }
            Self::Packet(T::PingReq | T::PingResp) => 0,
        }
    }
}

impl PropertyId {
    /// Returns `true` if the specification allows this property in the
    /// given context (MQTT v5 §2.2.2.2, Table 2-4). Including a property
    /// where it is not allowed is a Protocol Error.
    #[inline]
    pub fn is_allowed_in(self, ctx: impl Into<PropertyContext>) -> bool {
        ctx.into().allowed_ids() & (1 << (self as u8)) != 0
    }
}

impl TryFrom<u8> for PropertyId {
    type Error = Error;

    /// Converts a wire byte into a property identifier.
    ///
    /// Returns [`Error::MalformedPacket`] for an identifier the specification
    /// does not define, which §2.2.2.2 states outright. Contrast
    /// [`ReasonCode::try_from`](crate::ReasonCode), where an undefined value
    /// is a `ProtocolError`: the identifier is part of the packet's format,
    /// whereas a reason code is a correctly-typed field whose value is merely
    /// out of range.
    fn try_from(b: u8) -> Result<Self> {
        use PropertyId::*;
        match b {
            0x01 => Ok(PayloadFormatIndicator),
            0x02 => Ok(MessageExpiryInterval),
            0x03 => Ok(ContentType),
            0x08 => Ok(ResponseTopic),
            0x09 => Ok(CorrelationData),
            0x0B => Ok(SubscriptionIdentifier),
            0x11 => Ok(SessionExpiryInterval),
            0x12 => Ok(AssignedClientIdentifier),
            0x13 => Ok(ServerKeepAlive),
            0x15 => Ok(AuthenticationMethod),
            0x16 => Ok(AuthenticationData),
            0x17 => Ok(RequestProblemInformation),
            0x18 => Ok(WillDelayInterval),
            0x19 => Ok(RequestResponseInformation),
            0x1A => Ok(ResponseInformation),
            0x1C => Ok(ServerReference),
            0x1F => Ok(ReasonString),
            0x21 => Ok(ReceiveMaximum),
            0x22 => Ok(TopicAliasMaximum),
            0x23 => Ok(TopicAlias),
            0x24 => Ok(MaximumQoS),
            0x25 => Ok(RetainAvailable),
            0x26 => Ok(UserProperty),
            0x27 => Ok(MaximumPacketSize),
            0x28 => Ok(WildcardSubscriptionAvailable),
            0x29 => Ok(SubscriptionIdentifierAvailable),
            0x2A => Ok(SharedSubscriptionAvailable),
            _ => Err(Error::MalformedPacket),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////

/// The typed value of an MQTT v5 property.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum PropertyValue {
    /// Single-byte value.
    Byte(u8),
    /// Two-byte unsigned integer.
    U16(u16),
    /// Four-byte unsigned integer.
    U32(u32),
    /// Variable-byte integer.
    VarInt(u32),
    /// Length-prefixed binary data.
    Binary(Bytes),
    /// Length-prefixed UTF-8 string.
    String(String),
    /// Two length-prefixed UTF-8 strings (key, value).
    StringPair(String, String),
}

impl PropertyValue {
    /// Returns the wire encoding type of this value.
    pub fn property_type(&self) -> PropertyType {
        match self {
            PropertyValue::Byte(_) => PropertyType::Byte,
            PropertyValue::U16(_) => PropertyType::U16,
            PropertyValue::U32(_) => PropertyType::U32,
            PropertyValue::VarInt(_) => PropertyType::VarInt,
            PropertyValue::Binary(_) => PropertyType::Binary,
            PropertyValue::String(_) => PropertyType::String,
            PropertyValue::StringPair(..) => PropertyType::StringPair,
        }
    }

    /// Number of bytes this value occupies on the wire (excluding the property-ID byte).
    pub fn encoded_size(&self) -> usize {
        match self {
            PropertyValue::Byte(_) => 1,
            PropertyValue::U16(_) => 2,
            PropertyValue::U32(_) => 4,
            PropertyValue::VarInt(n) => varlen::encoded_size(*n),
            PropertyValue::Binary(b) => 2 + b.len(),
            PropertyValue::String(s) => 2 + s.len(),
            PropertyValue::StringPair(k, v) => 2 + k.len() + 2 + v.len(),
        }
    }

    fn encode<B: BufMut>(&self, dst: &mut B) -> Result<()> {
        match self {
            PropertyValue::Byte(b) => dst.put_u8(*b),
            PropertyValue::U16(n) => dst.put_u16(*n),
            PropertyValue::U32(n) => dst.put_u32(*n),
            PropertyValue::VarInt(n) => varlen::encode(*n, dst)?,
            PropertyValue::Binary(b) => put_binary(b, dst)?,
            PropertyValue::String(s) => put_utf8(s, dst)?,
            PropertyValue::StringPair(k, v) => {
                put_utf8(k, dst)?;
                put_utf8(v, dst)?;
            }
        }
        Ok(())
    }

    fn decode_typed<B: Buf>(typ: PropertyType, src: &mut B) -> Result<Self> {
        match typ {
            PropertyType::Byte => {
                if src.remaining() < 1 {
                    return Err(Error::PacketTooShort);
                }
                Ok(PropertyValue::Byte(src.get_u8()))
            }
            PropertyType::U16 => {
                if src.remaining() < 2 {
                    return Err(Error::PacketTooShort);
                }
                Ok(PropertyValue::U16(src.get_u16()))
            }
            PropertyType::U32 => {
                if src.remaining() < 4 {
                    return Err(Error::PacketTooShort);
                }
                Ok(PropertyValue::U32(src.get_u32()))
            }
            PropertyType::VarInt => {
                let n = varlen::decode(src)?.ok_or(Error::PacketTooShort)?;
                Ok(PropertyValue::VarInt(n))
            }
            PropertyType::Binary => Ok(PropertyValue::Binary(get_binary(src)?)),
            PropertyType::String => Ok(PropertyValue::String(get_utf8(src)?)),
            PropertyType::StringPair => {
                let k = get_utf8(src)?;
                let v = get_utf8(src)?;
                Ok(PropertyValue::StringPair(k, v))
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////

/// A single MQTT v5 property: an identifier paired with its typed value.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Property {
    /// The property identifier.
    pub id: PropertyId,
    /// The property value, typed according to `id`.
    pub value: PropertyValue,
}

impl Property {
    /// Creates a new property.
    pub fn new(id: PropertyId, value: PropertyValue) -> Self {
        Self { id, value }
    }

    /// Create a new byte (u8) property.
    pub fn new_byte(code: PropertyId, val: u8) -> Self {
        Self::new(code, PropertyValue::Byte(val))
    }

    /// Create a new u16 property.
    pub fn new_u16(code: PropertyId, val: u16) -> Self {
        Self::new(code, PropertyValue::U16(val))
    }

    /// Create a new u32 property.
    pub fn new_u32(code: PropertyId, val: u32) -> Self {
        Self::new(code, PropertyValue::U32(val))
    }

    /// Create a new VarInt property.
    pub fn new_var_int<I>(code: PropertyId, val: I) -> Self
    where
        I: Into<u32>,
    {
        Self::new(code, PropertyValue::VarInt(val.into()))
    }

    /// Create a new binary property.
    pub fn new_binary<B>(code: PropertyId, val: B) -> Self
    where
        B: Into<Bytes>,
    {
        Self::new(code, PropertyValue::Binary(val.into()))
    }

    /// Create a new string property.
    pub fn new_string<S>(code: PropertyId, s: S) -> Self
    where
        S: Into<String>,
    {
        Self::new(code, PropertyValue::String(s.into()))
    }

    /// Create a new string property.
    pub fn new_string_pair<K, V>(code: PropertyId, key: K, val: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        Self::new(code, PropertyValue::StringPair(key.into(), val.into()))
    }

    /// Wire size: 1 byte for the property ID plus the encoded value size.
    pub fn encoded_size(&self) -> usize {
        1 + self.value.encoded_size()
    }

    /// Checks that the value has the wire type the identifier requires and
    /// is within the range the specification allows for that property.
    ///
    /// Several properties restrict the value beyond its type: the one-byte
    /// flags must be 0 or 1; `ReceiveMaximum`, `MaximumPacketSize`,
    /// `TopicAlias`, and `SubscriptionIdentifier` must be non-zero; and a
    /// `ResponseTopic` may not contain wildcards. Returns
    /// [`Error::ProtocolError`] on any violation.
    pub fn validate(&self) -> Result<()> {
        if self.value.property_type() != self.id.value_type() {
            return Err(Error::ProtocolError);
        }
        self.check_range()
    }

    /// The value-range half of [`validate`](Self::validate), for values whose
    /// type is already known to match (i.e. just decoded from the wire).
    #[inline]
    fn check_range(&self) -> Result<()> {
        use PropertyId::*;
        use PropertyValue::*;
        let ok = match self.id {
            PayloadFormatIndicator
            | RequestProblemInformation
            | RequestResponseInformation
            | MaximumQoS
            | RetainAvailable
            | WildcardSubscriptionAvailable
            | SubscriptionIdentifierAvailable
            | SharedSubscriptionAvailable => matches!(self.value, Byte(0 | 1)),
            ReceiveMaximum | TopicAlias => matches!(self.value, U16(1..)),
            MaximumPacketSize => matches!(self.value, U32(1..)),
            SubscriptionIdentifier => matches!(self.value, VarInt(1..)),
            ResponseTopic => {
                matches!(&self.value, String(s) if !s.bytes().any(|b| b == b'+' || b == b'#'))
            }
            _ => true,
        };
        if ok {
            Ok(())
        } else {
            Err(Error::ProtocolError)
        }
    }

    fn encode<B: BufMut>(&self, dst: &mut B) -> Result<()> {
        dst.put_u8(self.id as u8);
        self.value.encode(dst)
    }

    fn decode<B: Buf>(src: &mut B) -> Result<Self> {
        if src.remaining() < 1 {
            return Err(Error::PacketTooShort);
        }
        let id = PropertyId::try_from(src.get_u8())?;
        let value = PropertyValue::decode_typed(id.value_type(), src)?;
        let prop = Self { id, value };
        // The type is right by construction; only the range needs checking.
        prop.check_range()?;
        Ok(prop)
    }
}

/////////////////////////////////////////////////////////////////////////////

/// Tracks which non-repeatable property identifiers have been seen while
/// walking a property list, and whether the list has been found invalid.
///
/// Identifiers are all below 0x2B, so one bit each; bits for repeatable
/// identifiers are simply never set. Bit 63 is the "invalid" flag, kept
/// here rather than in a separate field so [`Properties`] stays small
/// enough to embed in error types.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct SeenIds(u64);

impl SeenIds {
    const REPEATABLE: u64 =
        (1 << PropertyId::UserProperty as u8) | (1 << PropertyId::SubscriptionIdentifier as u8);
    const INVALID: u64 = 1 << 63;

    /// Records `id`; returns [`Error::ProtocolError`] if a non-repeatable
    /// identifier has already been recorded.
    #[inline]
    fn note(&mut self, id: PropertyId) -> Result<()> {
        let bit = 1u64 << (id as u8);
        if self.0 & bit != 0 {
            return Err(Error::ProtocolError);
        }
        self.0 |= bit & !Self::REPEATABLE;
        Ok(())
    }

    /// Returns `true` if `id` has been recorded. Only meaningful for
    /// non-repeatable identifiers, which are the only ones `note` records.
    #[inline]
    fn has(self, id: PropertyId) -> bool {
        self.0 & (1u64 << (id as u8)) != 0
    }

    fn mark_invalid(&mut self) {
        self.0 |= Self::INVALID;
    }

    fn is_invalid(&self) -> bool {
        self.0 & Self::INVALID != 0
    }
}

/// MQTT v5 properties: an ordered list of [`Property`] pairs.
///
/// The same property identifier may appear more than once (required for
/// `UserProperty`; allowed for `SubscriptionIdentifier`). Use [`Properties::get`]
/// for the first occurrence and [`Properties::iter`] when you need all.
///
/// On v3 connections nothing is read from the wire; encoding a non-empty
/// list for v3 is an [`Error::ProtocolError`] (see [`encode`](Self::encode)).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(from = "Vec<Property>", into = "Vec<Property>")
)]
pub struct Properties {
    props: Vec<Property>,
    /// Sum of `encoded_size()` over `props`; kept in step by every mutator
    /// so encoding never has to walk the list to size the length prefix.
    payload_size: usize,
    /// Non-repeatable identifiers pushed so far (for duplicate detection),
    /// plus a flag set if any pushed property failed validation or repeated
    /// a non-repeatable identifier; `encode` then refuses the whole list.
    seen: SeenIds,
}

// `PartialEq` is derived over the cached fields as well as `props`, which is
// only sound because every way of building a `Properties` — `push`, and so
// `Extend` and `From<Vec<Property>>`, plus `decode_allowed` — maintains them
// identically from the same input. A future mutator that updated `props`
// without them would make equality lie; keep them in step or compare `props`
// alone.

impl Properties {
    /// Creates an empty property list.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if there are no properties.
    pub fn is_empty(&self) -> bool {
        self.props.is_empty()
    }

    /// Returns the number of properties.
    pub fn len(&self) -> usize {
        self.props.len()
    }

    /// Appends `prop` to the end of the list.
    ///
    /// The property is checked here rather than on every encode: if its
    /// value is out of range for its identifier (see [`Property::validate`])
    /// or it repeats a non-repeatable identifier, the list is marked invalid
    /// and [`encode`](Self::encode) will return [`Error::ProtocolError`].
    pub fn push(&mut self, prop: Property) {
        if prop.validate().is_err() || self.seen.note(prop.id).is_err() {
            self.seen.mark_invalid();
        }
        self.payload_size += prop.encoded_size();
        self.props.push(prop);
    }

    /// Adds an single-byte property to the collection.
    pub fn push_byte(&mut self, code: PropertyId, val: u8) {
        self.push(Property::new_byte(code, val))
    }

    /// Adds an two-byte integer property to the collection.
    pub fn push_u16(&mut self, code: PropertyId, val: u16) {
        self.push(Property::new_u16(code, val))
    }

    /// Adds a four-byte integer property to the collection.
    pub fn push_u32(&mut self, code: PropertyId, val: u32) {
        self.push(Property::new_u32(code, val))
    }

    /// Adds a variable-byte integer property to the collection.
    ///
    /// `SubscriptionIdentifier` is the only one, and it is encoded in one to
    /// four bytes depending on its value rather than always four.
    pub fn push_var_int<I>(&mut self, code: PropertyId, val: I)
    where
        I: Into<u32>,
    {
        self.push(Property::new_var_int(code, val))
    }

    /// Adds a binary property to the collection
    pub fn push_binary<V>(&mut self, code: PropertyId, bin: V)
    where
        V: Into<Bytes>,
    {
        self.push(Property::new_binary(code, bin))
    }

    /// Adds a string property to the collection
    pub fn push_string<S>(&mut self, code: PropertyId, s: S)
    where
        S: Into<String>,
    {
        self.push(Property::new_string(code, s))
    }

    /// Adds a string pair property to the collection
    pub fn push_string_pair<K, V>(&mut self, code: PropertyId, key: K, val: V)
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.push(Property::new_string_pair(code, key, val))
    }

    /// Returns the value of the first property with the given `id`, or `None`.
    ///
    /// This is the borrowing accessor. The typed `get_*` methods below return
    /// the value already unwrapped, at the cost of a clone for the owned
    /// types.
    pub fn get(&self, id: PropertyId) -> Option<&PropertyValue> {
        self.props.iter().find(|p| p.id == id).map(|p| &p.value)
    }

    /// Returns the value of the `idx`-th property with the given `id`, or
    /// `None`.
    ///
    /// Only `UserProperty`, and `SubscriptionIdentifier` in a PUBLISH, may
    /// appear more than once, so `idx` is 0 for every other identifier.
    pub fn get_at(&self, id: PropertyId, idx: usize) -> Option<&PropertyValue> {
        self.props
            .iter()
            .filter(|p| p.id == id)
            .nth(idx)
            .map(|p| &p.value)
    }

    /// Returns the value of the first property with the given `id` as a byte,
    /// or `None` if it is absent or not a byte property.
    pub fn get_byte(&self, id: PropertyId) -> Option<u8> {
        self.get_byte_at(id, 0)
    }

    /// Returns the `idx`-th `id` property's value as a byte.
    pub fn get_byte_at(&self, id: PropertyId, idx: usize) -> Option<u8> {
        match self.get_at(id, idx) {
            Some(PropertyValue::Byte(v)) => Some(*v),
            _ => None,
        }
    }

    /// Returns the value of the first property with the given `id` as a
    /// two-byte integer, or `None` if it is absent or not a `U16` property.
    pub fn get_u16(&self, id: PropertyId) -> Option<u16> {
        self.get_u16_at(id, 0)
    }

    /// Returns the `idx`-th `id` property's value as a two-byte integer.
    pub fn get_u16_at(&self, id: PropertyId, idx: usize) -> Option<u16> {
        match self.get_at(id, idx) {
            Some(PropertyValue::U16(v)) => Some(*v),
            _ => None,
        }
    }

    /// Returns the value of the first property with the given `id` as a
    /// 32-bit integer, or `None` if it is absent or holds some other type.
    ///
    /// Both wire forms of a 32-bit value are accepted: a `U32` and the
    /// variable-byte integer of a `SubscriptionIdentifier`. Use
    /// [`get_var_int`](Self::get_var_int) when the distinction matters.
    pub fn get_u32(&self, id: PropertyId) -> Option<u32> {
        self.get_u32_at(id, 0)
    }

    /// Returns the `idx`-th `id` property's value as a 32-bit integer, from
    /// either a `U32` or a variable-byte integer.
    pub fn get_u32_at(&self, id: PropertyId, idx: usize) -> Option<u32> {
        match self.get_at(id, idx) {
            Some(PropertyValue::U32(v) | PropertyValue::VarInt(v)) => Some(*v),
            _ => None,
        }
    }

    /// Returns the value of the first property with the given `id` as a
    /// variable-byte integer, or `None` if it is absent or not a `VarInt`
    /// property.
    ///
    /// Unlike [`get_u32`](Self::get_u32) this is exact about the wire form, so
    /// it reads a `SubscriptionIdentifier` and nothing else.
    pub fn get_var_int(&self, id: PropertyId) -> Option<u32> {
        self.get_var_int_at(id, 0)
    }

    /// Returns the `idx`-th `id` property's value as a variable-byte integer.
    ///
    /// A PUBLISH may carry several `SubscriptionIdentifier`s, one per matching
    /// subscription (§3.3.2.3.8), which is what `idx` is for.
    pub fn get_var_int_at(&self, id: PropertyId, idx: usize) -> Option<u32> {
        match self.get_at(id, idx) {
            Some(PropertyValue::VarInt(v)) => Some(*v),
            _ => None,
        }
    }

    /// Returns the value of the first property with the given `id` as binary
    /// data, or `None` if it is absent or not a binary property.
    ///
    /// Cloning [`Bytes`] shares the buffer rather than copying it.
    pub fn get_binary(&self, id: PropertyId) -> Option<Bytes> {
        self.get_binary_at(id, 0)
    }

    /// Returns the `idx`-th `id` property's value as binary data.
    pub fn get_binary_at(&self, id: PropertyId, idx: usize) -> Option<Bytes> {
        match self.get_at(id, idx) {
            Some(PropertyValue::Binary(v)) => Some(v.clone()),
            _ => None,
        }
    }

    /// Returns the value of the first property with the given `id` as a
    /// string, or `None` if it is absent or not a string property.
    ///
    /// This allocates; [`get`](Self::get) borrows the string in place.
    pub fn get_string(&self, id: PropertyId) -> Option<String> {
        self.get_string_at(id, 0)
    }

    /// Returns the `idx`-th `id` property's value as a string.
    pub fn get_string_at(&self, id: PropertyId, idx: usize) -> Option<String> {
        match self.get_at(id, idx) {
            Some(PropertyValue::String(v)) => Some(v.clone()),
            _ => None,
        }
    }

    /// Returns the value of the first property with the given `id` as a
    /// key/value pair, or `None` if it is absent or not a string-pair
    /// property.
    ///
    /// `UserProperty` is the only string pair, and it may repeat — see
    /// [`get_string_pair_at`](Self::get_string_pair_at).
    pub fn get_string_pair(&self, id: PropertyId) -> Option<(String, String)> {
        self.get_string_pair_at(id, 0)
    }

    /// Returns the `idx`-th `id` property's value as a key/value pair.
    pub fn get_string_pair_at(&self, id: PropertyId, idx: usize) -> Option<(String, String)> {
        match self.get_at(id, idx) {
            Some(PropertyValue::StringPair(k, v)) => Some((k.clone(), v.clone())),
            _ => None,
        }
    }

    /// Iterates over the values of every property with the given `id`, in
    /// order.
    ///
    /// Only `UserProperty`, and `SubscriptionIdentifier` in a PUBLISH, can
    /// yield more than one. This is v0.14's `iter(code)`; the name differs
    /// because [`iter`](Self::iter) here walks the whole collection.
    pub fn get_all(&self, id: PropertyId) -> impl Iterator<Item = &PropertyValue> {
        self.props
            .iter()
            .filter(move |p| p.id == id)
            .map(|p| &p.value)
    }

    /// Adds a user property.
    ///
    /// Shorthand for [`push_string_pair`](Self::push_string_pair) with
    /// `UserProperty`, the only string-pair property and the only one an
    /// application defines for itself.
    pub fn push_user<K, V>(&mut self, key: K, val: V)
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.push_string_pair(PropertyId::UserProperty, key, val)
    }

    /// Iterates over the user properties as key/value pairs, in order.
    ///
    /// Keys are not unique: an application may send the same key more than
    /// once, and the order is the order they were sent in, which MQTT
    /// preserves (§3.3.2.3.7).
    ///
    /// Unlike v0.14 this borrows rather than allocating a `String` per item;
    /// `.map(|(k, v)| (k.to_string(), v.to_string()))` recovers the owned form.
    pub fn user_iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.props.iter().filter_map(|p| match (&p.id, &p.value) {
            (PropertyId::UserProperty, PropertyValue::StringPair(k, v)) => {
                Some((k.as_str(), v.as_str()))
            }
            _ => None,
        })
    }

    /// Returns the value of the first user property with the given `key`, or
    /// `None`.
    ///
    /// Use [`user_iter`](Self::user_iter) when a key may appear more than
    /// once. Borrows rather than allocating, unlike v0.14's
    /// `find_user_property`; `.map(String::from)` recovers the owned form.
    pub fn get_user(&self, key: &str) -> Option<&str> {
        self.user_iter().find(|(k, _)| *k == key).map(|(_, v)| v)
    }

    /// Obsolete name for [`get_user`](Self::get_user), which this calls.
    ///
    /// Kept so code written against the v0.14 client, where this was the only
    /// spelling, still compiles. Note that it borrows where v0.14 returned an
    /// owned `String`.
    #[deprecated(note = "renamed to `get_user`")]
    pub fn find_user_property(&self, key: &str) -> Option<&str> {
        self.get_user(key)
    }

    /// Keeps only the properties for which `f` returns `true`, in order.
    ///
    /// This is the general form of [`remove`](Self::remove) and
    /// [`remove_user`](Self::remove_user), for a condition on the value rather
    /// than the identifier — dropping every `UserProperty` with an empty
    /// value, say, or stripping what a server may not forward.
    ///
    /// The cached wire size and seen-identifier set are recomputed if anything
    /// was dropped, so [`encoded_size`](Self::encoded_size) and
    /// [`encode`](Self::encode) stay correct.
    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&Property) -> bool,
    {
        let before = self.props.len();
        self.props.retain(f);
        if self.props.len() != before {
            self.rebuild_cache();
        }
    }

    /// Removes every property with the given `id`, returning how many were
    /// removed.
    pub fn remove(&mut self, id: PropertyId) -> usize {
        let before = self.props.len();
        self.retain(|p| p.id != id);
        before - self.props.len()
    }

    /// Removes every user property with the given `key`, returning how many
    /// were removed.
    pub fn remove_user(&mut self, key: &str) -> usize {
        let before = self.props.len();
        self.retain(|p| {
            !matches!(
                (&p.id, &p.value),
                (PropertyId::UserProperty, PropertyValue::StringPair(k, _)) if k == key
            )
        });
        before - self.props.len()
    }

    /// Removes every property.
    pub fn clear(&mut self) {
        self.props.clear();
        self.payload_size = 0;
        self.seen = SeenIds::default();
    }

    /// Recomputes the cached wire size and seen-identifier set from the
    /// properties that remain.
    ///
    /// Removal is the one operation that cannot update them incrementally: a
    /// cleared identifier bit may need to stay set for a duplicate further
    /// along, and the invalid flag does not record which property set it. This
    /// means removing the offending property also clears that flag, which is
    /// the behaviour a caller fixing up a list would expect.
    fn rebuild_cache(&mut self) {
        self.payload_size = self.props.iter().map(Property::encoded_size).sum();
        let mut seen = SeenIds::default();
        for prop in &self.props {
            if prop.validate().is_err() || seen.note(prop.id).is_err() {
                seen.mark_invalid();
            }
        }
        self.seen = seen;
    }

    /// Iterates over all properties in order.
    ///
    /// See [`get_all`](Self::get_all) to walk just one identifier, and
    /// [`user_iter`](Self::user_iter) for the user properties as pairs.
    pub fn iter(&self) -> impl Iterator<Item = &Property> {
        self.props.iter()
    }

    /// Total bytes occupied by all properties on the wire (excluding the VBI prefix).
    fn payload_size(&self) -> usize {
        self.payload_size
    }

    /// Number of bytes this encodes to on the wire, including the VBI length prefix.
    ///
    /// Returns 0 for v3 connections — properties are not present on the wire.
    ///
    /// A list too long for a variable-byte integer to describe has no valid
    /// encoding at all; the prefix is sized as if it were the maximum rather
    /// than wrapping, and [`encode`](Self::encode) reports the real error.
    pub fn encoded_size(&self, ver: MqttVersion) -> usize {
        if ver < MqttVersion::V5 {
            0
        } else {
            let data_len = self.payload_size();
            let capped = data_len.min(varlen::MAX_VARLEN as usize) as u32;
            varlen::encoded_size(capped) + data_len
        }
    }

    /// Encodes the properties to the wire: VBI(payload_len) followed by each property.
    ///
    /// Returns [`Error::VarLenOverflow`] if the list is longer than a
    /// variable-byte integer can describe ([`varlen::MAX_VARLEN`] bytes).
    ///
    /// Properties do not exist before v5, so on a v3 connection an empty
    /// list writes nothing and a non-empty one is [`Error::ProtocolError`]:
    /// the caller built a packet the connection cannot carry, and dropping
    /// the properties silently would hide that.
    pub fn encode<B: BufMut>(&self, dst: &mut B, ver: MqttVersion) -> Result<()> {
        if ver < MqttVersion::V5 {
            return if self.is_empty() {
                Ok(())
            } else {
                Err(Error::ProtocolError)
            };
        }
        if self.seen.is_invalid() {
            return Err(Error::ProtocolError);
        }
        // A list longer than a variable-byte integer can describe cannot be
        // encoded; casting would wrap and emit a length prefix that disagrees
        // with the bytes that follow.
        let data_len = u32::try_from(self.payload_size()).map_err(|_| Error::VarLenOverflow)?;
        varlen::encode(data_len, dst)?;
        for prop in &self.props {
            prop.encode(dst)?;
        }
        Ok(())
    }

    /// Authentication Data is meaningless without the method that defines it,
    /// so including it alone is a Protocol Error (§3.1.2.11.10 for CONNECT,
    /// §3.2.2.3.18 for CONNACK, §3.15.2.2.3 for AUTH — the three contexts that
    /// allow the pair at all).
    ///
    /// Both identifiers are non-repeatable, so `seen` already records them and
    /// this costs no extra walk. It cannot be checked in
    /// [`push`](Self::push), which sees the properties one at a time and in
    /// any order.
    #[inline]
    fn check_auth_pair(seen: SeenIds) -> Result<()> {
        if seen.has(PropertyId::AuthenticationData) && !seen.has(PropertyId::AuthenticationMethod) {
            return Err(Error::ProtocolError);
        }
        Ok(())
    }

    /// Checks that every property is allowed in `ctx` (MQTT v5 §2.2.2.2),
    /// that none repeats where `ctx` forbids it — in practice, that a
    /// `SubscriptionIdentifier` appears at most once outside a PUBLISH
    /// (§3.8.2.1.2) — and that Authentication Data is accompanied by an
    /// Authentication Method. Returns [`Error::ProtocolError`] otherwise.
    ///
    /// Packet encoders call this; use it directly when assembling properties
    /// for a packet to catch a misplaced one early.
    #[inline]
    pub fn check_context(&self, ctx: impl Into<PropertyContext>) -> Result<()> {
        Self::check_auth_pair(self.seen)?;
        let ctx = ctx.into();
        let allowed = ctx.allowed_ids();
        let mut sub_ids = 0usize;
        for p in &self.props {
            if allowed & (1 << (p.id as u8)) == 0 {
                return Err(Error::ProtocolError);
            }
            if p.id == PropertyId::SubscriptionIdentifier {
                sub_ids += 1;
            }
        }
        if sub_ids > 1 && !PropertyId::SubscriptionIdentifier.is_repeatable_in(ctx) {
            return Err(Error::ProtocolError);
        }
        Ok(())
    }

    /// Decodes the properties from the wire: reads the VBI length prefix, then
    /// parses each property from exactly that many bytes.
    ///
    /// Accepts any property identifier in any position; see
    /// [`decode_in`](Self::decode_in) to also enforce which properties may
    /// appear in a given packet. Returns an empty collection for v3
    /// connections without consuming any bytes.
    ///
    /// Value ranges, repeated non-repeatable identifiers, and Authentication
    /// Data without an Authentication Method are rejected either way: those
    /// rules do not depend on which packet carries the properties.
    pub fn decode<B: Buf>(src: &mut B, ver: MqttVersion) -> Result<Self> {
        Self::decode_allowed(src, ver, u64::MAX, true)
    }

    /// Decodes the properties from the wire as [`decode`](Self::decode) does,
    /// and additionally rejects with [`Error::ProtocolError`] any property the
    /// specification does not allow in `ctx` (MQTT v5 §2.2.2.2), or a
    /// `SubscriptionIdentifier` repeated anywhere but a PUBLISH (§3.8.2.1.2).
    pub fn decode_in<B: Buf>(
        src: &mut B,
        ver: MqttVersion,
        ctx: impl Into<PropertyContext>,
    ) -> Result<Self> {
        let ctx = ctx.into();
        let sub_id_repeats = PropertyId::SubscriptionIdentifier.is_repeatable_in(ctx);
        Self::decode_allowed(src, ver, ctx.allowed_ids(), sub_id_repeats)
    }

    /// `allowed` is the identifier bitmask for the context; `sub_id_repeats`
    /// says whether a second `SubscriptionIdentifier` is permitted there.
    fn decode_allowed<B: Buf>(
        src: &mut B,
        ver: MqttVersion,
        allowed: u64,
        sub_id_repeats: bool,
    ) -> Result<Self> {
        if ver < MqttVersion::V5 {
            return Ok(Self::new());
        }

        let len = varlen::decode(src)?.ok_or(Error::PacketTooShort)? as usize;
        if src.remaining() < len {
            return Err(Error::PacketTooShort);
        }
        let mut payload = src.copy_to_bytes(len);
        let mut props = Vec::new();
        let mut seen = SeenIds::default();
        let mut payload_size = 0;
        let mut saw_sub_id = false;
        while payload.has_remaining() {
            let prop = Property::decode(&mut payload)?; // validated
            if allowed & (1 << (prop.id as u8)) == 0 {
                return Err(Error::ProtocolError);
            }
            seen.note(prop.id)?;
            if prop.id == PropertyId::SubscriptionIdentifier {
                if saw_sub_id && !sub_id_repeats {
                    return Err(Error::ProtocolError);
                }
                saw_sub_id = true;
            }
            // Size from the decoded value, not the wire length, so the cache
            // is right by construction even for a non-canonical encoding.
            payload_size += prop.encoded_size();
            props.push(prop);
        }
        Self::check_auth_pair(seen)?;
        Ok(Self {
            props,
            payload_size,
            seen,
        })
    }
}

impl From<Vec<Property>> for Properties {
    fn from(props: Vec<Property>) -> Self {
        let mut out = Self {
            props: Vec::with_capacity(props.len()),
            ..Self::default()
        };
        out.extend(props);
        out
    }
}

impl From<Properties> for Vec<Property> {
    fn from(props: Properties) -> Self {
        props.props
    }
}

impl FromIterator<Property> for Properties {
    fn from_iter<I: IntoIterator<Item = Property>>(iter: I) -> Self {
        Self::from(iter.into_iter().collect::<Vec<_>>())
    }
}

impl Extend<Property> for Properties {
    fn extend<I: IntoIterator<Item = Property>>(&mut self, iter: I) {
        for prop in iter {
            self.push(prop);
        }
    }
}

impl IntoIterator for Properties {
    type Item = Property;
    type IntoIter = alloc::vec::IntoIter<Property>;

    fn into_iter(self) -> Self::IntoIter {
        self.props.into_iter()
    }
}

impl<'a> IntoIterator for &'a Properties {
    type Item = &'a Property;
    type IntoIter = core::slice::Iter<'a, Property>;

    fn into_iter(self) -> Self::IntoIter {
        self.props.iter()
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::MQTT_VERSION_5;
    use alloc::{string::ToString, vec};
    use bytes::BytesMut;

    fn v5() -> MqttVersion {
        MQTT_VERSION_5
    }

    fn round_trip(props: &Properties) {
        let mut dst = BytesMut::new();
        props.encode(&mut dst, v5()).unwrap();
        assert_eq!(dst.len(), props.encoded_size(v5()));
        let decoded = Properties::decode(&mut dst.freeze(), v5()).unwrap();
        assert_eq!(&decoded, props);
    }

    fn prop(id: PropertyId, value: PropertyValue) -> Property {
        Property::new(id, value)
    }

    // ---- empty ----

    #[test]
    fn test_empty_properties() {
        let props = Properties::new();
        assert!(props.is_empty());
        assert_eq!(props.encoded_size(v5()), 1); // just the 0x00 VBI
    }

    #[test]
    fn test_encode_empty() {
        let mut dst = BytesMut::new();
        Properties::new().encode(&mut dst, v5()).unwrap();
        assert_eq!(&dst[..], &[0x00]);
    }

    #[test]
    fn test_decode_empty() {
        let src = Bytes::from_static(&[0x00]);
        let props = Properties::decode(&mut src.clone(), v5()).unwrap();
        assert!(props.is_empty());
    }

    #[test]
    fn test_round_trip_empty() {
        round_trip(&Properties::new());
    }

    // ---- Byte properties ----

    #[test]
    fn test_payload_format_indicator_round_trip() {
        let mut props = Properties::new();
        props.push(prop(
            PropertyId::PayloadFormatIndicator,
            PropertyValue::Byte(1),
        ));
        round_trip(&props);
    }

    #[test]
    fn test_byte_encoded_size() {
        let p = prop(PropertyId::RetainAvailable, PropertyValue::Byte(1));
        assert_eq!(p.encoded_size(), 2); // 1 id + 1 byte
    }

    // ---- U16 properties ----

    #[test]
    fn test_receive_maximum_round_trip() {
        let mut props = Properties::new();
        props.push(prop(PropertyId::ReceiveMaximum, PropertyValue::U16(1000)));
        round_trip(&props);
    }

    #[test]
    fn test_u16_encoded_size() {
        let p = prop(PropertyId::TopicAlias, PropertyValue::U16(42));
        assert_eq!(p.encoded_size(), 3); // 1 id + 2 bytes
    }

    #[test]
    fn test_receive_maximum_wire_bytes() {
        let mut props = Properties::new();
        props.push(prop(PropertyId::ReceiveMaximum, PropertyValue::U16(0x0064)));
        let mut dst = BytesMut::new();
        props.encode(&mut dst, v5()).unwrap();
        // VBI(3), id=0x21, value=0x00 0x64
        assert_eq!(&dst[..], &[0x03, 0x21, 0x00, 0x64]);
    }

    // ---- U32 properties ----

    #[test]
    fn test_session_expiry_interval_round_trip() {
        let mut props = Properties::new();
        props.push(prop(
            PropertyId::SessionExpiryInterval,
            PropertyValue::U32(3600),
        ));
        round_trip(&props);
    }

    #[test]
    fn test_u32_encoded_size() {
        let p = prop(
            PropertyId::MessageExpiryInterval,
            PropertyValue::U32(0xDEAD_BEEF),
        );
        assert_eq!(p.encoded_size(), 5); // 1 id + 4 bytes
    }

    // ---- VarInt property (SubscriptionIdentifier) ----

    #[test]
    fn test_subscription_identifier_single_byte() {
        let mut props = Properties::new();
        props.push(prop(
            PropertyId::SubscriptionIdentifier,
            PropertyValue::VarInt(5),
        ));
        round_trip(&props);
    }

    #[test]
    fn test_subscription_identifier_multibyte() {
        let mut props = Properties::new();
        props.push(prop(
            PropertyId::SubscriptionIdentifier,
            PropertyValue::VarInt(300),
        ));
        round_trip(&props);
    }

    // ---- Binary properties ----

    #[test]
    fn test_correlation_data_round_trip() {
        let mut props = Properties::new();
        props.push(prop(
            PropertyId::CorrelationData,
            PropertyValue::Binary(Bytes::from_static(b"\x01\x02\x03")),
        ));
        round_trip(&props);
    }

    #[test]
    fn test_binary_encoded_size() {
        let data = Bytes::from_static(b"hello");
        let p = prop(PropertyId::CorrelationData, PropertyValue::Binary(data));
        assert_eq!(p.encoded_size(), 1 + 2 + 5); // id + len_prefix + data
    }

    // ---- String properties ----

    #[test]
    fn test_content_type_round_trip() {
        let mut props = Properties::new();
        props.push(prop(
            PropertyId::ContentType,
            PropertyValue::String("application/json".to_string()),
        ));
        round_trip(&props);
    }

    #[test]
    fn test_reason_string_round_trip() {
        let mut props = Properties::new();
        props.push(prop(
            PropertyId::ReasonString,
            PropertyValue::String("Normal".to_string()),
        ));
        round_trip(&props);
    }

    // ---- StringPair (UserProperty) ----

    #[test]
    fn test_user_property_round_trip() {
        let mut props = Properties::new();
        props.push(prop(
            PropertyId::UserProperty,
            PropertyValue::StringPair("key".to_string(), "value".to_string()),
        ));
        round_trip(&props);
    }

    #[test]
    fn test_multiple_user_properties() {
        let mut props = Properties::new();
        props.push(prop(
            PropertyId::UserProperty,
            PropertyValue::StringPair("a".to_string(), "1".to_string()),
        ));
        props.push(prop(
            PropertyId::UserProperty,
            PropertyValue::StringPair("b".to_string(), "2".to_string()),
        ));
        round_trip(&props);
        // Both preserved in order.
        let mut count = 0;
        for p in props.iter() {
            assert_eq!(p.id, PropertyId::UserProperty);
            count += 1;
        }
        assert_eq!(count, 2);
    }

    // ---- Multiple mixed properties ----

    #[test]
    fn test_mixed_properties_round_trip() {
        let mut props = Properties::new();
        props.push(prop(
            PropertyId::SessionExpiryInterval,
            PropertyValue::U32(120),
        ));
        props.push(prop(PropertyId::ReceiveMaximum, PropertyValue::U16(100)));
        props.push(prop(
            PropertyId::UserProperty,
            PropertyValue::StringPair("env".to_string(), "prod".to_string()),
        ));
        round_trip(&props);
    }

    // ---- get() accessor ----

    #[test]
    fn test_get_existing_property() {
        let mut props = Properties::new();
        props.push(prop(
            PropertyId::SessionExpiryInterval,
            PropertyValue::U32(60),
        ));
        assert_eq!(
            props.get(PropertyId::SessionExpiryInterval),
            Some(&PropertyValue::U32(60))
        );
    }

    #[test]
    fn test_get_missing_property_returns_none() {
        let props = Properties::new();
        assert!(props.get(PropertyId::ReasonString).is_none());
    }

    #[test]
    fn test_get_returns_first_occurrence() {
        let mut props = Properties::new();
        props.push(prop(
            PropertyId::UserProperty,
            PropertyValue::StringPair("k".to_string(), "first".to_string()),
        ));
        props.push(prop(
            PropertyId::UserProperty,
            PropertyValue::StringPair("k".to_string(), "second".to_string()),
        ));
        assert_eq!(
            props.get(PropertyId::UserProperty),
            Some(&PropertyValue::StringPair(
                "k".to_string(),
                "first".to_string()
            ))
        );
    }

    // ---- encoded_size matches encode ----

    #[test]
    fn test_encoded_size_matches_encode_empty() {
        let props = Properties::new();
        let mut dst = BytesMut::new();
        props.encode(&mut dst, v5()).unwrap();
        assert_eq!(dst.len(), props.encoded_size(v5()));
    }

    #[test]
    fn test_encoded_size_matches_encode_multiple() {
        let mut props = Properties::new();
        props.push(prop(
            PropertyId::MaximumPacketSize,
            PropertyValue::U32(65536),
        ));
        props.push(prop(PropertyId::TopicAliasMaximum, PropertyValue::U16(10)));
        let mut dst = BytesMut::new();
        props.encode(&mut dst, v5()).unwrap();
        assert_eq!(dst.len(), props.encoded_size(v5()));
    }

    // ---- value validation (B8 f) ----

    fn encode_one(p: Property) -> Result<()> {
        let mut props = Properties::new();
        props.push(p);
        let mut dst = BytesMut::new();
        props.encode(&mut dst, v5())
    }

    fn decode_raw(body: &[u8]) -> Result<Properties> {
        let mut src = BytesMut::new();
        src.put_u8(body.len() as u8);
        src.put_slice(body);
        Properties::decode(&mut src.freeze(), v5())
    }

    #[test]
    fn test_validate_flag_properties_must_be_0_or_1() {
        for id in [
            PropertyId::PayloadFormatIndicator,
            PropertyId::RequestProblemInformation,
            PropertyId::RequestResponseInformation,
            PropertyId::MaximumQoS,
            PropertyId::RetainAvailable,
            PropertyId::WildcardSubscriptionAvailable,
            PropertyId::SubscriptionIdentifierAvailable,
            PropertyId::SharedSubscriptionAvailable,
        ] {
            assert!(Property::new(id, PropertyValue::Byte(0)).validate().is_ok());
            assert!(Property::new(id, PropertyValue::Byte(1)).validate().is_ok());
            assert_eq!(
                Property::new(id, PropertyValue::Byte(2)).validate(),
                Err(Error::ProtocolError),
                "{id:?}"
            );
            // ... and on the wire
            assert_eq!(decode_raw(&[id as u8, 0x07]), Err(Error::ProtocolError));
            assert_eq!(
                encode_one(Property::new(id, PropertyValue::Byte(7))),
                Err(Error::ProtocolError)
            );
        }
    }

    #[test]
    fn test_validate_non_zero_properties() {
        assert_eq!(
            Property::new(PropertyId::ReceiveMaximum, PropertyValue::U16(0)).validate(),
            Err(Error::ProtocolError)
        );
        assert_eq!(
            Property::new(PropertyId::TopicAlias, PropertyValue::U16(0)).validate(),
            Err(Error::ProtocolError)
        );
        assert_eq!(
            Property::new(PropertyId::MaximumPacketSize, PropertyValue::U32(0)).validate(),
            Err(Error::ProtocolError)
        );
        assert_eq!(
            Property::new(PropertyId::SubscriptionIdentifier, PropertyValue::VarInt(0)).validate(),
            Err(Error::ProtocolError)
        );
        // Wire forms: ReceiveMaximum = 0, MaximumPacketSize = 0, TopicAlias = 0, SubId = 0
        assert_eq!(decode_raw(&[0x21, 0x00, 0x00]), Err(Error::ProtocolError));
        assert_eq!(
            decode_raw(&[0x27, 0x00, 0x00, 0x00, 0x00]),
            Err(Error::ProtocolError)
        );
        assert_eq!(decode_raw(&[0x23, 0x00, 0x00]), Err(Error::ProtocolError));
        assert_eq!(decode_raw(&[0x0B, 0x00]), Err(Error::ProtocolError));
        // Minimum legal values are fine.
        assert!(decode_raw(&[0x21, 0x00, 0x01]).is_ok());
        assert!(decode_raw(&[0x0B, 0x01]).is_ok());
        // TopicAliasMaximum = 0 is explicitly allowed (means "no aliases").
        assert!(decode_raw(&[0x22, 0x00, 0x00]).is_ok());
    }

    #[test]
    fn test_validate_response_topic_rejects_wildcards() {
        for t in ["a/#", "a/+/b", "+", "#"] {
            assert_eq!(
                Property::new(PropertyId::ResponseTopic, PropertyValue::String(t.into()))
                    .validate(),
                Err(Error::ProtocolError),
                "{t}"
            );
        }
        assert!(
            Property::new(
                PropertyId::ResponseTopic,
                PropertyValue::String("a/b".into())
            )
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn test_validate_value_type_must_match_id() {
        // A U32 where the id calls for a Byte can only be built via the API,
        // never decoded; encode must refuse it.
        assert_eq!(
            Property::new(PropertyId::MaximumQoS, PropertyValue::U32(1)).validate(),
            Err(Error::ProtocolError)
        );
        assert_eq!(
            encode_one(Property::new(
                PropertyId::ContentType,
                PropertyValue::Binary(Bytes::from_static(b"x"))
            )),
            Err(Error::ProtocolError)
        );
    }

    // ---- property-to-packet context (G2) ----

    #[test]
    fn test_is_allowed_in_matches_spec_table() {
        use PacketType as T;
        use PropertyId::*;
        // A few cells from Table 2-4, positive and negative.
        assert!(SessionExpiryInterval.is_allowed_in(T::Connect));
        assert!(SessionExpiryInterval.is_allowed_in(T::ConnAck));
        assert!(SessionExpiryInterval.is_allowed_in(T::Disconnect));
        assert!(!SessionExpiryInterval.is_allowed_in(T::Publish));
        assert!(MaximumQoS.is_allowed_in(T::ConnAck));
        assert!(!MaximumQoS.is_allowed_in(T::Connect));
        assert!(TopicAlias.is_allowed_in(T::Publish));
        assert!(!TopicAlias.is_allowed_in(PropertyContext::Will));
        assert!(WillDelayInterval.is_allowed_in(PropertyContext::Will));
        assert!(!WillDelayInterval.is_allowed_in(T::Connect));
        assert!(SubscriptionIdentifier.is_allowed_in(T::Subscribe));
        assert!(SubscriptionIdentifier.is_allowed_in(T::Publish));
        assert!(!SubscriptionIdentifier.is_allowed_in(T::SubAck));
        assert!(ReasonString.is_allowed_in(T::PubAck));
        assert!(ReasonString.is_allowed_in(T::PubComp));
        assert!(!ReasonString.is_allowed_in(T::Subscribe));
        assert!(!ReasonString.is_allowed_in(T::Unsubscribe));
        assert!(AuthenticationMethod.is_allowed_in(T::Auth));
        assert!(!AuthenticationMethod.is_allowed_in(T::Publish));
        // UserProperty is allowed everywhere properties exist, and nowhere
        // they don't.
        for ty in [
            T::Connect,
            T::ConnAck,
            T::Publish,
            T::PubAck,
            T::PubRec,
            T::PubRel,
            T::PubComp,
            T::Subscribe,
            T::SubAck,
            T::Unsubscribe,
            T::UnsubAck,
            T::Disconnect,
            T::Auth,
        ] {
            assert!(UserProperty.is_allowed_in(ty), "{ty:?}");
        }
        assert!(UserProperty.is_allowed_in(PropertyContext::Will));
        assert!(!UserProperty.is_allowed_in(T::PingReq));
        assert!(!UserProperty.is_allowed_in(T::PingResp));
    }

    #[test]
    fn test_decode_in_rejects_property_from_wrong_packet() {
        // MaximumQoS = 1 is a valid property, but not in a CONNECT.
        let mut src = BytesMut::new();
        src.put_u8(2);
        src.put_slice(&[0x24, 0x01]);
        let bytes = src.freeze();
        assert_eq!(
            Properties::decode_in(&mut bytes.clone(), v5(), PacketType::Connect),
            Err(Error::ProtocolError)
        );
        assert!(Properties::decode_in(&mut bytes.clone(), v5(), PacketType::ConnAck).is_ok());
        // The context-free decoder still accepts it.
        assert!(Properties::decode(&mut bytes.clone(), v5()).is_ok());
    }

    #[test]
    fn test_check_context() {
        let mut props = Properties::new();
        props.push(prop(PropertyId::TopicAlias, PropertyValue::U16(3)));
        assert!(props.check_context(PacketType::Publish).is_ok());
        assert_eq!(
            props.check_context(PropertyContext::Will),
            Err(Error::ProtocolError)
        );
        assert_eq!(
            props.check_context(PacketType::Connect),
            Err(Error::ProtocolError)
        );
    }

    // ---- duplicate detection (B8 g) ----

    #[test]
    fn test_decode_rejects_duplicate_non_repeatable_property() {
        // SessionExpiryInterval twice
        assert_eq!(
            decode_raw(&[0x11, 0, 0, 0, 1, 0x11, 0, 0, 0, 2]),
            Err(Error::ProtocolError)
        );
        // ContentType twice
        assert_eq!(
            decode_raw(&[0x03, 0, 1, b'a', 0x03, 0, 1, b'b']),
            Err(Error::ProtocolError)
        );
    }

    #[test]
    fn test_decode_allows_repeatable_properties() {
        // Two user properties and two subscription identifiers.
        let props = decode_raw(&[
            0x26, 0, 1, b'k', 0, 1, b'v', 0x26, 0, 1, b'k', 0, 1, b'w', 0x0B, 0x01, 0x0B, 0x02,
        ])
        .unwrap();
        assert_eq!(props.len(), 4);
    }

    #[test]
    fn test_encode_rejects_duplicate_non_repeatable_property() {
        let mut props = Properties::new();
        props.push(prop(PropertyId::ReceiveMaximum, PropertyValue::U16(1)));
        props.push(prop(PropertyId::ReceiveMaximum, PropertyValue::U16(2)));
        let mut dst = BytesMut::new();
        assert_eq!(props.encode(&mut dst, v5()), Err(Error::ProtocolError));
        // Rejected on v3 too — non-empty properties cannot be carried there.
        assert_eq!(
            props.encode(&mut dst, MqttVersion::V3_1_1),
            Err(Error::ProtocolError)
        );
    }

    #[test]
    fn test_encode_allows_repeatable_properties() {
        let mut props = Properties::new();
        props.push(prop(
            PropertyId::UserProperty,
            PropertyValue::StringPair("a".to_string(), "1".to_string()),
        ));
        props.push(prop(
            PropertyId::UserProperty,
            PropertyValue::StringPair("a".to_string(), "2".to_string()),
        ));
        props.push(prop(
            PropertyId::SubscriptionIdentifier,
            PropertyValue::VarInt(1),
        ));
        props.push(prop(
            PropertyId::SubscriptionIdentifier,
            PropertyValue::VarInt(2),
        ));
        let mut dst = BytesMut::new();
        props.encode(&mut dst, v5()).unwrap();
        let back = Properties::decode(&mut dst.freeze(), v5()).unwrap();
        assert_eq!(back, props);
    }

    #[test]
    fn test_is_repeatable() {
        assert!(PropertyId::UserProperty.is_repeatable());
        assert!(PropertyId::SubscriptionIdentifier.is_repeatable());
        assert!(!PropertyId::SessionExpiryInterval.is_repeatable());
        assert!(!PropertyId::ContentType.is_repeatable());
    }

    // ---- cached payload size ----

    fn sample_props() -> alloc::vec::Vec<Property> {
        alloc::vec![
            prop(PropertyId::MaximumPacketSize, PropertyValue::U32(65536)),
            prop(PropertyId::TopicAliasMaximum, PropertyValue::U16(10)),
            prop(
                PropertyId::UserProperty,
                PropertyValue::StringPair("key".to_string(), "value".to_string()),
            ),
            prop(
                PropertyId::CorrelationData,
                PropertyValue::Binary(Bytes::from_static(b"abc")),
            ),
        ]
    }

    fn summed(props: &Properties) -> usize {
        props.iter().map(Property::encoded_size).sum()
    }

    #[test]
    fn test_cached_size_tracks_push() {
        let mut props = Properties::new();
        assert_eq!(props.payload_size(), 0);
        for p in sample_props() {
            props.push(p);
            assert_eq!(props.payload_size(), summed(&props));
        }
        assert_eq!(props.len(), 4);
    }

    #[test]
    fn test_cached_size_from_vec_and_collect() {
        let a = Properties::from(sample_props());
        assert_eq!(a.payload_size(), summed(&a));
        let b: Properties = sample_props().into_iter().collect();
        assert_eq!(b.payload_size(), summed(&b));
        assert_eq!(a, b);
    }

    #[test]
    fn test_cached_size_tracks_extend() {
        let mut props = Properties::new();
        props.extend(sample_props());
        assert_eq!(props.payload_size(), summed(&props));
    }

    #[test]
    fn test_cached_size_after_decode() {
        let src = Properties::from(sample_props());
        let mut dst = BytesMut::new();
        src.encode(&mut dst, v5()).unwrap();
        let decoded = Properties::decode(&mut dst.freeze(), v5()).unwrap();
        assert_eq!(decoded, src);
        assert_eq!(decoded.payload_size(), summed(&decoded));
    }

    #[test]
    fn test_decode_rejects_non_minimal_varint_property() {
        // SubscriptionIdentifier = 44 encoded as two bytes; regression for a
        // fuzzer finding where the cached size came from the wire length.
        let mut src = BytesMut::new();
        src.put_u8(3); // property length
        src.put_u8(0x0B);
        src.put_slice(&[0xAC, 0x00]);
        assert_eq!(
            Properties::decode(&mut src.freeze(), v5()),
            Err(Error::MalformedVarLen)
        );
    }

    #[test]
    fn test_into_vec_round_trip() {
        let props = Properties::from(sample_props());
        let v: alloc::vec::Vec<Property> = props.clone().into();
        assert_eq!(v, sample_props());
        assert_eq!(
            props.into_iter().collect::<alloc::vec::Vec<_>>(),
            sample_props()
        );
    }

    // ---- error cases ----

    #[test]
    fn test_decode_too_short_returns_err() {
        // length prefix says 5 bytes but only 2 follow
        let src = Bytes::from_static(&[0x05, 0x01, 0x02]);
        assert_eq!(
            Properties::decode(&mut src.clone(), v5()),
            Err(Error::PacketTooShort)
        );
    }

    #[test]
    fn test_decode_unknown_property_id_returns_err() {
        // length=1, id=0xFF (unknown)
        let src = Bytes::from_static(&[0x01, 0xFF]);
        assert_eq!(
            Properties::decode(&mut src.clone(), v5()),
            Err(Error::MalformedPacket)
        );
    }

    #[test]
    fn test_property_id_try_from_unknown_is_err() {
        assert_eq!(PropertyId::try_from(0x00), Err(Error::MalformedPacket));
        assert_eq!(PropertyId::try_from(0xFF), Err(Error::MalformedPacket));
    }

    #[test]
    fn test_property_id_try_from_known() {
        assert_eq!(
            PropertyId::try_from(0x01),
            Ok(PropertyId::PayloadFormatIndicator)
        );
        assert_eq!(PropertyId::try_from(0x26), Ok(PropertyId::UserProperty));
    }

    // ---- field length limits ----

    #[test]
    fn test_encode_binary_too_long_errors() {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::CorrelationData,
            PropertyValue::Binary(Bytes::from(alloc::vec![0u8; 65_536])),
        ));
        let mut dst = BytesMut::new();
        assert_eq!(props.encode(&mut dst, v5()), Err(Error::BinaryTooLong));
    }

    #[test]
    fn test_encode_string_too_long_errors() {
        let s: String = core::iter::repeat_n('x', 65_536).collect();
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::ContentType,
            PropertyValue::String(s),
        ));
        let mut dst = BytesMut::new();
        assert_eq!(props.encode(&mut dst, v5()), Err(Error::StringTooLong));
    }

    #[test]
    fn test_encode_string_pair_value_too_long_errors() {
        let v: String = core::iter::repeat_n('x', 65_536).collect();
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::UserProperty,
            PropertyValue::StringPair("k".to_string(), v),
        ));
        let mut dst = BytesMut::new();
        assert_eq!(props.encode(&mut dst, v5()), Err(Error::StringTooLong));
    }

    // ---- SubscriptionIdentifier repeats only in PUBLISH (§3.3.2.3.8, §3.8.2.1.2) ----

    fn two_sub_ids() -> Properties {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::SubscriptionIdentifier,
            PropertyValue::VarInt(1),
        ));
        props.push(Property::new(
            PropertyId::SubscriptionIdentifier,
            PropertyValue::VarInt(2),
        ));
        props
    }

    #[test]
    fn test_is_repeatable_in() {
        use PropertyId::*;
        assert!(SubscriptionIdentifier.is_repeatable_in(PacketType::Publish));
        assert!(!SubscriptionIdentifier.is_repeatable_in(PacketType::Subscribe));
        assert!(UserProperty.is_repeatable_in(PacketType::Subscribe));
        assert!(UserProperty.is_repeatable_in(PropertyContext::Will));
        assert!(!ReasonString.is_repeatable_in(PacketType::PubAck));
    }

    #[test]
    fn test_check_context_subscription_identifier_repeat() {
        let props = two_sub_ids();
        assert!(props.check_context(PacketType::Publish).is_ok());
        assert_eq!(
            props.check_context(PacketType::Subscribe),
            Err(Error::ProtocolError)
        );
        // The list itself is still valid: `encode` without a context accepts it.
        let mut dst = BytesMut::new();
        assert!(props.encode(&mut dst, v5()).is_ok());
    }

    #[test]
    fn test_decode_in_subscription_identifier_repeat() {
        let mut wire = BytesMut::new();
        two_sub_ids().encode(&mut wire, v5()).unwrap();
        let wire = wire.freeze();

        let decoded = Properties::decode_in(&mut wire.clone(), v5(), PacketType::Publish)
            .expect("PUBLISH may carry several");
        assert_eq!(decoded.len(), 2);

        assert_eq!(
            Properties::decode_in(&mut wire.clone(), v5(), PacketType::Subscribe),
            Err(Error::ProtocolError)
        );
        // The context-free decoder stays permissive.
        assert!(Properties::decode(&mut wire.clone(), v5()).is_ok());
    }

    // ---- Non-empty properties cannot be encoded for v3 ----

    #[test]
    fn test_encode_non_empty_on_v3_is_protocol_error() {
        let mut props = Properties::new();
        props.push(prop(
            PropertyId::UserProperty,
            PropertyValue::StringPair("k".into(), "v".into()),
        ));
        for ver in [MqttVersion::V3_1, MqttVersion::V3_1_1] {
            let mut dst = BytesMut::new();
            assert_eq!(
                props.encode(&mut dst, ver),
                Err(Error::ProtocolError),
                "{ver}"
            );
            assert!(dst.is_empty());
            assert_eq!(props.encoded_size(ver), 0);
        }
        // Empty is still fine and writes nothing.
        let mut dst = BytesMut::new();
        assert!(
            Properties::new()
                .encode(&mut dst, MqttVersion::V3_1_1)
                .is_ok()
        );
        assert!(dst.is_empty());
    }

    // ---- Authentication Data requires an Authentication Method ----

    fn auth_method(name: &str) -> Property {
        Property::new(
            PropertyId::AuthenticationMethod,
            PropertyValue::String(name.into()),
        )
    }

    fn auth_data(bytes: &'static [u8]) -> Property {
        Property::new(
            PropertyId::AuthenticationData,
            PropertyValue::Binary(Bytes::from_static(bytes)),
        )
    }

    /// Wire bytes for a property list, built with `encode` — which is
    /// context-free and so will emit the invalid pair we want to decode.
    fn to_wire(props: &Properties) -> Bytes {
        let mut dst = BytesMut::new();
        props.encode(&mut dst, v5()).unwrap();
        dst.freeze()
    }

    #[test]
    fn test_auth_data_alone_fails_check_context() {
        let mut props = Properties::new();
        props.push(auth_data(b"tok"));
        for ctx in [PacketType::Connect, PacketType::ConnAck, PacketType::Auth] {
            assert_eq!(
                props.check_context(ctx),
                Err(Error::ProtocolError),
                "{ctx:?}"
            );
        }
    }

    #[test]
    fn test_auth_data_with_method_passes_check_context() {
        let mut props = Properties::new();
        props.push(auth_method("SCRAM-SHA-1"));
        props.push(auth_data(b"tok"));
        assert!(props.check_context(PacketType::Auth).is_ok());
    }

    /// The rule is about the pair, not the order they were added in.
    #[test]
    fn test_auth_data_before_method_is_accepted() {
        let mut props = Properties::new();
        props.push(auth_data(b"tok"));
        props.push(auth_method("SCRAM-SHA-1"));
        assert!(props.check_context(PacketType::Auth).is_ok());
    }

    /// A method with no data is fine: it names the method and asks the peer
    /// to start.
    #[test]
    fn test_auth_method_alone_is_accepted() {
        let mut props = Properties::new();
        props.push(auth_method("SCRAM-SHA-1"));
        assert!(props.check_context(PacketType::Auth).is_ok());
    }

    #[test]
    fn test_auth_data_alone_is_rejected_on_decode() {
        let mut props = Properties::new();
        props.push(auth_data(b"tok"));
        let wire = to_wire(&props);

        // Both the context-aware and the context-free decoders: the rule does
        // not depend on which packet carries the properties.
        assert_eq!(
            Properties::decode_in(&mut wire.clone(), v5(), PacketType::Auth),
            Err(Error::ProtocolError)
        );
        assert_eq!(
            Properties::decode(&mut wire.clone(), v5()),
            Err(Error::ProtocolError)
        );
    }

    #[test]
    fn test_auth_pair_round_trips_on_decode() {
        let mut props = Properties::new();
        props.push(auth_method("SCRAM-SHA-1"));
        props.push(auth_data(b"tok"));
        let wire = to_wire(&props);
        assert_eq!(
            Properties::decode_in(&mut wire.clone(), v5(), PacketType::Auth).unwrap(),
            props
        );
    }

    // ---- typed getters ----

    /// One property of every type the `push_*` helpers can produce.
    fn one_of_each() -> Properties {
        let mut props = Properties::new();
        props.push_byte(PropertyId::PayloadFormatIndicator, 1);
        props.push_u16(PropertyId::ReceiveMaximum, 20);
        props.push_u32(PropertyId::SessionExpiryInterval, 300);
        props.push_var_int(PropertyId::SubscriptionIdentifier, 9u32);
        props.push_binary(PropertyId::CorrelationData, Bytes::from_static(b"\x01\x02"));
        props.push_string(PropertyId::ContentType, "text/plain");
        props.push_string_pair(PropertyId::UserProperty, "k", "v");
        props
    }

    #[test]
    fn test_typed_getters_read_each_type() {
        let props = one_of_each();
        assert_eq!(props.get_byte(PropertyId::PayloadFormatIndicator), Some(1));
        assert_eq!(props.get_u16(PropertyId::ReceiveMaximum), Some(20));
        assert_eq!(props.get_u32(PropertyId::SessionExpiryInterval), Some(300));
        assert_eq!(
            props.get_var_int(PropertyId::SubscriptionIdentifier),
            Some(9)
        );
        assert_eq!(
            props.get_binary(PropertyId::CorrelationData),
            Some(Bytes::from_static(b"\x01\x02"))
        );
        assert_eq!(
            props.get_string(PropertyId::ContentType).as_deref(),
            Some("text/plain")
        );
        assert_eq!(
            props.get_string_pair(PropertyId::UserProperty),
            Some(("k".to_string(), "v".to_string()))
        );
    }

    /// An absent identifier is `None` for every accessor.
    #[test]
    fn test_typed_getters_on_a_missing_property() {
        let props = Properties::new();
        assert_eq!(props.get_byte(PropertyId::PayloadFormatIndicator), None);
        assert_eq!(props.get_u16(PropertyId::ReceiveMaximum), None);
        assert_eq!(props.get_u32(PropertyId::SessionExpiryInterval), None);
        assert_eq!(props.get_var_int(PropertyId::SubscriptionIdentifier), None);
        assert_eq!(props.get_binary(PropertyId::CorrelationData), None);
        assert_eq!(props.get_string(PropertyId::ContentType), None);
        assert_eq!(props.get_string_pair(PropertyId::UserProperty), None);
    }

    /// Asking for the wrong type is `None`, not a panic or a coercion.
    #[test]
    fn test_typed_getters_reject_a_mismatched_type() {
        let props = one_of_each();
        assert_eq!(props.get_u32(PropertyId::ReceiveMaximum), None); // it is a U16
        assert_eq!(props.get_u16(PropertyId::SessionExpiryInterval), None); // a U32
        assert_eq!(props.get_var_int(PropertyId::SessionExpiryInterval), None); // a U32
        assert_eq!(props.get_byte(PropertyId::SubscriptionIdentifier), None); // a VarInt
        assert_eq!(props.get_byte(PropertyId::ContentType), None); // a String
        assert_eq!(props.get_string(PropertyId::UserProperty), None); // a pair
        assert_eq!(props.get_string_pair(PropertyId::ContentType), None); // a String
        assert_eq!(props.get_binary(PropertyId::ContentType), None); // a String
    }

    /// `UserProperty` is the one that repeats in practice, which is what the
    /// `_at` accessors are for.
    #[test]
    fn test_string_pair_at_walks_repeated_user_properties() {
        let mut props = Properties::new();
        props.push_string_pair(PropertyId::UserProperty, "a", "1");
        props.push_string_pair(PropertyId::UserProperty, "b", "2");
        props.push_string_pair(PropertyId::UserProperty, "c", "3");

        for (i, (k, v)) in [("a", "1"), ("b", "2"), ("c", "3")].iter().enumerate() {
            assert_eq!(
                props.get_string_pair_at(PropertyId::UserProperty, i),
                Some((k.to_string(), v.to_string())),
                "index {i}"
            );
        }
        assert_eq!(props.get_string_pair_at(PropertyId::UserProperty, 3), None);
        // The un-indexed accessor is the first one.
        assert_eq!(
            props.get_string_pair(PropertyId::UserProperty),
            props.get_string_pair_at(PropertyId::UserProperty, 0)
        );
    }

    /// `get_at` counts only properties with the requested identifier, not the
    /// position in the whole list.
    #[test]
    fn test_get_at_indexes_within_the_identifier() {
        let mut props = Properties::new();
        props.push_string(PropertyId::ContentType, "text/plain");
        props.push_string_pair(PropertyId::UserProperty, "a", "1");
        props.push_u16(PropertyId::ReceiveMaximum, 7);
        props.push_string_pair(PropertyId::UserProperty, "b", "2");

        assert_eq!(
            props.get_at(PropertyId::UserProperty, 1),
            Some(&PropertyValue::StringPair("b".into(), "2".into()))
        );
        assert_eq!(
            props.get_at(PropertyId::ReceiveMaximum, 0),
            props.get(PropertyId::ReceiveMaximum)
        );
        assert_eq!(props.get_at(PropertyId::ReceiveMaximum, 1), None);
    }

    /// `get_u32` takes either 32-bit wire form; `get_var_int` is exact.
    #[test]
    fn test_get_u32_reads_both_integer_forms() {
        let mut props = Properties::new();
        props.push_var_int(PropertyId::SubscriptionIdentifier, 5u32);
        props.push_u32(PropertyId::SessionExpiryInterval, 300);

        assert_eq!(props.get_u32(PropertyId::SubscriptionIdentifier), Some(5));
        assert_eq!(props.get_u32(PropertyId::SessionExpiryInterval), Some(300));

        assert_eq!(
            props.get_var_int(PropertyId::SubscriptionIdentifier),
            Some(5)
        );
        assert_eq!(props.get_var_int(PropertyId::SessionExpiryInterval), None);
    }

    /// `push_var_int` agrees with the constructor it wraps, and takes anything
    /// that converts into a `u32`.
    #[test]
    fn test_push_var_int_matches_the_constructor() {
        let mut a = Properties::new();
        a.push_var_int(PropertyId::SubscriptionIdentifier, 7u32);
        let mut b = Properties::new();
        b.push(Property::new_var_int(
            PropertyId::SubscriptionIdentifier,
            7u32,
        ));
        assert_eq!(a, b);

        // A narrower integer converts.
        let mut c = Properties::new();
        c.push_var_int(PropertyId::SubscriptionIdentifier, 7u16);
        assert_eq!(c, a);
    }

    /// A PUBLISH may carry one `SubscriptionIdentifier` per matching
    /// subscription (§3.3.2.3.8), which is what `_at` is for.
    #[test]
    fn test_var_int_at_walks_repeated_subscription_identifiers() {
        let mut props = Properties::new();
        for id in [1u32, 2, 3] {
            props.push_var_int(PropertyId::SubscriptionIdentifier, id);
        }
        for (i, want) in [1u32, 2, 3].iter().enumerate() {
            assert_eq!(
                props.get_var_int_at(PropertyId::SubscriptionIdentifier, i),
                Some(*want),
                "index {i}"
            );
            // The lenient accessor walks them too.
            assert_eq!(
                props.get_u32_at(PropertyId::SubscriptionIdentifier, i),
                Some(*want)
            );
        }
        assert_eq!(
            props.get_var_int_at(PropertyId::SubscriptionIdentifier, 3),
            None
        );
        // Repeating it is only legal in a PUBLISH.
        assert!(props.check_context(PacketType::Publish).is_ok());
        assert_eq!(
            props.check_context(PacketType::Subscribe),
            Err(Error::ProtocolError)
        );
    }

    // ---- user properties and removal ----

    fn three_users() -> Properties {
        let mut props = Properties::new();
        props.push_user("a", "1");
        props.push_string(PropertyId::ContentType, "text/plain");
        props.push_user("b", "2");
        props.push_user("a", "3");
        props
    }

    #[test]
    fn test_push_user_is_a_user_property() {
        let mut a = Properties::new();
        a.push_user("k", "v");
        let mut b = Properties::new();
        b.push_string_pair(PropertyId::UserProperty, "k", "v");
        assert_eq!(a, b);
    }

    #[test]
    fn test_user_iter_yields_pairs_in_order_and_skips_others() {
        let props = three_users();
        assert_eq!(
            props.user_iter().collect::<Vec<_>>(),
            vec![("a", "1"), ("b", "2"), ("a", "3")]
        );
        assert_eq!(Properties::new().user_iter().count(), 0);
    }

    #[test]
    fn test_get_user_returns_the_first_match() {
        let props = three_users();
        // "a" appears twice; the first wins.
        assert_eq!(props.get_user("a"), Some("1"));
        assert_eq!(props.get_user("b"), Some("2"));
        assert_eq!(props.get_user("nope"), None);
    }

    /// The obsolete name is the new one.
    #[test]
    #[allow(deprecated)]
    fn test_find_user_property_delegates_to_get_user() {
        let props = three_users();
        for key in ["a", "b", "nope"] {
            assert_eq!(props.find_user_property(key), props.get_user(key), "{key}");
        }
        assert_eq!(props.find_user_property("a"), Some("1"));
    }

    #[test]
    fn test_get_all_walks_one_identifier() {
        let props = three_users();
        assert_eq!(props.get_all(PropertyId::UserProperty).count(), 3);
        assert_eq!(
            props.get_all(PropertyId::ContentType).collect::<Vec<_>>(),
            vec![&PropertyValue::String("text/plain".into())]
        );
        assert_eq!(props.get_all(PropertyId::ReceiveMaximum).count(), 0);
    }

    #[test]
    fn test_remove_user_takes_every_entry_with_that_key() {
        let mut props = three_users();
        assert_eq!(props.remove_user("a"), 2);
        assert_eq!(props.user_iter().collect::<Vec<_>>(), vec![("b", "2")]);
        // The non-user property is untouched.
        assert_eq!(
            props.get_string(PropertyId::ContentType).as_deref(),
            Some("text/plain")
        );
        assert_eq!(props.remove_user("gone"), 0);
    }

    #[test]
    fn test_remove_takes_every_property_with_that_id() {
        let mut props = three_users();
        assert_eq!(props.remove(PropertyId::UserProperty), 3);
        assert_eq!(props.len(), 1);
        assert_eq!(props.remove(PropertyId::ContentType), 1);
        assert!(props.is_empty());
        assert_eq!(props.remove(PropertyId::ContentType), 0);
    }

    #[test]
    fn test_clear_empties_the_collection() {
        let mut props = three_users();
        props.clear();
        assert!(props.is_empty());
        assert_eq!(props.encoded_size(v5()), 1); // just the zero-length VBI
        assert_eq!(props, Properties::new());
    }

    #[test]
    fn test_retain_keeps_only_what_the_predicate_wants() {
        let mut props = three_users();
        // Drop the user properties whose key is "a".
        props.retain(|p| {
            !matches!(
                (&p.id, &p.value),
                (PropertyId::UserProperty, PropertyValue::StringPair(k, _)) if k == "a"
            )
        });
        assert_eq!(props.user_iter().collect::<Vec<_>>(), vec![("b", "2")]);
        assert_eq!(
            props.get_string(PropertyId::ContentType).as_deref(),
            Some("text/plain")
        );
    }

    /// The condition can be on the value, which is what `remove` and
    /// `remove_user` cannot express.
    #[test]
    fn test_retain_can_filter_on_the_value() {
        let mut props = Properties::new();
        props.push_user("keep", "yes");
        props.push_user("drop", "");
        props.push_user("also", "yes");
        props.retain(|p| !matches!(&p.value, PropertyValue::StringPair(_, v) if v.is_empty()));
        assert_eq!(
            props.user_iter().collect::<Vec<_>>(),
            vec![("keep", "yes"), ("also", "yes")]
        );
    }

    #[test]
    fn test_retain_everything_and_nothing() {
        let mut all = three_users();
        let before = all.clone();
        all.retain(|_| true);
        assert_eq!(all, before);

        let mut none = three_users();
        none.retain(|_| false);
        assert!(none.is_empty());
        assert_eq!(none, Properties::new());
    }

    /// `remove` and `remove_user` are `retain` underneath, so they must agree.
    #[test]
    fn test_remove_agrees_with_the_equivalent_retain() {
        let mut a = three_users();
        a.remove(PropertyId::UserProperty);
        let mut b = three_users();
        b.retain(|p| p.id != PropertyId::UserProperty);
        assert_eq!(a, b);
    }

    /// Removal has to keep the cached wire size in step, or `encode` writes a
    /// length prefix that disagrees with the bytes after it.
    #[test]
    fn test_removal_keeps_the_cached_size_correct() {
        for remove in [
            |p: &mut Properties| {
                p.remove_user("a");
            },
            |p: &mut Properties| {
                p.remove(PropertyId::UserProperty);
            },
            |p: &mut Properties| {
                p.remove(PropertyId::ContentType);
            },
            |p: &mut Properties| p.clear(),
            |p: &mut Properties| p.retain(|prop| prop.id == PropertyId::ContentType),
            |p: &mut Properties| p.retain(|_| false),
        ] {
            let mut props = three_users();
            remove(&mut props);

            let mut dst = BytesMut::new();
            props.encode(&mut dst, v5()).unwrap();
            assert_eq!(dst.len(), props.encoded_size(v5()));
            // And the bytes still decode back to the same collection.
            assert_eq!(Properties::decode(&mut dst.freeze(), v5()).unwrap(), props);
        }
    }

    /// Removing a duplicate frees the identifier for re-use, and removing the
    /// property that invalidated the list clears that too.
    #[test]
    fn test_removal_rebuilds_the_seen_set() {
        let mut props = Properties::new();
        props.push_u16(PropertyId::ReceiveMaximum, 10);
        props.push_u16(PropertyId::ReceiveMaximum, 20); // a repeat: invalid
        let mut dst = BytesMut::new();
        assert_eq!(props.encode(&mut dst, v5()), Err(Error::ProtocolError));

        // Dropping both and pushing one again is a valid list.
        assert_eq!(props.remove(PropertyId::ReceiveMaximum), 2);
        props.push_u16(PropertyId::ReceiveMaximum, 30);
        let mut dst = BytesMut::new();
        props.encode(&mut dst, v5()).unwrap();
        assert_eq!(props.get_u16(PropertyId::ReceiveMaximum), Some(30));
    }
}
