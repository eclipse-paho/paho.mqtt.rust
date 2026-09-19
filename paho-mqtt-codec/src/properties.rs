// paho-mqtt-codec/src/properties.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.

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
        Property { id, value }
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
        let prop = Property { id, value };
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

    /// Returns the value of the first property with the given `id`, or `None`.
    pub fn get(&self, id: PropertyId) -> Option<&PropertyValue> {
        self.props.iter().find(|p| p.id == id).map(|p| &p.value)
    }

    /// Iterates over all properties in order.
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
    pub fn encoded_size(&self, ver: MqttVersion) -> usize {
        if ver < MqttVersion::V5 {
            0
        } else {
            let data_len = self.payload_size();
            varlen::encoded_size(data_len as u32) + data_len
        }
    }

    /// Encodes the properties to the wire: VBI(payload_len) followed by each property.
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
        varlen::encode(self.payload_size() as u32, dst)?;
        for prop in &self.props {
            prop.encode(dst)?;
        }
        Ok(())
    }

    /// Checks that every property is allowed in `ctx` (MQTT v5 §2.2.2.2)
    /// and that none repeats where `ctx` forbids it — in practice, that a
    /// `SubscriptionIdentifier` appears at most once outside a PUBLISH
    /// (§3.8.2.1.2). Returns [`Error::ProtocolError`] otherwise.
    ///
    /// Packet encoders call this; use it directly when assembling properties
    /// for a packet to catch a misplaced one early.
    #[inline]
    pub fn check_context(&self, ctx: impl Into<PropertyContext>) -> Result<()> {
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
    use alloc::string::ToString;
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
}
