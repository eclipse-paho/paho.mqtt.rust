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
//! has a fixed wire type determined by its identifier, so [`Property`] is one
//! enum with a variant per identifier, carrying the value that identifier
//! calls for: `Property::TopicAlias(7)`, `Property::ContentType(s)`. A value
//! of the wrong type for its identifier therefore does not compile, where the
//! earlier identifier-plus-value struct could build one and leave it to be
//! caught at encode time.
//!
//! Values are read back through the accessors on [`Properties`]: the typed
//! ones ([`get_u16`](Properties::get_u16), [`get_str`](Properties::get_str))
//! when the identifier is known, [`get`](Properties::get) for the property
//! itself. The type-tagged `PropertyValue` the earlier struct stored is now
//! internal, since nothing outside the crate needs a value separated from the
//! identifier that gives it meaning.
//!
//! The [`Properties`] collection encodes and decodes the length-prefixed
//! property list that appears in the variable header of every v5 packet type
//! that supports them.
//!
//! Properties exist only in v5. On a v3 connection nothing is read from the
//! wire and an empty list writes nothing; encoding a non-empty list for v3
//! is an error rather than a silent drop.

use crate::{
    Error, MqttVersion, PacketType, Result, String, Vec, varlen,
    wire::{get_binary, get_utf8, put_binary, put_utf8},
};
use bytes::{Buf, BufMut, Bytes};
use strum_macros::{EnumDiscriminants, FromRepr};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////

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
    ///
    /// The byte-to-identifier table comes from `from_repr`, which the
    /// `FromRepr` derive writes from the discriminants on [`Property`], so
    /// the identifier bytes still appear exactly once in this crate.
    fn try_from(b: u8) -> Result<Self> {
        Self::from_repr(b).ok_or(Error::MalformedPacket)
    }
}

/////////////////////////////////////////////////////////////////////////////

/// The typed value of an MQTT v5 property, independent of which property
/// carries it.
///
/// Internal: [`Property`] carries its value in the variant, so this is not
/// how a value is stored or encoded. It remains as the vocabulary for the two
/// places that handle a value whose type is known only at run time —
/// `Property::value` and `Property::from_value` — and as the source
/// [`PropertyType`] is generated from. Callers reach values through the typed
/// accessors on [`Properties`] instead.
#[derive(Debug, Clone, PartialEq, Eq, EnumDiscriminants)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[strum_discriminants(name(PropertyType), vis(pub))]
#[strum_discriminants(derive(Hash, PartialOrd, Ord))]
#[strum_discriminants(cfg_attr(feature = "serde", derive(Serialize, Deserialize)))]
#[strum_discriminants(doc = "The wire encoding type of an MQTT v5 property value.")]
#[strum_discriminants(doc = "")]
#[strum_discriminants(doc = "Generated from [`PropertyValue`], whose variants are these")]
#[strum_discriminants(doc = "types carrying a value, so the two cannot drift apart.")]
pub(crate) enum PropertyValue {
    /// Single byte.
    Byte(u8),
    /// Two-byte big-endian unsigned integer.
    U16(u16),
    /// Four-byte big-endian unsigned integer.
    U32(u32),
    /// Variable-byte integer (1–4 bytes).
    VarInt(u32),
    /// Two-byte length-prefixed binary blob.
    Binary(Bytes),
    /// Two-byte length-prefixed UTF-8 string.
    String(String),
    /// Two UTF-8 strings (key then value), each length-prefixed.
    StringPair(String, String),
}

/////////////////////////////////////////////////////////////////////////////

/// An MQTT v5 property: its identifier and the value that identifier's wire
/// type calls for.
///
/// The discriminants are the identifier bytes from the specification's
/// Table 2-4, so a variant carries its own identifier; see
/// [`property_id`](Self::property_id).
#[repr(u8)]
#[derive(Debug, Clone, PartialEq, Eq, EnumDiscriminants)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[strum_discriminants(name(PropertyId), vis(pub))]
#[strum_discriminants(derive(Hash, PartialOrd, Ord, FromRepr))]
#[strum_discriminants(cfg_attr(feature = "serde", derive(Serialize, Deserialize)))]
#[strum_discriminants(doc = "MQTT v5 property identifiers (§2.2.2.2, Table 2-4).")]
#[strum_discriminants(doc = "")]
#[strum_discriminants(doc = "Generated from [`Property`], whose variants carry these")]
#[strum_discriminants(doc = "bytes as their discriminants, so the identifier table is")]
#[strum_discriminants(doc = "written once. Each identifier has a fixed associated wire")]
#[strum_discriminants(doc = "type; see [`value_type`](Self::value_type).")]
pub enum Property {
    /// 0 for unspecified bytes, 1 for UTF-8 text \[MQTT-3.3.2.3.2\].
    PayloadFormatIndicator(u8) = 0x01,
    /// How long the server may hold the message for a subscriber, in seconds.
    MessageExpiryInterval(u32) = 0x02,
    /// The content type of the payload, a MIME type by convention.
    ContentType(String) = 0x03,
    /// The topic a response to this request should be published to.
    ResponseTopic(String) = 0x08,
    /// Data a requester uses to match a response to its request.
    CorrelationData(Bytes) = 0x09,
    /// A subscription identifier; encoded as a variable-byte integer.
    SubscriptionIdentifier(u32) = 0x0B,
    /// How long the session outlives the connection, in seconds.
    SessionExpiryInterval(u32) = 0x11,
    /// The client identifier the server assigned.
    AssignedClientIdentifier(String) = 0x12,
    /// The keep-alive the server imposes, in seconds.
    ServerKeepAlive(u16) = 0x13,
    /// The name of the enhanced authentication method.
    AuthenticationMethod(String) = 0x15,
    /// Data for this step of an enhanced authentication exchange.
    AuthenticationData(Bytes) = 0x16,
    /// Whether the sender wants a reason string or user properties on failure.
    RequestProblemInformation(u8) = 0x17,
    /// How long the server waits before publishing the Will, in seconds.
    WillDelayInterval(u32) = 0x18,
    /// Whether the client asks the server for response information.
    RequestResponseInformation(u8) = 0x19,
    /// The base of a response topic the server suggests.
    ResponseInformation(String) = 0x1A,
    /// Another server the client should use instead.
    ServerReference(String) = 0x1C,
    /// The human-readable reason accompanying a reason code.
    ReasonString(String) = 0x1F,
    /// How many QoS 1 and 2 publications the sender will handle at once.
    ReceiveMaximum(u16) = 0x21,
    /// The highest topic alias the sender will accept.
    TopicAliasMaximum(u16) = 0x22,
    /// The alias standing in for a PUBLISH's topic name.
    TopicAlias(u16) = 0x23,
    /// The highest QoS the server accepts, 0 or 1.
    MaximumQoS(u8) = 0x24,
    /// Whether the server supports retained messages.
    RetainAvailable(u8) = 0x25,
    /// A key/value pair an application defines for itself; may repeat.
    UserProperty(String, String) = 0x26,
    /// The largest packet the sender will accept, in bytes.
    MaximumPacketSize(u32) = 0x27,
    /// Whether the server supports wildcard subscriptions.
    WildcardSubscriptionAvailable(u8) = 0x28,
    /// Whether the server supports subscription identifiers.
    SubscriptionIdentifierAvailable(u8) = 0x29,
    /// Whether the server supports shared subscriptions.
    SharedSubscriptionAvailable(u8) = 0x2A,
}

impl Property {
    /// The identifier of this property.
    ///
    /// The variant's own discriminant, as mapped by the `EnumDiscriminants`
    /// derive above: [`PropertyId`] is generated from this enum, so the
    /// identifier bytes are written once and this mapping needs neither a
    /// match of its own nor a read of the tag.
    pub fn property_id(&self) -> PropertyId {
        PropertyId::from(self)
    }

    /// The wire type of this property's value.
    pub fn value_type(&self) -> PropertyType {
        self.property_id().value_type()
    }

    /// This property's value, as a type-tagged [`PropertyValue`].
    ///
    /// The variant already carries the value; this copies it out for code
    /// working by identifier rather than by variant, so it allocates for the
    /// string and binary properties. Matching the variant avoids that, which
    /// is what the accessors on [`Properties`] do.
    ///
    // Unused by the library: the accessors read values out of the variant
    // directly, so nothing here needs the tagged form, and the only callers
    // are the tests that prove this and `from_value` are inverses. It is kept
    // rather than deleted because the pair is the crate's single way to
    // speak about a value whose type is known only at run time — what a
    // decoder for another format, a generic rules engine, or a different
    // fuzzing strategy would need — and because `PropertyValue` has to exist
    // regardless, being what `PropertyType` is generated from. Give it a
    // caller and the attribute comes off; find none for a release or two and
    // delete the pair, at which point hand-writing `PropertyType` is the
    // tidier end state.
    #[allow(dead_code)]
    pub(crate) fn value(&self) -> PropertyValue {
        use Property::*;
        match self {
            PayloadFormatIndicator(v)
            | RequestProblemInformation(v)
            | RequestResponseInformation(v)
            | MaximumQoS(v)
            | RetainAvailable(v)
            | WildcardSubscriptionAvailable(v)
            | SubscriptionIdentifierAvailable(v)
            | SharedSubscriptionAvailable(v) => PropertyValue::Byte(*v),
            ServerKeepAlive(n) | ReceiveMaximum(n) | TopicAliasMaximum(n) | TopicAlias(n) => {
                PropertyValue::U16(*n)
            }
            MessageExpiryInterval(n)
            | SessionExpiryInterval(n)
            | WillDelayInterval(n)
            | MaximumPacketSize(n) => PropertyValue::U32(*n),
            SubscriptionIdentifier(n) => PropertyValue::VarInt(*n),
            CorrelationData(b) | AuthenticationData(b) => PropertyValue::Binary(b.clone()),
            ContentType(s)
            | ResponseTopic(s)
            | AssignedClientIdentifier(s)
            | AuthenticationMethod(s)
            | ResponseInformation(s)
            | ServerReference(s)
            | ReasonString(s) => PropertyValue::String(s.clone()),
            UserProperty(k, v) => PropertyValue::StringPair(k.clone(), v.clone()),
        }
    }

    /// Builds a property from an identifier and a loose value.
    ///
    /// The inverse of [`value`](Self::value), for code that works by
    /// identifier — a decoder for another format, a generic tool, a fuzzer —
    /// rather than naming a variant. Returns
    /// [`ProtocolError`](Error::ProtocolError) when the value is not of the
    /// type the identifier calls for, which naming a variant makes
    /// impossible and which this cannot.
    ///
    // Unused by the library, and kept for the reasons given on `value`.
    #[allow(dead_code)]
    pub(crate) fn from_value(id: PropertyId, value: PropertyValue) -> Result<Self> {
        use PropertyId as Id;
        use PropertyValue as Val;
        Ok(match (id, value) {
            (Id::PayloadFormatIndicator, Val::Byte(x)) => Self::PayloadFormatIndicator(x),
            (Id::MessageExpiryInterval, Val::U32(x)) => Self::MessageExpiryInterval(x),
            (Id::ContentType, Val::String(x)) => Self::ContentType(x),
            (Id::ResponseTopic, Val::String(x)) => Self::ResponseTopic(x),
            (Id::CorrelationData, Val::Binary(x)) => Self::CorrelationData(x),
            (Id::SubscriptionIdentifier, Val::VarInt(x)) => Self::SubscriptionIdentifier(x),
            (Id::SessionExpiryInterval, Val::U32(x)) => Self::SessionExpiryInterval(x),
            (Id::AssignedClientIdentifier, Val::String(x)) => Self::AssignedClientIdentifier(x),
            (Id::ServerKeepAlive, Val::U16(x)) => Self::ServerKeepAlive(x),
            (Id::AuthenticationMethod, Val::String(x)) => Self::AuthenticationMethod(x),
            (Id::AuthenticationData, Val::Binary(x)) => Self::AuthenticationData(x),
            (Id::RequestProblemInformation, Val::Byte(x)) => Self::RequestProblemInformation(x),
            (Id::WillDelayInterval, Val::U32(x)) => Self::WillDelayInterval(x),
            (Id::RequestResponseInformation, Val::Byte(x)) => Self::RequestResponseInformation(x),
            (Id::ResponseInformation, Val::String(x)) => Self::ResponseInformation(x),
            (Id::ServerReference, Val::String(x)) => Self::ServerReference(x),
            (Id::ReasonString, Val::String(x)) => Self::ReasonString(x),
            (Id::ReceiveMaximum, Val::U16(x)) => Self::ReceiveMaximum(x),
            (Id::TopicAliasMaximum, Val::U16(x)) => Self::TopicAliasMaximum(x),
            (Id::TopicAlias, Val::U16(x)) => Self::TopicAlias(x),
            (Id::MaximumQoS, Val::Byte(x)) => Self::MaximumQoS(x),
            (Id::RetainAvailable, Val::Byte(x)) => Self::RetainAvailable(x),
            (Id::UserProperty, Val::StringPair(k, x)) => Self::UserProperty(k, x),
            (Id::MaximumPacketSize, Val::U32(x)) => Self::MaximumPacketSize(x),
            (Id::WildcardSubscriptionAvailable, Val::Byte(x)) => {
                Self::WildcardSubscriptionAvailable(x)
            }
            (Id::SubscriptionIdentifierAvailable, Val::Byte(x)) => {
                Self::SubscriptionIdentifierAvailable(x)
            }
            (Id::SharedSubscriptionAvailable, Val::Byte(x)) => Self::SharedSubscriptionAvailable(x),
            _ => return Err(Error::ProtocolError),
        })
    }

    /// Number of bytes this property occupies on the wire, identifier byte
    /// included.
    pub fn encoded_size(&self) -> usize {
        use Property::*;
        let value = match self {
            PayloadFormatIndicator(_)
            | RequestProblemInformation(_)
            | RequestResponseInformation(_)
            | MaximumQoS(_)
            | RetainAvailable(_)
            | WildcardSubscriptionAvailable(_)
            | SubscriptionIdentifierAvailable(_)
            | SharedSubscriptionAvailable(_) => 1,
            ServerKeepAlive(_) | ReceiveMaximum(_) | TopicAliasMaximum(_) | TopicAlias(_) => 2,
            MessageExpiryInterval(_)
            | SessionExpiryInterval(_)
            | WillDelayInterval(_)
            | MaximumPacketSize(_) => 4,
            SubscriptionIdentifier(n) => varlen::encoded_size(*n),
            CorrelationData(b) | AuthenticationData(b) => 2 + b.len(),
            ContentType(s)
            | ResponseTopic(s)
            | AssignedClientIdentifier(s)
            | AuthenticationMethod(s)
            | ResponseInformation(s)
            | ServerReference(s)
            | ReasonString(s) => 2 + s.len(),
            UserProperty(k, v) => 2 + k.len() + 2 + v.len(),
        };
        1 + value
    }

    /// Checks the value against the range the specification allows for this
    /// identifier, the only validation a variant cannot express in its type.
    pub fn validate(&self) -> Result<()> {
        use Property::*;
        let ok = match self {
            PayloadFormatIndicator(v)
            | RequestProblemInformation(v)
            | RequestResponseInformation(v)
            | MaximumQoS(v)
            | RetainAvailable(v)
            | WildcardSubscriptionAvailable(v)
            | SubscriptionIdentifierAvailable(v)
            | SharedSubscriptionAvailable(v) => matches!(v, 0 | 1),
            ReceiveMaximum(n) | TopicAlias(n) => *n > 0,
            MaximumPacketSize(n) => *n > 0,
            SubscriptionIdentifier(n) => *n > 0,
            ResponseTopic(t) => !t.bytes().any(|b| b == b'+' || b == b'#'),
            _ => true,
        };
        if ok {
            Ok(())
        } else {
            Err(Error::ProtocolError)
        }
    }

    /// Writes the property, identifier byte first.
    pub fn encode<B: BufMut>(&self, dst: &mut B) -> Result<()> {
        use Property::*;
        dst.put_u8(self.property_id() as u8);
        match self {
            PayloadFormatIndicator(v)
            | RequestProblemInformation(v)
            | RequestResponseInformation(v)
            | MaximumQoS(v)
            | RetainAvailable(v)
            | WildcardSubscriptionAvailable(v)
            | SubscriptionIdentifierAvailable(v)
            | SharedSubscriptionAvailable(v) => dst.put_u8(*v),
            ServerKeepAlive(n) | ReceiveMaximum(n) | TopicAliasMaximum(n) | TopicAlias(n) => {
                dst.put_u16(*n)
            }
            MessageExpiryInterval(n)
            | SessionExpiryInterval(n)
            | WillDelayInterval(n)
            | MaximumPacketSize(n) => dst.put_u32(*n),
            SubscriptionIdentifier(n) => varlen::encode(*n, dst)?,
            CorrelationData(b) | AuthenticationData(b) => put_binary(b, dst)?,
            ContentType(s)
            | ResponseTopic(s)
            | AssignedClientIdentifier(s)
            | AuthenticationMethod(s)
            | ResponseInformation(s)
            | ServerReference(s)
            | ReasonString(s) => put_utf8(s, dst)?,
            UserProperty(k, v) => {
                put_utf8(k, dst)?;
                put_utf8(v, dst)?;
            }
        }
        Ok(())
    }

    /// Reads one property, identifier byte first.
    ///
    /// The identifier decides the variant, so the value's type is right by
    /// construction and only its range needs checking.
    pub fn decode<B: Buf>(src: &mut B) -> Result<Self> {
        if !src.has_remaining() {
            return Err(Error::PacketTooShort);
        }
        let id = PropertyId::try_from(src.get_u8())?;
        let prop = Self::decode_value(id, src)?;
        prop.validate()?;
        Ok(prop)
    }

    /// Reads the value for an identifier already taken from the wire.
    fn decode_value<B: Buf>(id: PropertyId, src: &mut B) -> Result<Self> {
        use PropertyId as Id;

        fn byte<B: Buf>(src: &mut B) -> Result<u8> {
            if src.remaining() < 1 {
                return Err(Error::PacketTooShort);
            }
            Ok(src.get_u8())
        }
        fn u16_<B: Buf>(src: &mut B) -> Result<u16> {
            if src.remaining() < 2 {
                return Err(Error::PacketTooShort);
            }
            Ok(src.get_u16())
        }
        fn u32_<B: Buf>(src: &mut B) -> Result<u32> {
            if src.remaining() < 4 {
                return Err(Error::PacketTooShort);
            }
            Ok(src.get_u32())
        }

        Ok(match id {
            Id::PayloadFormatIndicator => Self::PayloadFormatIndicator(byte(src)?),
            Id::MessageExpiryInterval => Self::MessageExpiryInterval(u32_(src)?),
            Id::ContentType => Self::ContentType(get_utf8(src)?),
            Id::ResponseTopic => Self::ResponseTopic(get_utf8(src)?),
            Id::CorrelationData => Self::CorrelationData(get_binary(src)?),
            Id::SubscriptionIdentifier => {
                Self::SubscriptionIdentifier(varlen::decode(src)?.ok_or(Error::PacketTooShort)?)
            }
            Id::SessionExpiryInterval => Self::SessionExpiryInterval(u32_(src)?),
            Id::AssignedClientIdentifier => Self::AssignedClientIdentifier(get_utf8(src)?),
            Id::ServerKeepAlive => Self::ServerKeepAlive(u16_(src)?),
            Id::AuthenticationMethod => Self::AuthenticationMethod(get_utf8(src)?),
            Id::AuthenticationData => Self::AuthenticationData(get_binary(src)?),
            Id::RequestProblemInformation => Self::RequestProblemInformation(byte(src)?),
            Id::WillDelayInterval => Self::WillDelayInterval(u32_(src)?),
            Id::RequestResponseInformation => Self::RequestResponseInformation(byte(src)?),
            Id::ResponseInformation => Self::ResponseInformation(get_utf8(src)?),
            Id::ServerReference => Self::ServerReference(get_utf8(src)?),
            Id::ReasonString => Self::ReasonString(get_utf8(src)?),
            Id::ReceiveMaximum => Self::ReceiveMaximum(u16_(src)?),
            Id::TopicAliasMaximum => Self::TopicAliasMaximum(u16_(src)?),
            Id::TopicAlias => Self::TopicAlias(u16_(src)?),
            Id::MaximumQoS => Self::MaximumQoS(byte(src)?),
            Id::RetainAvailable => Self::RetainAvailable(byte(src)?),
            Id::UserProperty => {
                let k = get_utf8(src)?;
                let x = get_utf8(src)?;
                Self::UserProperty(k, x)
            }
            Id::MaximumPacketSize => Self::MaximumPacketSize(u32_(src)?),
            Id::WildcardSubscriptionAvailable => Self::WildcardSubscriptionAvailable(byte(src)?),
            Id::SubscriptionIdentifierAvailable => {
                Self::SubscriptionIdentifierAvailable(byte(src)?)
            }
            Id::SharedSubscriptionAvailable => Self::SharedSubscriptionAvailable(byte(src)?),
        })
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

    /// Starts building a collection a property at a time.
    ///
    /// Shorthand for [`PropertiesBuilder::new`].
    pub fn builder() -> PropertiesBuilder {
        PropertiesBuilder::new()
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
        if prop.validate().is_err() || self.seen.note(prop.property_id()).is_err() {
            self.seen.mark_invalid();
        }
        self.payload_size += prop.encoded_size();
        self.props.push(prop);
    }

    /// Adds `prop`, reporting what [`push`](Self::push) records silently.
    ///
    /// Returns [`Error::ProtocolError`] and **leaves the collection
    /// untouched** when the value is out of range for its identifier (see
    /// [`Property::validate`]), or when the identifier is one the
    /// specification forbids repeating and is already present. Nothing is
    /// added and the collection stays valid, so a caller can report the
    /// problem and carry on with what it has.
    ///
    /// Prefer this wherever the value came from outside the program. `push`
    /// remains the infallible form that [`Extend`] and [`FromIterator`]
    /// require: it adds the property and marks the collection invalid, which
    /// [`encode`](Self::encode) refuses later — correct, but reported far
    /// from the mistake.
    ///
    /// ```
    /// use paho_mqtt_codec::{Properties, Property, PropertyId};
    ///
    /// let mut props = Properties::new();
    /// props.try_push(Property::TopicAlias(7)).unwrap();
    ///
    /// // A topic alias of 0 is not allowed (§3.3.2.3.4).
    /// assert!(props.try_push(Property::TopicAlias(0)).is_err());
    /// // Nor may a second one appear.
    /// assert!(props.try_push(Property::TopicAlias(9)).is_err());
    ///
    /// // Neither attempt changed anything.
    /// assert_eq!(props.len(), 1);
    /// assert_eq!(props.get_u16(PropertyId::TopicAlias), Some(7));
    /// assert!(props.is_valid());
    /// ```
    pub fn try_push(&mut self, prop: Property) -> Result<()> {
        prop.validate()?;
        self.seen.note(prop.property_id())?;
        self.payload_size += prop.encoded_size();
        self.props.push(prop);
        Ok(())
    }

    /// Returns `false` if the list holds a property the specification does
    /// not allow on its own — a value out of range for its identifier, or a
    /// repeat of an identifier that may not repeat.
    ///
    /// Such a list is refused by [`encode`](Self::encode), so this answers
    /// "will this encode?" without building the packet, for a caller that
    /// used [`push`](Self::push) and wants to know before sending. A list
    /// that was never given a bad property is always valid, and one becomes
    /// valid again if the offending property is taken back out with
    /// [`retain`](Self::retain), [`remove`](Self::remove) or
    /// [`clear`](Self::clear).
    ///
    /// It does not speak for the *packet*: a property may be valid here and
    /// still not be allowed in the packet carrying it — that is
    /// [`check_context`](Self::check_context) — and encoding any non-empty
    /// list for a v3 connection is an error whatever this returns.
    pub fn is_valid(&self) -> bool {
        !self.seen.is_invalid()
    }

    // The generic setters that took an identifier and a loose value are gone:
    // with the identifier in the variant there is nothing for
    // `push_byte(PropertyId::ContentType, 1)` to build. Use the per-property
    // setters below, `push_user`, or `push(Property::ContentType(..))`.

    /// Returns the value of the first property with the given `id`, or `None`.
    ///
    /// This is the borrowing accessor. The typed `get_*` methods below return
    /// the value already unwrapped, at the cost of a clone for the owned
    /// types.
    pub fn get(&self, id: PropertyId) -> Option<&Property> {
        self.props.iter().find(|p| p.property_id() == id)
    }

    /// Returns the value of the `idx`-th property with the given `id`, or
    /// `None`.
    ///
    /// Only `UserProperty`, and `SubscriptionIdentifier` in a PUBLISH, may
    /// appear more than once, so `idx` is 0 for every other identifier.
    pub fn get_at(&self, id: PropertyId, idx: usize) -> Option<&Property> {
        self.props.iter().filter(|p| p.property_id() == id).nth(idx)
    }

    /// Returns the value of the first property with the given `id` as a byte,
    /// or `None` if it is absent or not a byte property.
    pub fn get_byte(&self, id: PropertyId) -> Option<u8> {
        self.get_byte_at(id, 0)
    }

    /// Returns the `idx`-th `id` property's value as a byte.
    pub fn get_byte_at(&self, id: PropertyId, idx: usize) -> Option<u8> {
        use Property::*;
        match self.get_at(id, idx)? {
            PayloadFormatIndicator(v)
            | RequestProblemInformation(v)
            | RequestResponseInformation(v)
            | MaximumQoS(v)
            | RetainAvailable(v)
            | WildcardSubscriptionAvailable(v)
            | SubscriptionIdentifierAvailable(v)
            | SharedSubscriptionAvailable(v) => Some(*v),
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
        use Property::*;
        match self.get_at(id, idx)? {
            ServerKeepAlive(v) | ReceiveMaximum(v) | TopicAliasMaximum(v) | TopicAlias(v) => {
                Some(*v)
            }
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
        use Property::*;
        match self.get_at(id, idx)? {
            MessageExpiryInterval(v)
            | SessionExpiryInterval(v)
            | WillDelayInterval(v)
            | MaximumPacketSize(v)
            | SubscriptionIdentifier(v) => Some(*v),
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
        match self.get_at(id, idx)? {
            Property::SubscriptionIdentifier(v) => Some(*v),
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
        use Property::*;
        match self.get_at(id, idx)? {
            CorrelationData(v) | AuthenticationData(v) => Some(v.clone()),
            _ => None,
        }
    }

    /// Returns the first property with the given `id` as a borrowed string,
    /// or `None` if it is absent or does not carry a single string.
    ///
    /// The borrowing counterpart of [`get_string`](Self::get_string), for the
    /// common case of reading a string out of a received packet without
    /// copying it. `UserProperty` carries a *pair* and so is not a single
    /// string: use [`get_user`](Self::get_user) or
    /// [`user_iter`](Self::user_iter) for those.
    ///
    /// ```
    /// use paho_mqtt_codec::{Properties, Property, PropertyId};
    ///
    /// let mut props = Properties::new();
    /// props.push(Property::ContentType("application/json".into()));
    /// assert_eq!(props.get_str(PropertyId::ContentType), Some("application/json"));
    /// assert_eq!(props.get_str(PropertyId::ReasonString), None);
    /// ```
    pub fn get_str(&self, id: PropertyId) -> Option<&str> {
        use Property::*;
        match self.get(id)? {
            ContentType(s)
            | ResponseTopic(s)
            | AssignedClientIdentifier(s)
            | AuthenticationMethod(s)
            | ResponseInformation(s)
            | ServerReference(s)
            | ReasonString(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Returns the first property with the given `id` as borrowed bytes, or
    /// `None` if it is absent or not a binary property.
    ///
    /// The borrowing counterpart of [`get_binary`](Self::get_binary). Cloning
    /// a [`Bytes`] is cheap, but a caller that only needs to look at the
    /// bytes — comparing authentication data, hashing correlation data —
    /// needs neither the clone nor the refcount traffic.
    pub fn get_bytes(&self, id: PropertyId) -> Option<&[u8]> {
        use Property::*;
        match self.get(id)? {
            CorrelationData(b) | AuthenticationData(b) => Some(&b[..]),
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
        use Property::*;
        match self.get_at(id, idx)? {
            ContentType(v)
            | ResponseTopic(v)
            | AssignedClientIdentifier(v)
            | AuthenticationMethod(v)
            | ResponseInformation(v)
            | ServerReference(v)
            | ReasonString(v) => Some(v.clone()),
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
        match self.get_at(id, idx)? {
            Property::UserProperty(k, v) => Some((k.clone(), v.clone())),
            _ => None,
        }
    }

    /// Whether any property with the given `id` is present.
    ///
    /// ```
    /// use paho_mqtt_codec::{Properties, Property, PropertyId};
    ///
    /// let mut props = Properties::new();
    /// props.push_user("trace-id", "abc123");
    ///
    /// assert!(props.contains(PropertyId::UserProperty));
    /// assert!(!props.contains(PropertyId::ContentType));
    /// ```
    pub fn contains(&self, id: PropertyId) -> bool {
        // A scan rather than the seen-identifier set, which records only the
        // identifiers that may *not* repeat: it would answer `false` for a
        // user property that is plainly there.
        self.props.iter().any(|p| p.property_id() == id)
    }

    /// How many properties with the given `id` the list holds.
    ///
    /// At most one for every identifier except `UserProperty`, and
    /// `SubscriptionIdentifier` in a PUBLISH, which the specification allows
    /// to repeat — so this is the count to check before indexing with
    /// [`get_at`](Self::get_at).
    ///
    /// ```
    /// use paho_mqtt_codec::{Properties, Property, PropertyId};
    ///
    /// let mut props = Properties::new();
    /// props.push_user("a", "1");
    /// props.push_user("b", "2");
    /// props.push(Property::ContentType("text/plain".into()));
    ///
    /// assert_eq!(props.count(PropertyId::UserProperty), 2);
    /// assert_eq!(props.count(PropertyId::ContentType), 1);
    /// assert_eq!(props.count(PropertyId::ReasonString), 0);
    /// assert_eq!(props.len(), 3, "`len` counts every property");
    /// ```
    pub fn count(&self, id: PropertyId) -> usize {
        self.props.iter().filter(|p| p.property_id() == id).count()
    }

    /// Iterates over the values of every property with the given `id`, in
    /// order.
    ///
    /// Only `UserProperty`, and `SubscriptionIdentifier` in a PUBLISH, can
    /// yield more than one. This is v0.14's `iter(code)`; the name differs
    /// because [`iter`](Self::iter) here walks the whole collection.
    pub fn get_all(&self, id: PropertyId) -> impl Iterator<Item = &Property> {
        self.props.iter().filter(move |p| p.property_id() == id)
    }

    /// Adds a user property, the only string-pair property and the only one
    /// an application defines for itself.
    pub fn push_user<K, V>(&mut self, key: K, val: V)
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.push(Property::UserProperty(key.into(), val.into()))
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
        self.props.iter().filter_map(|p| match p {
            Property::UserProperty(k, v) => Some((k.as_str(), v.as_str())),
            _ => None,
        })
    }

    /// Whether a user property with the given `key` is present.
    ///
    /// Keys are not unique — an application may send the same one more than
    /// once — so this answers "at least one", and
    /// [`user_iter`](Self::user_iter) walks them all. It matches on the key
    /// only: a value equal to `key` does not count.
    ///
    /// ```
    /// use paho_mqtt_codec::Properties;
    ///
    /// let mut props = Properties::new();
    /// props.push_user("trace-id", "abc123");
    ///
    /// assert!(props.contains_user("trace-id"));
    /// assert!(!props.contains_user("span-id"));
    /// assert!(!props.contains_user("abc123"), "the value is not a key");
    /// ```
    pub fn contains_user(&self, key: &str) -> bool {
        self.user_iter().any(|(k, _)| k == key)
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
        self.retain(|p| p.property_id() != id);
        before - self.props.len()
    }

    /// Removes every user property with the given `key`, returning how many
    /// were removed.
    pub fn remove_user(&mut self, key: &str) -> usize {
        let before = self.props.len();
        self.retain(|p| !matches!(p, Property::UserProperty(k, _) if k == key));
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
            if prop.validate().is_err() || seen.note(prop.property_id()).is_err() {
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
            if allowed & (1 << (p.property_id() as u8)) == 0 {
                return Err(Error::ProtocolError);
            }
            if p.property_id() == PropertyId::SubscriptionIdentifier {
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
            if allowed & (1 << (prop.property_id() as u8)) == 0 {
                return Err(Error::ProtocolError);
            }
            seen.note(prop.property_id())?;
            if prop.property_id() == PropertyId::SubscriptionIdentifier {
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

/////////////////////////////////////////////////////////////////////////////
//                            PropertiesBuilder
/////////////////////////////////////////////////////////////////////////////

/// Builds a [`Properties`] collection a property at a time.
///
/// Each setter is named for its property, without the `push_` prefix the
/// collection's own methods carry, and takes the Rust type that property's
/// wire type calls for — so `content_type("application/json")` takes a
/// string, `topic_alias(7)` a `u16`, and `retain_available(true)` a `bool`.
/// The setters and the collection's `push_*` methods are generated from one
/// table, so the two can never disagree.
///
/// Each setter consumes the builder and returns it, and
/// [`finalize`](Self::finalize) yields the collection.
/// [`Properties::builder`] is a shorthand for [`new`](Self::new).
///
/// ```
/// use paho_mqtt_codec::Properties;
///
/// let props = Properties::builder()
///     .message_expiry_interval(3600)
///     .content_type("application/json")
///     .response_topic("reply/here")
///     .user("trace-id", "abc123")
///     .finalize();
///
/// assert_eq!(props.get_content_type().as_deref(), Some("application/json"));
/// assert_eq!(props.get_message_expiry_interval(), Some(3600));
/// ```
///
/// The builder does not validate, as nothing in this crate does before
/// encoding: a property whose value is out of range, or which repeats where
/// the specification forbids it, marks the collection invalid and is refused
/// when the packet carrying it is encoded.
#[derive(Debug, Default, Clone)]
pub struct PropertiesBuilder(Properties);

impl PropertiesBuilder {
    /// Creates a builder for an empty collection.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a user property, the one property an application defines for
    /// itself and the only one that may repeat freely.
    ///
    /// The builder's form of [`Properties::push_user`].
    pub fn user<K, V>(mut self, key: K, val: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.0.push_user(key, val);
        self
    }

    /// Adds an already-built property, for an identifier held in a variable
    /// rather than named in the source.
    ///
    /// The builder's form of [`Properties::push`].
    pub fn property(mut self, prop: Property) -> Self {
        self.0.push(prop);
        self
    }

    /// Finalizes the builder into the collection.
    pub fn finalize(self) -> Properties {
        self.0
    }
}

impl From<PropertiesBuilder> for Properties {
    fn from(builder: PropertiesBuilder) -> Self {
        builder.finalize()
    }
}

/// Lets a builder stand in for the optional properties that the
/// acknowledgement and AUTH constructors take, so that a caller writing
/// `PubAck::with_properties(id, rc, Properties::builder().reason_string("no"))`
/// need not finalize it first.
///
/// The standard library's `From<T> for Option<T>` covers a finished
/// [`Properties`]; this covers the builder, which is a different type and so
/// does not overlap it.
impl From<PropertiesBuilder> for Option<Properties> {
    fn from(builder: PropertiesBuilder) -> Self {
        Some(builder.finalize())
    }
}

/////////////////////////////////////////////////////////////////////////////
//                      Per-property typed accessors
/////////////////////////////////////////////////////////////////////////////

/// Writes one `push_*` / `get_*` pair per property identifier.
///
/// The table below is the point of this macro: it states every identifier's
/// wire type once, in one place, so that a caller never has to remember which
/// `push_*` goes with which identifier — and so that a `push_x` cannot
/// disagree with its `get_x`, both being generated from the same row.
///
/// Each row is: the identifier, the name shared by the pair, the generic
/// accessor underneath, the type the setter takes, the type the getter
/// returns, and what the property means.
macro_rules! typed_property_accessors {
    ($(
        $id:ident, $name:ident, $push_name:ident, $get_name:ident,
            $get_fn:ident, $push_ty:ty, $get_ty:ty,
            $what:expr;
    )+) => {
        impl Properties {
            $(
                #[doc = concat!("Adds ", $what, ".")]
                ///
                #[doc = concat!(
                    "Shorthand for [`push`](Self::push) with [`Property::",
                    stringify!($id), "`](Property::", stringify!($id),
                    "), whose payload type it takes."
                )]
                pub fn $push_name(&mut self, val: $push_ty) {
                    self.push(Property::$id(val.into()))
                }

                #[doc = concat!("Returns ", $what, ", or `None` if it is absent.")]
                ///
                #[doc = concat!(
                    "Shorthand for [`", stringify!($get_fn), "`](Self::", stringify!($get_fn),
                    ") with [`PropertyId::", stringify!($id), "`](PropertyId::", stringify!($id),
                    ")."
                )]
                pub fn $get_name(&self) -> Option<$get_ty> {
                    self.$get_fn(PropertyId::$id)
                }
            )+
        }

        impl PropertiesBuilder {
            $(
                #[doc = concat!("Sets ", $what, ".")]
                ///
                #[doc = concat!(
                    "The builder's form of [`Properties::", stringify!($push_name),
                    "`](Properties::", stringify!($push_name), ")."
                )]
                pub fn $name(mut self, val: $push_ty) -> Self {
                    self.0.$push_name(val);
                    self
                }
            )+
        }
    };
}

// `UserProperty` is deliberately absent: it is the one property an
// application defines for itself, it may repeat, and it already has
// `push_user`, `get_user` and `user_iter`, which serve it better than a pair
// generated here would.
typed_property_accessors! {
    PayloadFormatIndicator, payload_format_indicator, push_payload_format_indicator, get_payload_format_indicator,
        get_byte, u8, u8,
        r"the payload format indicator \[MQTT-3.3.2.3.2\]: 0 for unspecified bytes, 1 for UTF-8 text";
    MaximumQoS, maximum_qos, push_maximum_qos, get_maximum_qos,
        get_byte, u8, u8,
        r"the highest QoS the server accepts, 0 or 1 \[MQTT-3.2.2.3.4\]";
    ServerKeepAlive, server_keep_alive, push_server_keep_alive, get_server_keep_alive,
        get_u16, u16, u16,
        r"the keep-alive the server imposes, in seconds \[MQTT-3.2.2.3.14\]";
    ReceiveMaximum, receive_maximum, push_receive_maximum, get_receive_maximum,
        get_u16, u16, u16,
        r"how many QoS 1 and 2 publications the sender will handle at once \[MQTT-3.1.2.11.3\]";
    TopicAliasMaximum, topic_alias_maximum, push_topic_alias_maximum, get_topic_alias_maximum,
        get_u16, u16, u16,
        r"the highest topic alias the sender will accept \[MQTT-3.1.2.11.5\]";
    TopicAlias, topic_alias, push_topic_alias, get_topic_alias,
        get_u16, u16, u16,
        r"the topic alias standing in for a PUBLISH's topic name \[MQTT-3.3.2.3.4\]";
    MessageExpiryInterval, message_expiry_interval, push_message_expiry_interval, get_message_expiry_interval,
        get_u32, u32, u32,
        r"how long the server may hold this message for a subscriber, in seconds \[MQTT-3.3.2.3.3\]";
    SessionExpiryInterval, session_expiry_interval, push_session_expiry_interval, get_session_expiry_interval,
        get_u32, u32, u32,
        r"how long the session outlives the connection, in seconds \[MQTT-3.1.2.11.2\]";
    WillDelayInterval, will_delay_interval, push_will_delay_interval, get_will_delay_interval,
        get_u32, u32, u32,
        r"how long the server waits before publishing the Will, in seconds \[MQTT-3.1.3.2.2\]";
    MaximumPacketSize, maximum_packet_size, push_maximum_packet_size, get_maximum_packet_size,
        get_u32, u32, u32,
        r"the largest packet the sender will accept, in bytes \[MQTT-3.1.2.11.4\]";
    SubscriptionIdentifier, subscription_identifier, push_subscription_identifier, get_subscription_identifier,
        get_var_int, impl Into<u32>, u32,
        r"the subscription identifier \[MQTT-3.8.2.1.2\], which a PUBLISH may carry more than once";
    CorrelationData, correlation_data, push_correlation_data, get_correlation_data,
        get_binary, impl Into<Bytes>, Bytes,
        r"the correlation data a requester uses to match a response to its request \[MQTT-3.3.2.3.6\]";
    AuthenticationData, authentication_data, push_authentication_data, get_authentication_data,
        get_binary, impl Into<Bytes>, Bytes,
        r"the data for this step of an enhanced authentication exchange \[MQTT-3.1.2.11.10\]";
    ContentType, content_type, push_content_type, get_content_type,
        get_string, impl Into<String>, String,
        r"the content type of the payload, a MIME type by convention \[MQTT-3.3.2.3.9\]";
    ResponseTopic, response_topic, push_response_topic, get_response_topic,
        get_string, impl Into<String>, String,
        r"the topic a response to this request should be published to \[MQTT-3.3.2.3.5\]";
    AssignedClientIdentifier, assigned_client_identifier, push_assigned_client_identifier, get_assigned_client_identifier,
        get_string, impl Into<String>, String,
        r"the client identifier the server assigned, the client having sent an empty one \[MQTT-3.2.2.3.7\]";
    AuthenticationMethod, authentication_method, push_authentication_method, get_authentication_method,
        get_string, impl Into<String>, String,
        r"the name of the enhanced authentication method \[MQTT-3.1.2.11.9\]";
    ResponseInformation, response_information, push_response_information, get_response_information,
        get_string, impl Into<String>, String,
        r"the base of a response topic the server suggests \[MQTT-3.2.2.3.15\]";
    ServerReference, server_reference, push_server_reference, get_server_reference,
        get_string, impl Into<String>, String,
        r"another server the client should use instead \[MQTT-3.2.2.3.16\]";
    ReasonString, reason_string, push_reason_string, get_reason_string,
        get_string, impl Into<String>, String,
        r"the human-readable reason accompanying a reason code \[MQTT-3.2.2.3.2\]";
}

/// Writes the pair for a property the specification restricts to 0 or 1 and
/// whose meaning is a yes/no, so that the pair can speak in `bool`.
///
/// Same table shape as [`typed_property_accessors`], minus the types: a
/// setter takes `bool` and a getter returns `Option<bool>`, converting at the
/// boundary. Every value the collection can hold is 0 or 1, since `push` and
/// `decode` both reject anything else for these identifiers, so the getter's
/// conversion cannot lose information.
macro_rules! flag_property_accessors {
    ($(
        $id:ident, $name:ident, $push_name:ident, $get_name:ident, $what:expr;
    )+) => {
        impl Properties {
            $(
                #[doc = concat!("Sets ", $what, ".")]
                ///
                /// The property itself is a one-byte value restricted to 0 or
                /// 1, which this writes as `0` for `false` and `1` for
                /// `true`; the variant
                #[doc = concat!(
                    "[`Property::", stringify!($id), "`](Property::",
                    stringify!($id), ") takes it as the `u8` it is on the wire."
                )]
                pub fn $push_name(&mut self, val: bool) {
                    self.push(Property::$id(u8::from(val)))
                }

                #[doc = concat!("Returns ", $what, ", or `None` if it is absent.")]
                ///
                /// Reads the one-byte property as a flag: `0` is `false` and
                /// `1` is `true`, and nothing else can be stored, since both
                /// `push` and `decode` refuse it. Use
                #[doc = concat!(
                    "[`get_byte`](Self::get_byte) with [`PropertyId::",
                    stringify!($id), "`](PropertyId::", stringify!($id),
                    ") for the raw byte."
                )]
                pub fn $get_name(&self) -> Option<bool> {
                    self.get_byte(PropertyId::$id).map(|v| v != 0)
                }
            )+
        }

        impl PropertiesBuilder {
            $(
                #[doc = concat!("Sets ", $what, ".")]
                ///
                /// The property is a one-byte 0 or 1 on the wire; see
                #[doc = concat!(
                    "[`Properties::", stringify!($push_name), "`](Properties::",
                    stringify!($push_name), ")."
                )]
                pub fn $name(mut self, val: bool) -> Self {
                    self.0.$push_name(val);
                    self
                }
            )+
        }
    };
}

// `PayloadFormatIndicator` and `MaximumQoS` are 0-or-1 too, but neither is a
// yes/no: the first says which of two payload formats is meant, the second is
// a QoS level. Both keep their `u8` pair above.
flag_property_accessors! {
    RequestProblemInformation, request_problem_information,
        push_request_problem_information, get_request_problem_information,
        r"whether the sender wants a reason string or user properties on a failure \[MQTT-3.1.2.11.7\]";
    RequestResponseInformation, request_response_information,
        push_request_response_information, get_request_response_information,
        r"whether the client asks the server for response information \[MQTT-3.1.2.11.6\]";
    RetainAvailable, retain_available,
        push_retain_available, get_retain_available,
        r"whether the server supports retained messages \[MQTT-3.2.2.3.5\]";
    WildcardSubscriptionAvailable, wildcard_subscription_available,
        push_wildcard_subscription_available, get_wildcard_subscription_available,
        r"whether the server supports wildcard subscriptions \[MQTT-3.2.2.3.11\]";
    SubscriptionIdentifierAvailable, subscription_identifier_available,
        push_subscription_identifier_available, get_subscription_identifier_available,
        r"whether the server supports subscription identifiers \[MQTT-3.2.2.3.12\]";
    SharedSubscriptionAvailable, shared_subscription_available,
        push_shared_subscription_available, get_shared_subscription_available,
        r"whether the server supports shared subscriptions \[MQTT-3.2.2.3.13\]";
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

/// Builds a collection from a borrowed slice, cloning each property.
///
/// Useful where the properties are held elsewhere — a table of defaults, a
/// set read from configuration — and the collection is built from them
/// repeatedly. Where the caller owns the properties,
/// [`From<Vec<Property>>`](Properties::from) takes them without cloning.
///
/// Each property goes through [`push`](Self::push), so the size cache and the
/// seen-identifier set are built as they would be one at a time, and an
/// invalid property marks the collection invalid rather than being refused
/// here.
///
/// An array converts too, borrowed or owned — see the two impls below.
impl From<&[Property]> for Properties {
    fn from(props: &[Property]) -> Self {
        let mut out = Self {
            props: Vec::with_capacity(props.len()),
            ..Self::default()
        };
        out.extend(props.iter().cloned());
        out
    }
}

/// Builds a collection from an array, taking the properties as they are.
///
/// This is the one for an array written in place — `[Property::new_u32(..),
/// Property::new_string(..)].into()` — which the slice impl would reach only
/// through a reference, and only by cloning.
impl<const N: usize> From<[Property; N]> for Properties {
    fn from(props: [Property; N]) -> Self {
        let mut out = Self {
            props: Vec::with_capacity(N),
            ..Self::default()
        };
        out.extend(props);
        out
    }
}

/// Builds a collection from a borrowed array, cloning each property.
///
/// The same as the slice impl, which an array reference cannot reach through
/// `into`: trait resolution does not see through the unsizing coercion from
/// `&[Property; N]` to `&[Property]`. With this, `(&arr).into()` and
/// `arr[..].into()` both work.
impl<const N: usize> From<&[Property; N]> for Properties {
    fn from(props: &[Property; N]) -> Self {
        Self::from(&props[..])
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
        props.push(Property::PayloadFormatIndicator(1));
        round_trip(&props);
    }

    #[test]
    fn test_byte_encoded_size() {
        let p = Property::RetainAvailable(1);
        assert_eq!(p.encoded_size(), 2); // 1 id + 1 byte
    }

    // ---- U16 properties ----

    #[test]
    fn test_receive_maximum_round_trip() {
        let mut props = Properties::new();
        props.push(Property::ReceiveMaximum(1000));
        round_trip(&props);
    }

    #[test]
    fn test_u16_encoded_size() {
        let p = Property::TopicAlias(42);
        assert_eq!(p.encoded_size(), 3); // 1 id + 2 bytes
    }

    #[test]
    fn test_receive_maximum_wire_bytes() {
        let mut props = Properties::new();
        props.push(Property::ReceiveMaximum(0x0064));
        let mut dst = BytesMut::new();
        props.encode(&mut dst, v5()).unwrap();
        // VBI(3), id=0x21, value=0x00 0x64
        assert_eq!(&dst[..], &[0x03, 0x21, 0x00, 0x64]);
    }

    // ---- U32 properties ----

    #[test]
    fn test_session_expiry_interval_round_trip() {
        let mut props = Properties::new();
        props.push(Property::SessionExpiryInterval(3600));
        round_trip(&props);
    }

    #[test]
    fn test_u32_encoded_size() {
        let p = Property::MessageExpiryInterval(0xDEAD_BEEF);
        assert_eq!(p.encoded_size(), 5); // 1 id + 4 bytes
    }

    // ---- VarInt property (SubscriptionIdentifier) ----

    #[test]
    fn test_subscription_identifier_single_byte() {
        let mut props = Properties::new();
        props.push(Property::SubscriptionIdentifier(5));
        round_trip(&props);
    }

    #[test]
    fn test_subscription_identifier_multibyte() {
        let mut props = Properties::new();
        props.push(Property::SubscriptionIdentifier(300));
        round_trip(&props);
    }

    // ---- Binary properties ----

    #[test]
    fn test_correlation_data_round_trip() {
        let mut props = Properties::new();
        props.push(Property::CorrelationData(Bytes::from_static(
            b"\x01\x02\x03",
        )));
        round_trip(&props);
    }

    #[test]
    fn test_binary_encoded_size() {
        let data = Bytes::from_static(b"hello");
        let p = Property::CorrelationData(data);
        assert_eq!(p.encoded_size(), 1 + 2 + 5); // id + len_prefix + data
    }

    // ---- String properties ----

    #[test]
    fn test_content_type_round_trip() {
        let mut props = Properties::new();
        props.push(Property::ContentType("application/json".to_string()));
        round_trip(&props);
    }

    #[test]
    fn test_reason_string_round_trip() {
        let mut props = Properties::new();
        props.push(Property::ReasonString("Normal".to_string()));
        round_trip(&props);
    }

    // ---- StringPair (UserProperty) ----

    #[test]
    fn test_user_property_round_trip() {
        let mut props = Properties::new();
        props.push(Property::UserProperty(
            "key".to_string(),
            "value".to_string(),
        ));
        round_trip(&props);
    }

    #[test]
    fn test_multiple_user_properties() {
        let mut props = Properties::new();
        props.push(Property::UserProperty("a".to_string(), "1".to_string()));
        props.push(Property::UserProperty("b".to_string(), "2".to_string()));
        round_trip(&props);
        // Both preserved in order.
        let mut count = 0;
        for p in props.iter() {
            assert_eq!(p.property_id(), PropertyId::UserProperty);
            count += 1;
        }
        assert_eq!(count, 2);
    }

    // ---- Multiple mixed properties ----

    #[test]
    fn test_mixed_properties_round_trip() {
        let mut props = Properties::new();
        props.push(Property::SessionExpiryInterval(120));
        props.push(Property::ReceiveMaximum(100));
        props.push(Property::UserProperty(
            "env".to_string(),
            "prod".to_string(),
        ));
        round_trip(&props);
    }

    // ---- get() accessor ----

    #[test]
    fn test_get_existing_property() {
        let mut props = Properties::new();
        props.push(Property::SessionExpiryInterval(60));
        assert_eq!(
            props.get(PropertyId::SessionExpiryInterval),
            Some(&Property::SessionExpiryInterval(60))
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
        props.push(Property::UserProperty("k".to_string(), "first".to_string()));
        props.push(Property::UserProperty(
            "k".to_string(),
            "second".to_string(),
        ));
        assert_eq!(
            props.get(PropertyId::UserProperty),
            Some(&Property::UserProperty(
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
        props.push(Property::MaximumPacketSize(65536));
        props.push(Property::TopicAliasMaximum(10));
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
        // Each variant is a constructor function, which is what lets this
        // iterate now that an identifier alone cannot build a property.
        let flags: [fn(u8) -> Property; 8] = [
            Property::PayloadFormatIndicator,
            Property::RequestProblemInformation,
            Property::RequestResponseInformation,
            Property::MaximumQoS,
            Property::RetainAvailable,
            Property::WildcardSubscriptionAvailable,
            Property::SubscriptionIdentifierAvailable,
            Property::SharedSubscriptionAvailable,
        ];
        for make in flags {
            let id = make(0).property_id();
            assert!(make(0).validate().is_ok(), "{id:?}");
            assert!(make(1).validate().is_ok(), "{id:?}");
            assert_eq!(make(2).validate(), Err(Error::ProtocolError), "{id:?}");
            // ... and on the wire
            assert_eq!(decode_raw(&[id as u8, 0x07]), Err(Error::ProtocolError));
            assert_eq!(encode_one(make(7)), Err(Error::ProtocolError));
        }
    }

    #[test]
    fn test_validate_response_topic_rejects_wildcards() {
        for t in ["a/#", "a/+/b", "+", "#"] {
            assert_eq!(
                Property::ResponseTopic(t.into()).validate(),
                Err(Error::ProtocolError),
                "{t}"
            );
        }
        assert!(Property::ResponseTopic("a/b".into()).validate().is_ok());
    }

    // A value whose type does not match its identifier used to be buildable
    // (`Property::new(PropertyId::MaximumQoS, PropertyValue::U32(1))`) and was
    // caught only by `validate`. With the identifier in the variant it cannot
    // be written down at all, so the test that covered it is gone: the
    // compiler covers it now.

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
        props.push(Property::TopicAlias(3));
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
        props.push(Property::ReceiveMaximum(1));
        props.push(Property::ReceiveMaximum(2));
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
        props.push(Property::UserProperty("a".to_string(), "1".to_string()));
        props.push(Property::UserProperty("a".to_string(), "2".to_string()));
        props.push(Property::SubscriptionIdentifier(1));
        props.push(Property::SubscriptionIdentifier(2));
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
            Property::MaximumPacketSize(65536),
            Property::TopicAliasMaximum(10),
            Property::UserProperty("key".to_string(), "value".to_string()),
            Property::CorrelationData(Bytes::from_static(b"abc")),
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
        // Every identifier, by way of the fixture that holds one of each
        // variant: this also covers the byte-to-identifier table that
        // `FromRepr` writes.
        for p in every_property() {
            let id = p.property_id();
            assert_eq!(PropertyId::try_from(id as u8), Ok(id), "{id:?}");
        }
        // Spot-checks that the discriminants are the specification's bytes.
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
        props.push(Property::CorrelationData(Bytes::from(
            alloc::vec![0u8; 65_536],
        )));
        let mut dst = BytesMut::new();
        assert_eq!(props.encode(&mut dst, v5()), Err(Error::BinaryTooLong));
    }

    #[test]
    fn test_encode_string_too_long_errors() {
        let s: String = core::iter::repeat_n('x', 65_536).collect();
        let mut props = Properties::new();
        props.push(Property::ContentType(s));
        let mut dst = BytesMut::new();
        assert_eq!(props.encode(&mut dst, v5()), Err(Error::StringTooLong));
    }

    #[test]
    fn test_encode_string_pair_value_too_long_errors() {
        let v: String = core::iter::repeat_n('x', 65_536).collect();
        let mut props = Properties::new();
        props.push(Property::UserProperty("k".to_string(), v));
        let mut dst = BytesMut::new();
        assert_eq!(props.encode(&mut dst, v5()), Err(Error::StringTooLong));
    }

    // ---- SubscriptionIdentifier repeats only in PUBLISH (§3.3.2.3.8, §3.8.2.1.2) ----

    fn two_sub_ids() -> Properties {
        let mut props = Properties::new();
        props.push(Property::SubscriptionIdentifier(1));
        props.push(Property::SubscriptionIdentifier(2));
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
        props.push(Property::UserProperty("k".into(), "v".into()));
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
        Property::AuthenticationMethod(name.into())
    }

    fn auth_data(bytes: &'static [u8]) -> Property {
        Property::AuthenticationData(Bytes::from_static(bytes))
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
        props.push(Property::PayloadFormatIndicator(1));
        props.push(Property::ReceiveMaximum(20));
        props.push(Property::SessionExpiryInterval(300));
        props.push(Property::SubscriptionIdentifier(9u32));
        props.push(Property::CorrelationData(Bytes::from_static(b"\x01\x02")));
        props.push(Property::ContentType("text/plain".into()));
        props.push(Property::UserProperty("k".into(), "v".into()));
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
        props.push(Property::UserProperty("a".into(), "1".into()));
        props.push(Property::UserProperty("b".into(), "2".into()));
        props.push(Property::UserProperty("c".into(), "3".into()));

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
        props.push(Property::ContentType("text/plain".into()));
        props.push(Property::UserProperty("a".into(), "1".into()));
        props.push(Property::ReceiveMaximum(7));
        props.push(Property::UserProperty("b".into(), "2".into()));

        assert_eq!(
            props.get_at(PropertyId::UserProperty, 1),
            Some(&Property::UserProperty("b".into(), "2".into()))
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
        props.push(Property::SubscriptionIdentifier(5u32));
        props.push(Property::SessionExpiryInterval(300));

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
        a.push(Property::SubscriptionIdentifier(7u32));
        let mut b = Properties::new();
        b.push(Property::SubscriptionIdentifier(7u32));
        assert_eq!(a, b);

        // A narrower integer converts.
        let mut c = Properties::new();
        c.push(Property::SubscriptionIdentifier(7));
        assert_eq!(c, a);
    }

    /// A PUBLISH may carry one `SubscriptionIdentifier` per matching
    /// subscription (§3.3.2.3.8), which is what `_at` is for.
    #[test]
    fn test_var_int_at_walks_repeated_subscription_identifiers() {
        let mut props = Properties::new();
        for id in [1u32, 2, 3] {
            props.push(Property::SubscriptionIdentifier(id));
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
        props.push(Property::ContentType("text/plain".into()));
        props.push_user("b", "2");
        props.push_user("a", "3");
        props
    }

    #[test]
    fn test_push_user_is_a_user_property() {
        let mut a = Properties::new();
        a.push_user("k", "v");
        let mut b = Properties::new();
        b.push(Property::UserProperty("k".into(), "v".into()));
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
            vec![&Property::ContentType("text/plain".into())]
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
        props.retain(|p| !matches!(p, Property::UserProperty(k, _) if k == "a"));
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
        props.retain(|p| !matches!(p, Property::UserProperty(_, v) if v.is_empty()));
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
        b.retain(|p| p.property_id() != PropertyId::UserProperty);
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
            |p: &mut Properties| p.retain(|prop| prop.property_id() == PropertyId::ContentType),
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
        props.push(Property::ReceiveMaximum(10));
        props.push(Property::ReceiveMaximum(20)); // a repeat: invalid
        let mut dst = BytesMut::new();
        assert_eq!(props.encode(&mut dst, v5()), Err(Error::ProtocolError));

        // Dropping both and pushing one again is a valid list.
        assert_eq!(props.remove(PropertyId::ReceiveMaximum), 2);
        props.push(Property::ReceiveMaximum(30));
        let mut dst = BytesMut::new();
        props.encode(&mut dst, v5()).unwrap();
        assert_eq!(props.get_u16(PropertyId::ReceiveMaximum), Some(30));
    }

    // ---- orderings (a property id is a number, and sorts like one) ----

    /// Identifiers sort by their wire value, so they can key a `BTreeMap` or
    /// be sorted for a canonical order.
    #[test]
    fn test_property_id_orders_by_wire_value() {
        let mut ids = vec![
            PropertyId::UserProperty,
            PropertyId::PayloadFormatIndicator,
            PropertyId::TopicAlias,
        ];
        ids.sort();
        assert_eq!(
            ids,
            vec![
                PropertyId::PayloadFormatIndicator,
                PropertyId::TopicAlias,
                PropertyId::UserProperty,
            ]
        );
        assert!(PropertyId::PayloadFormatIndicator < PropertyId::UserProperty);

        let mut by_id = alloc::collections::BTreeMap::new();
        by_id.insert(PropertyId::TopicAlias, 1u8);
        by_id.insert(PropertyId::PayloadFormatIndicator, 2);
        assert_eq!(
            by_id.keys().copied().collect::<Vec<_>>(),
            vec![PropertyId::PayloadFormatIndicator, PropertyId::TopicAlias]
        );
    }

    /// A value type is hashable and ordered too, for the same reasons.
    #[test]
    fn test_property_type_is_hashable_and_ordered() {
        let mut types = vec![PropertyType::StringPair, PropertyType::Byte];
        types.sort();
        assert_eq!(types, vec![PropertyType::Byte, PropertyType::StringPair]);

        let mut set = alloc::collections::BTreeSet::new();
        assert!(set.insert(PropertyType::U32));
        assert!(!set.insert(PropertyType::U32));
    }

    // ---- per-property typed accessors ----

    /// Every identifier's pair, pushed and read back. A row whose type did
    /// not match its identifier would fail here — or, if the types happened
    /// to agree, in the encode test below.
    #[test]
    fn test_typed_accessors_round_trip() {
        let mut p = Properties::new();

        // Byte: the two that are values rather than flags.
        p.push_payload_format_indicator(1);
        p.push_maximum_qos(1);
        assert_eq!(p.get_payload_format_indicator(), Some(1));
        assert_eq!(p.get_maximum_qos(), Some(1));

        // Byte: the six whose byte is a yes/no.
        p.push_request_problem_information(true);
        p.push_request_response_information(false);
        p.push_retain_available(true);
        p.push_wildcard_subscription_available(false);
        p.push_subscription_identifier_available(true);
        p.push_shared_subscription_available(false);
        assert_eq!(p.get_request_problem_information(), Some(true));
        assert_eq!(p.get_request_response_information(), Some(false));
        assert_eq!(p.get_retain_available(), Some(true));
        assert_eq!(p.get_wildcard_subscription_available(), Some(false));
        assert_eq!(p.get_subscription_identifier_available(), Some(true));
        assert_eq!(p.get_shared_subscription_available(), Some(false));

        // Two-byte integers.
        p.push_server_keep_alive(30);
        p.push_receive_maximum(20);
        p.push_topic_alias_maximum(10);
        p.push_topic_alias(7);
        assert_eq!(p.get_server_keep_alive(), Some(30));
        assert_eq!(p.get_receive_maximum(), Some(20));
        assert_eq!(p.get_topic_alias_maximum(), Some(10));
        assert_eq!(p.get_topic_alias(), Some(7));

        // Four-byte integers.
        p.push_message_expiry_interval(3600);
        p.push_session_expiry_interval(300);
        p.push_will_delay_interval(60);
        p.push_maximum_packet_size(65_536);
        assert_eq!(p.get_message_expiry_interval(), Some(3600));
        assert_eq!(p.get_session_expiry_interval(), Some(300));
        assert_eq!(p.get_will_delay_interval(), Some(60));
        assert_eq!(p.get_maximum_packet_size(), Some(65_536));

        // Variable-byte integer.
        p.push_subscription_identifier(4444u32);
        assert_eq!(p.get_subscription_identifier(), Some(4444));

        // Binary.
        p.push_correlation_data(Bytes::from_static(b"corr"));
        p.push_authentication_data(Bytes::from_static(b"auth"));
        assert_eq!(p.get_correlation_data().as_deref(), Some(&b"corr"[..]));
        assert_eq!(p.get_authentication_data().as_deref(), Some(&b"auth"[..]));

        // Strings.
        p.push_content_type("application/json");
        p.push_response_topic("reply/here");
        p.push_assigned_client_identifier("auto-42");
        p.push_authentication_method("SCRAM-SHA-1");
        p.push_response_information("reply");
        p.push_server_reference("mqtt://other:1883");
        p.push_reason_string("because");
        assert_eq!(p.get_content_type().as_deref(), Some("application/json"));
        assert_eq!(p.get_response_topic().as_deref(), Some("reply/here"));
        assert_eq!(
            p.get_assigned_client_identifier().as_deref(),
            Some("auto-42")
        );
        assert_eq!(
            p.get_authentication_method().as_deref(),
            Some("SCRAM-SHA-1")
        );
        assert_eq!(p.get_response_information().as_deref(), Some("reply"));
        assert_eq!(
            p.get_server_reference().as_deref(),
            Some("mqtt://other:1883")
        );
        assert_eq!(p.get_reason_string().as_deref(), Some("because"));

        // One per identifier, `UserProperty` aside.
        assert_eq!(p.len(), 26);
    }

    /// A property whose value type does not match its identifier fails
    /// [`Property::validate`], which marks the collection invalid and makes
    /// `encode` refuse it. That every one of these encodes is the check that
    /// the table's types are the specification's.
    #[test]
    fn test_typed_accessors_use_the_specified_wire_types() {
        let mut p = Properties::new();
        p.push_payload_format_indicator(1);
        p.push_topic_alias(7);
        p.push_message_expiry_interval(3600);
        p.push_subscription_identifier(4444u32);
        p.push_correlation_data(Bytes::from_static(b"corr"));
        p.push_content_type("application/json");

        let mut dst = BytesMut::new();
        p.encode(&mut dst, MQTT_VERSION_5).unwrap();

        let mut src = dst.freeze();
        let back = Properties::decode(&mut src, MQTT_VERSION_5).unwrap();
        assert_eq!(back, p, "the wire form did not carry the same values");
        assert_eq!(back.get_content_type().as_deref(), Some("application/json"));
        assert_eq!(back.get_subscription_identifier(), Some(4444));
    }

    /// The shorthand is the generic call, nothing more.
    #[test]
    fn test_typed_accessors_match_the_generic_form() {
        let mut short = Properties::new();
        short.push_content_type("text/plain");
        short.push_topic_alias(3);

        let mut generic = Properties::new();
        generic.push(Property::ContentType("text/plain".into()));
        generic.push(Property::TopicAlias(3));

        assert_eq!(short, generic);
        assert_eq!(
            short.get_content_type(),
            generic.get_string(PropertyId::ContentType)
        );
    }

    /// An absent property reads as `None`, not as a default.
    #[test]
    fn test_typed_accessors_absent_is_none() {
        let p = Properties::new();
        assert_eq!(p.get_payload_format_indicator(), None);
        assert_eq!(p.get_topic_alias(), None);
        assert_eq!(p.get_content_type(), None);
        assert_eq!(p.get_correlation_data(), None);
        assert_eq!(p.get_subscription_identifier(), None);
    }

    /// A flag property is a byte on the wire, and the two views agree.
    #[test]
    fn test_flag_accessors_are_a_byte_underneath() {
        let mut p = Properties::new();
        p.push_retain_available(true);
        p.push_shared_subscription_available(false);

        // The flag setter writes what `push_byte` would.
        assert_eq!(p.get_byte(PropertyId::RetainAvailable), Some(1));
        assert_eq!(p.get_byte(PropertyId::SharedSubscriptionAvailable), Some(0));

        // And the flag getter reads what `push_byte` writes.
        let mut raw = Properties::new();
        raw.push(Property::RetainAvailable(1));
        raw.push(Property::SharedSubscriptionAvailable(0));
        assert_eq!(raw.get_retain_available(), Some(true));
        assert_eq!(raw.get_shared_subscription_available(), Some(false));
        assert_eq!(raw, p);

        // `false` is a value, not an absence.
        assert_eq!(
            Properties::new().get_shared_subscription_available(),
            None,
            "an absent flag must not read as false"
        );
    }

    /// A byte outside 0/1 cannot reach a flag property, so the getter's
    /// conversion never has to guess: `push` marks the collection invalid and
    /// `encode` refuses it.
    #[test]
    fn test_flag_property_rejects_other_bytes() {
        let mut p = Properties::new();
        p.push(Property::RetainAvailable(2));
        let mut dst = BytesMut::new();
        assert_eq!(
            p.encode(&mut dst, MQTT_VERSION_5),
            Err(Error::ProtocolError)
        );
    }

    // ---- PropertiesBuilder ----

    /// The builder reaches every property the collection does, and produces
    /// the same collection. Both surfaces come from one table, so this is
    /// the test that the table is complete in both directions.
    #[test]
    fn test_builder_matches_the_push_methods() {
        let built = Properties::builder()
            // Byte values that are not flags.
            .payload_format_indicator(1)
            .maximum_qos(1)
            // Flags.
            .request_problem_information(true)
            .request_response_information(false)
            .retain_available(true)
            .wildcard_subscription_available(false)
            .subscription_identifier_available(true)
            .shared_subscription_available(false)
            // Two-byte integers.
            .server_keep_alive(30)
            .receive_maximum(20)
            .topic_alias_maximum(10)
            .topic_alias(7)
            // Four-byte integers.
            .message_expiry_interval(3600)
            .session_expiry_interval(300)
            .will_delay_interval(60)
            .maximum_packet_size(65_536)
            // Variable-byte integer.
            .subscription_identifier(4444u32)
            // Binary.
            .correlation_data(Bytes::from_static(b"corr"))
            .authentication_data(Bytes::from_static(b"auth"))
            // Strings.
            .content_type("application/json")
            .response_topic("reply/here")
            .assigned_client_identifier("auto-42")
            .authentication_method("SCRAM-SHA-1")
            .response_information("reply")
            .server_reference("mqtt://other:1883")
            .reason_string("because")
            // The two that are not generated.
            .user("trace-id", "abc123")
            .property(Property::PayloadFormatIndicator(1))
            .finalize();

        let mut pushed = Properties::new();
        pushed.push_payload_format_indicator(1);
        pushed.push_maximum_qos(1);
        pushed.push_request_problem_information(true);
        pushed.push_request_response_information(false);
        pushed.push_retain_available(true);
        pushed.push_wildcard_subscription_available(false);
        pushed.push_subscription_identifier_available(true);
        pushed.push_shared_subscription_available(false);
        pushed.push_server_keep_alive(30);
        pushed.push_receive_maximum(20);
        pushed.push_topic_alias_maximum(10);
        pushed.push_topic_alias(7);
        pushed.push_message_expiry_interval(3600);
        pushed.push_session_expiry_interval(300);
        pushed.push_will_delay_interval(60);
        pushed.push_maximum_packet_size(65_536);
        pushed.push_subscription_identifier(4444u32);
        pushed.push_correlation_data(Bytes::from_static(b"corr"));
        pushed.push_authentication_data(Bytes::from_static(b"auth"));
        pushed.push_content_type("application/json");
        pushed.push_response_topic("reply/here");
        pushed.push_assigned_client_identifier("auto-42");
        pushed.push_authentication_method("SCRAM-SHA-1");
        pushed.push_response_information("reply");
        pushed.push_server_reference("mqtt://other:1883");
        pushed.push_reason_string("because");
        pushed.push_user("trace-id", "abc123");
        pushed.push(Property::PayloadFormatIndicator(1));

        assert_eq!(built, pushed);
        assert_eq!(
            built.len(),
            28,
            "26 generated, plus a user property and a raw push"
        );
    }

    /// The empty builder is the empty collection, and `new` agrees with the
    /// shortcut on the collection.
    #[test]
    fn test_builder_empty_and_shortcut() {
        assert_eq!(PropertiesBuilder::new().finalize(), Properties::new());
        assert!(Properties::builder().finalize().is_empty());
        assert_eq!(
            Properties::builder().topic_alias(3).finalize(),
            PropertiesBuilder::new().topic_alias(3).finalize()
        );
    }

    /// A builder converts into the collection it built, and into the
    /// `Option<Properties>` that the acknowledgement constructors take.
    #[test]
    fn test_builder_into_properties() {
        let props: Properties = Properties::builder().reason_string("nope").into();
        assert_eq!(props.get_reason_string().as_deref(), Some("nope"));

        let opt: Option<Properties> = Properties::builder().reason_string("nope").into();
        assert_eq!(opt, Some(props));

        // `None` and a finished collection still convert as they did.
        let none: Option<Properties> = None;
        assert_eq!(none, None);
        let from_finished: Option<Properties> = Properties::new().into();
        assert_eq!(from_finished, Some(Properties::new()));
    }

    /// The constructors that take `Into<Option<Properties>>` now accept a
    /// builder directly, without `finalize`.
    #[test]
    fn test_builder_passes_to_a_constructor_unfinalized() {
        use crate::{PubAck, ReasonCode};

        let ack = PubAck::with_properties(
            1,
            ReasonCode::UnspecifiedError,
            Properties::builder().reason_string("no room"),
        );
        assert_eq!(
            ack.properties.get_reason_string().as_deref(),
            Some("no room")
        );
        assert_eq!(
            ack,
            PubAck::with_properties(
                1,
                ReasonCode::UnspecifiedError,
                Properties::builder().reason_string("no room").finalize(),
            )
        );
    }

    /// The builder validates nothing, as documented: an out-of-range value
    /// is refused where everything else is, on encode.
    #[test]
    fn test_builder_defers_validation_to_encode() {
        let props = Properties::builder().topic_alias(0).finalize();
        let mut dst = BytesMut::new();
        assert_eq!(
            props.encode(&mut dst, MQTT_VERSION_5),
            Err(Error::ProtocolError),
            "a topic alias of 0 is invalid"
        );
    }

    /// A slice builds the same collection the owned `Vec` does, without
    /// giving up the properties.
    #[test]
    fn test_from_property_slice() {
        let owned = vec![
            Property::MessageExpiryInterval(3600),
            Property::ContentType("text/plain".into()),
            Property::UserProperty("k".into(), "v".into()),
        ];

        let from_slice = Properties::from(&owned[..]);
        let from_vec = Properties::from(owned.clone());
        assert_eq!(from_slice, from_vec);

        // The source is still usable, which is the point of the impl.
        assert_eq!(owned.len(), 3);
        assert_eq!(from_slice.len(), 3);
        assert_eq!(from_slice.get_message_expiry_interval(), Some(3600));
        assert_eq!(from_slice.get_content_type().as_deref(), Some("text/plain"));
        assert_eq!(from_slice.get_user("k"), Some("v"));
    }

    /// The caches are built as `push` would build them, so the collection
    /// encodes to what the same properties pushed one at a time encode to.
    #[test]
    fn test_from_property_slice_builds_the_caches() {
        let props = [Property::PayloadFormatIndicator(1), Property::TopicAlias(7)];
        let from_slice = Properties::from(&props[..]);

        let mut pushed = Properties::new();
        pushed.push_payload_format_indicator(1);
        pushed.push_topic_alias(7);

        assert_eq!(from_slice, pushed);
        assert_eq!(
            from_slice.encoded_size(MQTT_VERSION_5),
            pushed.encoded_size(MQTT_VERSION_5)
        );

        let mut a = BytesMut::new();
        let mut b = BytesMut::new();
        from_slice.encode(&mut a, MQTT_VERSION_5).unwrap();
        pushed.encode(&mut b, MQTT_VERSION_5).unwrap();
        assert_eq!(a, b);
    }

    /// An empty slice is an empty collection, not an error.
    #[test]
    fn test_from_empty_property_slice() {
        let none: [Property; 0] = [];
        assert_eq!(Properties::from(&none[..]), Properties::new());
        assert_eq!(Properties::from(none), Properties::new());
    }

    /// An array converts whether it is owned or borrowed, and `into` works
    /// on both — an array reference cannot reach the slice impl through
    /// `into`, which is why the borrowed form exists.
    #[test]
    fn test_from_property_array() {
        let make = || {
            [
                Property::MessageExpiryInterval(3600),
                Property::ContentType("text/plain".into()),
            ]
        };

        // Owned, written in place: no clone, no reference.
        let owned: Properties = make().into();
        assert_eq!(owned.get_message_expiry_interval(), Some(3600));
        assert_eq!(owned.get_content_type().as_deref(), Some("text/plain"));

        // Borrowed, by `into` and by `from`, leaving the array in place.
        let arr = make();
        let by_into: Properties = (&arr).into();
        let by_from = Properties::from(&arr);
        assert_eq!(arr.len(), 2);

        // And all four routes agree, including through the slice.
        let by_slice: Properties = arr[..].into();
        assert_eq!(owned, by_into);
        assert_eq!(owned, by_from);
        assert_eq!(owned, by_slice);
    }

    /// A property the specification refuses still marks the collection
    /// invalid, exactly as pushing it would; the conversion does not filter.
    #[test]
    fn test_from_property_slice_keeps_invalidity() {
        let bad = [Property::TopicAlias(0)];
        let props = Properties::from(&bad[..]);
        let mut dst = BytesMut::new();
        assert_eq!(
            props.encode(&mut dst, MQTT_VERSION_5),
            Err(Error::ProtocolError)
        );
    }

    // ---- Property as an enum: the discriminant, and the wire form ----

    /// One of every variant, so that a test can walk the whole set.
    fn every_property() -> Vec<Property> {
        vec![
            Property::PayloadFormatIndicator(1),
            Property::MessageExpiryInterval(3600),
            Property::ContentType("text/plain".to_string()),
            Property::ResponseTopic("reply/here".to_string()),
            Property::CorrelationData(Bytes::from_static(b"corr")),
            Property::SubscriptionIdentifier(4444),
            Property::SessionExpiryInterval(300),
            Property::AssignedClientIdentifier("auto-42".to_string()),
            Property::ServerKeepAlive(30),
            Property::AuthenticationMethod("SCRAM-SHA-1".to_string()),
            Property::AuthenticationData(Bytes::from_static(b"auth")),
            Property::RequestProblemInformation(1),
            Property::WillDelayInterval(60),
            Property::RequestResponseInformation(0),
            Property::ResponseInformation("reply".to_string()),
            Property::ServerReference("mqtt://other:1883".to_string()),
            Property::ReasonString("because".to_string()),
            Property::ReceiveMaximum(20),
            Property::TopicAliasMaximum(10),
            Property::TopicAlias(7),
            Property::MaximumQoS(1),
            Property::RetainAvailable(1),
            Property::UserProperty("k".to_string(), "v".to_string()),
            Property::MaximumPacketSize(65_536),
            Property::WildcardSubscriptionAvailable(0),
            Property::SubscriptionIdentifierAvailable(1),
            Property::SharedSubscriptionAvailable(0),
        ]
    }

    /// What the identifier would be if it were written out by hand, for every
    /// variant. This is the oracle the discriminant read is checked against.
    fn expected_id(p: &Property) -> PropertyId {
        use Property as P;
        use PropertyId as Id;
        match p {
            P::PayloadFormatIndicator(_) => Id::PayloadFormatIndicator,
            P::MessageExpiryInterval(_) => Id::MessageExpiryInterval,
            P::ContentType(_) => Id::ContentType,
            P::ResponseTopic(_) => Id::ResponseTopic,
            P::CorrelationData(_) => Id::CorrelationData,
            P::SubscriptionIdentifier(_) => Id::SubscriptionIdentifier,
            P::SessionExpiryInterval(_) => Id::SessionExpiryInterval,
            P::AssignedClientIdentifier(_) => Id::AssignedClientIdentifier,
            P::ServerKeepAlive(_) => Id::ServerKeepAlive,
            P::AuthenticationMethod(_) => Id::AuthenticationMethod,
            P::AuthenticationData(_) => Id::AuthenticationData,
            P::RequestProblemInformation(_) => Id::RequestProblemInformation,
            P::WillDelayInterval(_) => Id::WillDelayInterval,
            P::RequestResponseInformation(_) => Id::RequestResponseInformation,
            P::ResponseInformation(_) => Id::ResponseInformation,
            P::ServerReference(_) => Id::ServerReference,
            P::ReasonString(_) => Id::ReasonString,
            P::ReceiveMaximum(_) => Id::ReceiveMaximum,
            P::TopicAliasMaximum(_) => Id::TopicAliasMaximum,
            P::TopicAlias(_) => Id::TopicAlias,
            P::MaximumQoS(_) => Id::MaximumQoS,
            P::RetainAvailable(_) => Id::RetainAvailable,
            P::UserProperty(..) => Id::UserProperty,
            P::MaximumPacketSize(_) => Id::MaximumPacketSize,
            P::WildcardSubscriptionAvailable(_) => Id::WildcardSubscriptionAvailable,
            P::SubscriptionIdentifierAvailable(_) => Id::SubscriptionIdentifierAvailable,
            P::SharedSubscriptionAvailable(_) => Id::SharedSubscriptionAvailable,
        }
    }

    /// The identifier the derive produces is the one an exhaustive match
    /// would name, for every variant — so a discriminant typed wrongly in
    /// the enum, or a variant the derive mapped differently, fails here.
    #[test]
    fn test_property_id_matches_the_variant() {
        let all = every_property();

        // `expected_id` below matches exhaustively, so the compiler already
        // guarantees every *variant* is accounted for there; this count is
        // what guarantees the fixture lists them all. Raise it with the enum.
        assert_eq!(all.len(), 27, "a variant is missing from the fixture");

        for p in &all {
            assert_eq!(p.property_id(), expected_id(p), "{p:?}");
        }
    }

    /// And the tag is the specification's identifier byte, not merely
    /// self-consistent.
    #[test]
    fn test_property_id_is_the_wire_byte() {
        for p in &every_property() {
            let mut dst = BytesMut::new();
            p.encode(&mut dst).unwrap();
            assert_eq!(dst[0], p.property_id() as u8, "{p:?}");
        }
    }

    /// Every variant round-trips through the wire unchanged.
    #[test]
    fn test_round_trip_every_variant() {
        for p in every_property() {
            let mut dst = BytesMut::new();
            p.encode(&mut dst).unwrap();
            assert_eq!(dst.len(), p.encoded_size(), "{p:?}");

            let mut src = dst.freeze();
            let back = Property::decode(&mut src).unwrap();
            assert_eq!(back, p);
            assert!(!src.has_remaining(), "{p:?} left bytes behind");
        }
    }

    /// The value type follows from the identifier, as the specification's
    /// table says, and matches what the shipped `PropertyId` reports.
    #[test]
    fn test_value_type_follows_the_identifier() {
        for p in &every_property() {
            assert_eq!(p.value_type(), p.property_id().value_type(), "{p:?}");
        }
    }

    /// Ranges are still checked, since a type cannot express "0 or 1".
    #[test]
    fn test_validate_checks_ranges() {
        assert_eq!(
            Property::MaximumQoS(2).validate(),
            Err(Error::ProtocolError)
        );
        assert_eq!(
            Property::TopicAlias(0).validate(),
            Err(Error::ProtocolError)
        );
        assert_eq!(
            Property::SubscriptionIdentifier(0).validate(),
            Err(Error::ProtocolError)
        );
        assert_eq!(
            Property::ResponseTopic("a/+/b".to_string()).validate(),
            Err(Error::ProtocolError)
        );
        assert!(Property::MaximumQoS(1).validate().is_ok());
        assert!(Property::TopicAlias(1).validate().is_ok());
    }

    /// A range violation is refused on the way in as well as the way out.
    #[test]
    fn test_decode_rejects_a_bad_range() {
        let mut src = Bytes::from_static(&[0x24, 0x02]); // MaximumQoS = 2
        assert_eq!(Property::decode(&mut src), Err(Error::ProtocolError));
    }

    /// `from_value` pairs an identifier with a loose value, and refuses a
    /// pairing the specification does not define.
    #[test]
    fn test_from_value_pairs_or_refuses() {
        // Every variant survives the round trip through the loose form.
        for p in every_property() {
            let id = p.property_id();
            assert_eq!(Property::from_value(id, p.value()), Ok(p.clone()), "{p:?}");
        }

        // A value of the wrong type for the identifier is refused, where
        // naming the variant would not have compiled.
        assert_eq!(
            Property::from_value(PropertyId::MaximumQoS, PropertyValue::U32(1)),
            Err(Error::ProtocolError)
        );
        assert_eq!(
            Property::from_value(
                PropertyId::ContentType,
                PropertyValue::Binary(Bytes::from_static(b"x"))
            ),
            Err(Error::ProtocolError)
        );
        assert_eq!(
            Property::from_value(
                PropertyId::UserProperty,
                PropertyValue::String("no pair".into())
            ),
            Err(Error::ProtocolError)
        );
    }

    /// The range rules still apply to a property built this way, once it is
    /// pushed or encoded.
    #[test]
    fn test_from_value_still_range_checks() {
        let bad = Property::from_value(PropertyId::TopicAlias, PropertyValue::U16(0)).unwrap();
        assert_eq!(bad.validate(), Err(Error::ProtocolError));
    }

    // ---- borrowing reads ----

    /// `get_str` reads every single-string property, and nothing else.
    #[test]
    fn test_get_str_reads_string_properties() {
        let mut p = Properties::new();
        p.push(Property::ContentType("application/json".into()));
        p.push(Property::ResponseTopic("reply/here".into()));
        p.push(Property::AssignedClientIdentifier("auto-42".into()));
        p.push(Property::AuthenticationMethod("SCRAM-SHA-1".into()));
        p.push(Property::ResponseInformation("reply".into()));
        p.push(Property::ServerReference("mqtt://other:1883".into()));
        p.push(Property::ReasonString("because".into()));

        assert_eq!(p.get_str(PropertyId::ContentType), Some("application/json"));
        assert_eq!(p.get_str(PropertyId::ResponseTopic), Some("reply/here"));
        assert_eq!(
            p.get_str(PropertyId::AssignedClientIdentifier),
            Some("auto-42")
        );
        assert_eq!(
            p.get_str(PropertyId::AuthenticationMethod),
            Some("SCRAM-SHA-1")
        );
        assert_eq!(p.get_str(PropertyId::ResponseInformation), Some("reply"));
        assert_eq!(
            p.get_str(PropertyId::ServerReference),
            Some("mqtt://other:1883")
        );
        assert_eq!(p.get_str(PropertyId::ReasonString), Some("because"));

        // It agrees with the owning form, without the allocation.
        assert_eq!(
            p.get_str(PropertyId::ContentType).map(String::from),
            p.get_string(PropertyId::ContentType)
        );
    }

    /// Absent, and present-but-not-a-string, both read `None`.
    #[test]
    fn test_get_str_on_other_properties() {
        let mut p = Properties::new();
        p.push(Property::TopicAlias(7));
        p.push(Property::CorrelationData(Bytes::from_static(b"corr")));
        p.push_user("k", "v");

        assert_eq!(p.get_str(PropertyId::ContentType), None, "absent");
        assert_eq!(p.get_str(PropertyId::TopicAlias), None, "not a string");
        assert_eq!(p.get_str(PropertyId::CorrelationData), None, "binary");
        // A user property is a pair, not a single string.
        assert_eq!(p.get_str(PropertyId::UserProperty), None);
        assert_eq!(p.get_user("k"), Some("v"), "which is what get_user is for");
    }

    /// `get_bytes` reads the two binary properties, and nothing else.
    #[test]
    fn test_get_bytes_reads_binary_properties() {
        let mut p = Properties::new();
        p.push(Property::CorrelationData(Bytes::from_static(b"corr")));
        p.push(Property::AuthenticationData(Bytes::from_static(b"auth")));
        p.push(Property::ContentType("text/plain".into()));

        assert_eq!(p.get_bytes(PropertyId::CorrelationData), Some(&b"corr"[..]));
        assert_eq!(
            p.get_bytes(PropertyId::AuthenticationData),
            Some(&b"auth"[..])
        );
        assert_eq!(p.get_bytes(PropertyId::ContentType), None, "not binary");
        assert_eq!(p.get_bytes(PropertyId::ResponseTopic), None, "absent");

        assert_eq!(
            p.get_bytes(PropertyId::CorrelationData)
                .map(Bytes::copy_from_slice),
            p.get_binary(PropertyId::CorrelationData)
        );
    }

    // ---- try_push / is_valid ----

    /// A good property goes in and the caches move with it.
    #[test]
    fn test_try_push_accepts_a_valid_property() {
        let mut p = Properties::new();
        assert!(
            p.try_push(Property::ContentType("text/plain".into()))
                .is_ok()
        );
        assert!(p.try_push(Property::TopicAlias(7)).is_ok());
        assert_eq!(p.len(), 2);
        assert!(p.is_valid());

        // Identical to the same two pushed the infallible way.
        let mut pushed = Properties::new();
        pushed.push(Property::ContentType("text/plain".into()));
        pushed.push(Property::TopicAlias(7));
        assert_eq!(p, pushed);
        assert_eq!(
            p.encoded_size(MQTT_VERSION_5),
            pushed.encoded_size(MQTT_VERSION_5)
        );
    }

    /// An out-of-range value is refused, and refusing changes nothing: the
    /// collection still holds what it held and still encodes.
    #[test]
    fn test_try_push_rejects_a_bad_value_without_changing_anything() {
        let mut p = Properties::new();
        p.try_push(Property::TopicAlias(7)).unwrap();
        let before = p.clone();

        assert_eq!(
            p.try_push(Property::MaximumQoS(2)),
            Err(Error::ProtocolError),
            "a QoS above 1 is out of range"
        );
        assert_eq!(p, before, "the failed push left a trace");
        assert!(p.is_valid());

        let mut dst = BytesMut::new();
        p.encode(&mut dst, MQTT_VERSION_5).unwrap();
    }

    /// A second non-repeatable identifier is refused the same way.
    #[test]
    fn test_try_push_rejects_a_repeat() {
        let mut p = Properties::new();
        p.try_push(Property::ContentType("text/plain".into()))
            .unwrap();
        assert_eq!(
            p.try_push(Property::ContentType("application/json".into())),
            Err(Error::ProtocolError)
        );
        assert_eq!(p.len(), 1);
        assert_eq!(p.get_str(PropertyId::ContentType), Some("text/plain"));
        assert!(p.is_valid());
    }

    /// The two identifiers that may repeat still may.
    #[test]
    fn test_try_push_allows_the_repeatable_identifiers() {
        let mut p = Properties::new();
        p.try_push(Property::UserProperty("a".into(), "1".into()))
            .unwrap();
        p.try_push(Property::UserProperty("b".into(), "2".into()))
            .unwrap();
        p.try_push(Property::SubscriptionIdentifier(1)).unwrap();
        p.try_push(Property::SubscriptionIdentifier(2)).unwrap();
        assert_eq!(p.len(), 4);
        assert!(p.is_valid());
    }

    /// `is_valid` reports what `push` recorded silently, and tracks removal.
    #[test]
    fn test_is_valid_follows_push_and_removal() {
        let mut p = Properties::new();
        assert!(p.is_valid(), "an empty list is valid");

        p.push(Property::ContentType("text/plain".into()));
        assert!(p.is_valid());

        // `push` takes it and marks the list instead of reporting.
        p.push(Property::TopicAlias(0));
        assert!(!p.is_valid());
        let mut dst = BytesMut::new();
        assert_eq!(
            p.encode(&mut dst, MQTT_VERSION_5),
            Err(Error::ProtocolError),
            "is_valid() should predict what encode does"
        );

        // Taking the offender out makes it encodable again.
        p.remove(PropertyId::TopicAlias);
        assert!(p.is_valid());
        let mut dst = BytesMut::new();
        p.encode(&mut dst, MQTT_VERSION_5).unwrap();
    }

    /// Validity is about the properties themselves, not the packet carrying
    /// them: a list can be valid and still be refused by its context.
    #[test]
    fn test_is_valid_says_nothing_about_context() {
        let mut p = Properties::new();
        p.try_push(Property::TopicAlias(7)).unwrap();
        assert!(p.is_valid());
        // A topic alias is a PUBLISH property, not a SUBSCRIBE one.
        assert_eq!(
            p.check_context(PacketType::Subscribe),
            Err(Error::ProtocolError)
        );
    }

    // ---- contains / count ----

    /// Both agree with the accessors they are shorthand for.
    #[test]
    fn test_contains_and_count_agree_with_the_accessors() {
        let mut p = Properties::new();
        p.push(Property::ContentType("text/plain".into()));
        p.push(Property::TopicAlias(7));

        for id in [
            PropertyId::ContentType,
            PropertyId::TopicAlias,
            PropertyId::ReasonString,
        ] {
            assert_eq!(p.contains(id), p.get(id).is_some(), "{id:?}");
            assert_eq!(p.count(id), p.get_all(id).count(), "{id:?}");
        }
        assert_eq!(p.count(PropertyId::ContentType), 1);
        assert_eq!(p.count(PropertyId::ReasonString), 0);
        assert!(!p.contains(PropertyId::ReasonString));
    }

    /// The repeatable identifiers are the reason `count` exists: `len` is the
    /// whole list, and `get_at` needs to know how far it may index.
    #[test]
    fn test_count_counts_repeats() {
        let mut p = Properties::new();
        p.push_user("a", "1");
        p.push_user("b", "2");
        p.push_user("c", "3");
        p.push(Property::SubscriptionIdentifier(1));
        p.push(Property::SubscriptionIdentifier(2));

        assert_eq!(p.count(PropertyId::UserProperty), 3);
        assert_eq!(p.count(PropertyId::SubscriptionIdentifier), 2);
        assert_eq!(p.len(), 5);

        // Every index below the count resolves, and the next one does not.
        for idx in 0..p.count(PropertyId::UserProperty) {
            assert!(p.get_at(PropertyId::UserProperty, idx).is_some(), "{idx}");
        }
        assert!(p.get_at(PropertyId::UserProperty, 3).is_none());
    }

    /// `contains` must not consult the seen-identifier set, which records
    /// only the identifiers that may not repeat — it would deny a user
    /// property that is plainly present.
    #[test]
    fn test_contains_sees_repeatable_identifiers() {
        let mut p = Properties::new();
        p.push_user("k", "v");
        p.push(Property::SubscriptionIdentifier(4444));

        assert!(p.contains(PropertyId::UserProperty));
        assert!(p.contains(PropertyId::SubscriptionIdentifier));
    }

    /// Both follow removal.
    #[test]
    fn test_contains_and_count_follow_removal() {
        let mut p = Properties::new();
        p.push_user("a", "1");
        p.push_user("b", "2");
        p.push(Property::ContentType("text/plain".into()));

        assert_eq!(p.remove_user("a"), 1);
        assert_eq!(p.count(PropertyId::UserProperty), 1);

        assert_eq!(p.remove(PropertyId::ContentType), 1);
        assert!(!p.contains(PropertyId::ContentType));
        assert_eq!(p.count(PropertyId::ContentType), 0);

        p.clear();
        assert!(!p.contains(PropertyId::UserProperty));
        assert_eq!(p.count(PropertyId::UserProperty), 0);
    }

    /// `contains_user` keys off the key, agrees with `get_user`, and counts
    /// a repeated key once.
    #[test]
    fn test_contains_user() {
        let mut p = three_users();
        assert!(p.contains_user("a"));
        assert!(p.contains_user("b"));
        assert!(!p.contains_user("nope"));

        for key in ["a", "b", "nope"] {
            assert_eq!(p.contains_user(key), p.get_user(key).is_some(), "{key}");
        }

        // A value is not a key.
        assert_eq!(p.get_user("a"), Some("1"));
        assert!(!p.contains_user("1"));

        // The fixture already repeats "a"; one more changes nothing here.
        assert_eq!(p.count(PropertyId::UserProperty), 3);
        p.push_user("a", "again");
        assert!(p.contains_user("a"));
        assert_eq!(p.count(PropertyId::UserProperty), 4);

        // And it follows removal: every "a" goes, "b" stays.
        assert_eq!(p.remove_user("a"), 3);
        assert!(!p.contains_user("a"));
        assert!(p.contains_user("b"));
    }

    /// An empty key is a legal user property, and is found like any other.
    #[test]
    fn test_contains_user_with_an_empty_key() {
        let mut p = Properties::new();
        assert!(!p.contains_user(""));
        p.push_user("", "value");
        assert!(p.contains_user(""));
        assert_eq!(p.get_user(""), Some("value"));
    }

    /// Only user properties are considered, not some other string property
    /// that happens to hold the same text.
    #[test]
    fn test_contains_user_ignores_other_properties() {
        let mut p = Properties::new();
        p.push(Property::ContentType("text/plain".into()));
        p.push(Property::ReasonString("because".into()));
        assert!(!p.contains_user("text/plain"));
        assert!(!p.contains_user("because"));
        assert_eq!(p.count(PropertyId::UserProperty), 0);
    }
}
