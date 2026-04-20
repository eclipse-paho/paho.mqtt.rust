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
//! On v3.1.1 connections properties are always empty and are never written to
//! or read from the wire.

use crate::{
    error::{Error, Result},
    varlen,
    wire::{get_binary, get_utf8, put_utf8},
};
use bytes::{Buf, BufMut, Bytes};

// ---- PropertyType -------------------------------------------------------

/// The wire encoding type of an MQTT v5 property value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

// ---- PropertyId ---------------------------------------------------------

/// MQTT v5 property identifiers.
///
/// Each identifier has a fixed associated wire type (see [`PropertyId::value_type`]).
/// The numeric discriminants match the values on the wire.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub enum PropertyId {
    PayloadFormatIndicator = 0x01,
    MessageExpiryInterval = 0x02,
    ContentType = 0x03,
    ResponseTopic = 0x08,
    CorrelationData = 0x09,
    SubscriptionIdentifier = 0x0B,
    SessionExpiryInterval = 0x11,
    AssignedClientIdentifier = 0x12,
    ServerKeepAlive = 0x13,
    AuthenticationMethod = 0x15,
    AuthenticationData = 0x16,
    RequestProblemInformation = 0x17,
    WillDelayInterval = 0x18,
    RequestResponseInformation = 0x19,
    ResponseInformation = 0x1A,
    ServerReference = 0x1C,
    ReasonString = 0x1F,
    ReceiveMaximum = 0x21,
    TopicAliasMaximum = 0x22,
    TopicAlias = 0x23,
    MaximumQoS = 0x24,
    RetainAvailable = 0x25,
    UserProperty = 0x26,
    MaximumPacketSize = 0x27,
    WildcardSubscriptionAvailable = 0x28,
    SubscriptionIdentifierAvailable = 0x29,
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

// ---- PropertyValue ------------------------------------------------------

/// The typed value of an MQTT v5 property.
#[derive(Debug, Clone, PartialEq, Eq)]
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
            PropertyValue::Binary(b) => {
                dst.put_u16(b.len() as u16);
                dst.put_slice(b);
            }
            PropertyValue::String(s) => put_utf8(s, dst),
            PropertyValue::StringPair(k, v) => {
                put_utf8(k, dst);
                put_utf8(v, dst);
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

// ---- Property -----------------------------------------------------------

/// A single MQTT v5 property: an identifier paired with its typed value.
#[derive(Debug, Clone, PartialEq, Eq)]
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
        Ok(Property { id, value })
    }
}

// ---- Properties ---------------------------------------------------------

/// MQTT v5 properties: an ordered list of [`Property`] pairs.
///
/// The same property identifier may appear more than once (required for
/// `UserProperty`; allowed for `SubscriptionIdentifier`). Use [`Properties::get`]
/// for the first occurrence and [`Properties::iter`] when you need all.
///
/// On v3.1.1 connections, properties are always empty and are never written
/// to the wire.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Properties(Vec<Property>);

impl Properties {
    /// Creates an empty property list.
    pub fn new() -> Self {
        Properties(Vec::new())
    }

    /// Returns `true` if there are no properties.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Appends `prop` to the end of the list.
    pub fn push(&mut self, prop: Property) {
        self.0.push(prop);
    }

    /// Returns the value of the first property with the given `id`, or `None`.
    pub fn get(&self, id: PropertyId) -> Option<&PropertyValue> {
        self.0.iter().find(|p| p.id == id).map(|p| &p.value)
    }

    /// Iterates over all properties in order.
    pub fn iter(&self) -> impl Iterator<Item = &Property> {
        self.0.iter()
    }

    /// Total bytes occupied by all properties on the wire (excluding the VBI prefix).
    fn payload_size(&self) -> usize {
        self.0.iter().map(|p| p.encoded_size()).sum()
    }

    /// Number of bytes this encodes to on the wire, including the VBI length prefix.
    pub fn encoded_size(&self) -> usize {
        let data_len = self.payload_size();
        varlen::encoded_size(data_len as u32) + data_len
    }

    /// Encodes: VBI(payload_len) followed by each property.
    pub fn encode<B: BufMut>(&self, dst: &mut B) -> Result<()> {
        varlen::encode(self.payload_size() as u32, dst)?;
        for prop in &self.0 {
            prop.encode(dst)?;
        }
        Ok(())
    }

    /// Decodes: reads the VBI length prefix, then parses each property from
    /// exactly that many bytes.
    pub fn decode<B: Buf>(src: &mut B) -> Result<Self> {
        let len = varlen::decode(src)?.ok_or(Error::PacketTooShort)? as usize;
        if src.remaining() < len {
            return Err(Error::PacketTooShort);
        }
        let mut payload = src.copy_to_bytes(len);
        let mut props = Vec::new();
        while payload.has_remaining() {
            props.push(Property::decode(&mut payload)?);
        }
        Ok(Properties(props))
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    fn round_trip(props: &Properties) {
        let mut dst = BytesMut::new();
        props.encode(&mut dst).unwrap();
        assert_eq!(dst.len(), props.encoded_size());
        let decoded = Properties::decode(&mut dst.freeze()).unwrap();
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
        assert_eq!(props.encoded_size(), 1); // just the 0x00 VBI
    }

    #[test]
    fn test_encode_empty() {
        let mut dst = BytesMut::new();
        Properties::new().encode(&mut dst).unwrap();
        assert_eq!(&dst[..], &[0x00]);
    }

    #[test]
    fn test_decode_empty() {
        let src = Bytes::from_static(&[0x00]);
        let props = Properties::decode(&mut src.clone()).unwrap();
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
        props.encode(&mut dst).unwrap();
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
        props.encode(&mut dst).unwrap();
        assert_eq!(dst.len(), props.encoded_size());
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
        props.encode(&mut dst).unwrap();
        assert_eq!(dst.len(), props.encoded_size());
    }

    // ---- error cases ----

    #[test]
    fn test_decode_too_short_returns_err() {
        // length prefix says 5 bytes but only 2 follow
        let src = Bytes::from_static(&[0x05, 0x01, 0x02]);
        assert_eq!(
            Properties::decode(&mut src.clone()),
            Err(Error::PacketTooShort)
        );
    }

    #[test]
    fn test_decode_unknown_property_id_returns_err() {
        // length=1, id=0xFF (unknown)
        let src = Bytes::from_static(&[0x01, 0xFF]);
        assert_eq!(
            Properties::decode(&mut src.clone()),
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
}
