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
    Error, MqttVersion, Result, String, Vec, varlen,
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

/////////////////////////////////////////////////////////////////////////////

/// MQTT v5 properties: an ordered list of [`Property`] pairs.
///
/// The same property identifier may appear more than once (required for
/// `UserProperty`; allowed for `SubscriptionIdentifier`). Use [`Properties::get`]
/// for the first occurrence and [`Properties::iter`] when you need all.
///
/// On v3.1.1 connections, properties are always empty and are never written
/// to the wire.
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
    pub fn push(&mut self, prop: Property) {
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
    /// Returns 0 for v3.1.1 connections — properties are not present on the wire.
    pub fn encoded_size(&self, ver: MqttVersion) -> usize {
        if ver < MqttVersion::V5 {
            0
        }
        else {
            let data_len = self.payload_size();
            varlen::encoded_size(data_len as u32) + data_len
        }
    }

    /// Encodes the properties to the wire: VBI(payload_len) followed by each property.
    ///
    /// No bytes are written for v3.1.1 connections.
    pub fn encode<B: BufMut>(&self, dst: &mut B, ver: MqttVersion) -> Result<()> {
        if ver >= MqttVersion::V5 {
            varlen::encode(self.payload_size() as u32, dst)?;
            for prop in &self.props {
                prop.encode(dst)?;
            }
        }
        Ok(())
    }

    /// Decodes the properties from the wire: reads the VBI length prefix, then
    /// parses each property from exactly that many bytes.
    ///
    /// Returns an empty collection for v3.1.1 connections without consuming any bytes.
    pub fn decode<B: Buf>(src: &mut B, ver: MqttVersion) -> Result<Self> {
        if ver < MqttVersion::V5 {
            return Ok(Self::new());
        }

        let len = varlen::decode(src)?.ok_or(Error::PacketTooShort)? as usize;
        if src.remaining() < len {
            return Err(Error::PacketTooShort);
        }
        let mut payload = src.copy_to_bytes(len);
        let mut props = Vec::new();
        while payload.has_remaining() {
            props.push(Property::decode(&mut payload)?);
        }
        // Size from the decoded values, not the wire length, so the cache
        // is right by construction even if an encoding on the wire was
        // longer than canonical.
        Ok(Self::from(props))
    }
}

impl From<Vec<Property>> for Properties {
    fn from(props: Vec<Property>) -> Self {
        let payload_size = props.iter().map(Property::encoded_size).sum();
        Self {
            props,
            payload_size,
        }
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
}
