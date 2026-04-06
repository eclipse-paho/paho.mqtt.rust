// properties.rs
//
// The set of properties in an MQTT v5 packet.
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2019-2025 Frank Pagliughi <fpagliughi@mindspring.com>
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
//! This is the collection of metadata that can be added to a packet.
//!

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    convert::TryFrom,
    ffi::CString,
    fmt, mem,
    os::raw::{c_char, c_int},
    ptr,
    slice::{Iter, IterMut},
};

use crate::{Error, Result, ffi};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize, Serializer, ser::SerializeMap};

/// Error code for property mismatches
const INVALID_PROPERTY_ID: i32 = ffi::MQTT_INVALID_PROPERTY_ID;

/// The type for properties that take binary data.
pub type Binary = Vec<u8>;

/// The Property `value` union type.
pub type Value = ffi::MQTTProperty__bindgen_ty_1;

/// The struct to encapsulate property string values.
type LenString = ffi::MQTTLenString;

/// The underlying data type for a specific property
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum PropertyType {
    /// A property containing a single byte
    Byte = 0,
    /// A property containing a 16-bit unsigned integer, `u16`.
    TwoByteInteger = 1,
    /// A property containing a 32-bit unsigned integer, `u32`.
    FourByteInteger = 2,
    /// A property containing a variable-byte value.
    VariableByteInteger = 3,
    /// A property containing a binary blob, like `Vec<u8>`
    BinaryData = 4,
    /// A property containing a `String`
    Utf8EncodedString = 5,
    /// A property containing a pair of strings `(String,String)`
    Utf8StringPair = 6,
}

// Local alias for the C property type
type Type = ffi::MQTTPropertyTypes;

impl PropertyType {
    /// Tries to create a property type from a C integer value
    pub fn new(typ: ffi::MQTTPropertyTypes) -> Option<Self> {
        Self::try_from(typ).ok()
    }

    /// Gets the any::TypeId that corresponds to the property type.
    pub fn type_of(&self) -> TypeId {
        use PropertyType::*;
        match *self {
            Byte => TypeId::of::<u8>(),
            TwoByteInteger => TypeId::of::<u16>(),
            FourByteInteger => TypeId::of::<u32>(),
            VariableByteInteger => TypeId::of::<u32>(),
            BinaryData => TypeId::of::<Binary>(),
            Utf8EncodedString => TypeId::of::<String>(),
            Utf8StringPair => TypeId::of::<(String, String)>(),
        }
    }
}

impl TryFrom<ffi::MQTTPropertyTypes> for PropertyType {
    type Error = crate::Error;

    /// Try to convert from an integer property type to and enumeration
    /// value.
    fn try_from(typ: ffi::MQTTPropertyTypes) -> Result<Self> {
        use PropertyType::*;
        match typ {
            0 => Ok(Byte),
            1 => Ok(TwoByteInteger),
            2 => Ok(FourByteInteger),
            3 => Ok(VariableByteInteger),
            4 => Ok(BinaryData),
            5 => Ok(Utf8EncodedString),
            6 => Ok(Utf8StringPair),
            _ => Err(crate::Error::Conversion),
        }
    }
}

impl fmt::Display for PropertyType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// The enumerated codes for the MQTT v5 properties.
///
/// The property code defines both the meaning of the value in the property
/// (Correlation Data, Server Keep Alive) and the data type held by the
/// property. The underlying numeric value is what gets serialized on the
/// wire as the key for each value.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[allow(missing_docs)]
pub enum PropertyCode {
    /// \[Byte, u8\] Indicates whether the payload is unspecified, possibly
    /// binary (0) or UTF-8 text (1).
    PayloadFormatIndicator = 1,
    /// [FouByteInteger, u32] The lifetime of the application message in seconds.
    /// The broker is required to delete expired messages that have not been
    /// passed to a subscriber.
    MessageExpiryInterval = 2,
    /// \[String\] Description of the payload. Often the MIME type describing
    /// the serialization.
    ContentType = 3,
    /// \[String\] The topic name for a response message, such as for the
    /// RPC pattern.
    ResponseTopic = 8,
    /// \[Binary\] Used by the sender of a request message to identify the response.
    /// Typically used with the RPC pattern, or just to identify related messages.
    CorrelationData = 9,
    /// \[VariableByteInteger\] The identifier of the subscription.
    SubscriptionIdentifier = 11,
    /// \[FourByteInteger, u32\] The number of seconds after which the network
    /// connection is closed that the server should keep a persistent session.
    SessionExpiryInterval = 17,
    /// \[String\] The ClientID assigned by the server when it receives a
    /// zero-length ClientID in the CONNECT message.
    AssignedClientIdentifier = 18,
    /// \[TwoByteInteger, u16\] The Keep Alive duration that the server requires.
    ServerKeepAlive = 19,
    /// \[String\] The name of the authentication method.
    AuthenticationMethod = 21,
    /// \[Binary\] The authentication data
    AuthenticationData = 22,
    /// \[Byte,  u8\] The Client uses this value to indicate whether the Reason
    /// String or User Properties are sent in the case of failures.
    RequestProblemInformation = 23,
    /// \[Four Byte Integer, u32\] The number of seconds after which a network
    /// connection is lost before the server should publish the LWT message.
    WillDelayInterval = 24,
    /// \[Byte, u8\] Request that the server return Response Information in the CONNACK
    RequestResponseInformation = 25,
    /// \[String\] A hint for the client to create a response topic.
    ResponseInformation = 26,
    /// \[String\] Can be used by the client to identify another server to use.
    ServerReference = 28,
    /// \[String\] A reason associated with the response.
    ReasonString = 31,
    /// \[TwoByteInteger, u16\] The maximum number of QoS 1 and QoS 2
    /// publications that it is willing to process concurrently. If absent,
    /// defaults to 65,535.
    ReceiveMaximum = 33,
    /// \[TwoByteInteger, u16\] The highest value that the Client will accept
    /// as a Topic Alias sent by the Server. The Client uses this value to
    /// limit the number of Topic Aliases that it is willing to hold on this
    /// connection.
    TopicAliasMaximum = 34,
    /// \[TwoByteInteger, u16\] An integer value that is used to identify the
    /// topic instead of using the Topic Name. This reduces the size of the
    /// PUBLISH packet.
    TopicAlias = 35,
    /// \[Byte\] The maximum QoS supported by the server.
    MaximumQos = 36,
    /// \[Byte\] Whether retain messages are supported (1) or not (0).
    RetainAvailable = 37,
    /// \[StringPair\] A user defined property as a key/value string pair.
    UserProperty = 38,
    /// \[FourByteInteger, u32\] The maximum packet size that the client is
    /// willing to accept.
    MaximumPacketSize = 39,
    /// \[Byte, u8\] Whether the server supports wildcard subscriptions (1)
    /// or not (0).
    WildcardSubscriptionAvailable = 40,
    /// \[Byte, u8\] Whether the server supports subscription identifiers (1)
    /// or not (0).
    SubscriptionIdentifiersAvailable = 41,
    /// \[Byte, u8\] Whether the server supports shared subscriptions (1)
    /// or not (0).
    SharedSubscriptionAvailable = 42,
}

// Local alias for the C property code integer type
type Code = ffi::MQTTPropertyCodes;

impl PropertyCode {
    /// Tries to create a `PropertyCode` from the integer value.
    pub fn new(code: ffi::MQTTPropertyCodes) -> Option<Self> {
        Self::try_from(code as u8).ok()
    }

    /// Get the property type from the code identifier.
    pub fn property_type(&self) -> PropertyType {
        let typ = unsafe { ffi::MQTTProperty_getType(*self as Code) as Type };
        PropertyType::new(typ).unwrap()
    }

    /// Gets the any::TypeId that corresponds to the property type.
    pub fn type_of(&self) -> TypeId {
        self.property_type().type_of()
    }
}

impl TryFrom<u8> for PropertyCode {
    type Error = crate::Error;

    /// Try to convert from an integer property type to and enumeration
    /// value.
    fn try_from(code: u8) -> Result<Self> {
        use PropertyCode::*;
        match code {
            1 => Ok(PayloadFormatIndicator),
            2 => Ok(MessageExpiryInterval),
            3 => Ok(ContentType),
            8 => Ok(ResponseTopic),
            9 => Ok(CorrelationData),
            11 => Ok(SubscriptionIdentifier),
            17 => Ok(SessionExpiryInterval),
            18 => Ok(AssignedClientIdentifier),
            19 => Ok(ServerKeepAlive),
            21 => Ok(AuthenticationMethod),
            22 => Ok(AuthenticationData),
            23 => Ok(RequestProblemInformation),
            24 => Ok(WillDelayInterval),
            25 => Ok(RequestResponseInformation),
            26 => Ok(ResponseInformation),
            28 => Ok(ServerReference),
            31 => Ok(ReasonString),
            33 => Ok(ReceiveMaximum),
            34 => Ok(TopicAliasMaximum),
            35 => Ok(TopicAlias),
            36 => Ok(MaximumQos),
            37 => Ok(RetainAvailable),
            38 => Ok(UserProperty),
            39 => Ok(MaximumPacketSize),
            40 => Ok(WildcardSubscriptionAvailable),
            41 => Ok(SubscriptionIdentifiersAvailable),
            42 => Ok(SharedSubscriptionAvailable),
            _ => Err(crate::Error::Conversion),
        }
    }
}

impl fmt::Display for PropertyCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Property
/////////////////////////////////////////////////////////////////////////////

/// Type for a variable byte integer.
pub type VbiU32 = u32;

/// A single MQTT v5 property.
///
/// An MQTT v5 property consists of both a property "code"  and a value. The
/// code indicates what the property contains (Response Topic, Will Delay
/// Interval, etc), and also the data type for the value. Each copde
/// corresponds to a single, specific data type as described in the v5
/// spec, here:
/// <https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901027>
///
/// There are only a limited number of data types that are possible for
/// properties:
///   "Byte"  - `u8`
///   "Two Byte Integer"  - `u16`
///   "Four Byte Integer"  - `u32`
///   "Binary Data"  - `Vec<u8>`
///   "UTF-8 Encoded String"  - `String`
///   "UTF-8 String Pair"  - `(String,String)`
///
#[derive(Debug, Clone)]
#[repr(u8)]
#[allow(missing_docs)]
pub enum Property {
    PayloadFormatIndicator(u8) = 1,
    MessageExpiraryInterval(u32) = 2,
    ContentType(String) = 3,
    ResponseTopic(String) = 8,
    CorrelationData(Vec<u8>) = 9,
    SubscriptionIdentifier(VbiU32) = 11,
    SessionExpiryInterval(u32) = 17,
    AssignedClientIdentifier(String) = 18,
    ServerKeepAlive(u16) = 19,
    AuthenticationMethod(String) = 21,
    AuthenticationData(Vec<u8>) = 22,
    RequestProblemInformation(u8) = 23,
    WillDelayInterval(u32) = 24,
    RequestResponseInformation(u8) = 25,
    ResponseInformation(String) = 26,
    ServerReference(String) = 28,
    ReasonString(String) = 31,
    ReceiveMaximum(u16) = 33,
    TopicAliasMaximum(u16) = 34,
    TopicAlias(u16) = 35,
    MaximumQos(u8) = 36,
    RetainAvailable(u8) = 37,
    User((String, String)) = 38,
    MaximumPacketSize(u32) = 39,
    WildcardSubscriptionAvailable(u8) = 40,
    SubscriptionIdentifiersAvailable(u8) = 41,
    SharedSubscriptionAvailable(u8) = 42,
}

impl Property {
    /// Creates a new property for a given code and value.
    ///
    /// The type for the value must match the type expected for the given
    /// property code exactly, otherwise it will be rejected and return None.
    pub fn new<T>(code: PropertyCode, val: T) -> Option<Self>
    where
        T: Any + 'static,
    {
        use PropertyCode::*;
        let rval: &(dyn Any + 'static) = &val;

        match code {
            PayloadFormatIndicator => rval
                .downcast_ref::<u8>()
                .copied()
                .map(Self::PayloadFormatIndicator),
            MessageExpiryInterval => rval
                .downcast_ref::<u32>()
                .copied()
                .map(Self::MessageExpiraryInterval),
            ContentType => rval
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| rval.downcast_ref::<String>().map(String::clone))
                .map(Self::ContentType),
            ResponseTopic => rval
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| rval.downcast_ref::<String>().map(String::clone))
                .map(Self::ResponseTopic),
            CorrelationData => rval
                .downcast_ref::<&[u8]>()
                .map(|v| (*v).to_vec())
                .or_else(|| rval.downcast_ref::<Vec<u8>>().map(Vec::clone))
                .map(Self::CorrelationData),
            SubscriptionIdentifier => rval
                .downcast_ref::<u32>()
                .copied()
                .map(Self::SubscriptionIdentifier),
            SessionExpiryInterval => rval
                .downcast_ref::<u32>()
                .copied()
                .map(Self::SessionExpiryInterval),
            AssignedClientIdentifier => rval
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| rval.downcast_ref::<String>().map(|s| s.into()))
                .map(Self::AssignedClientIdentifier),
            ServerKeepAlive => rval
                .downcast_ref::<u16>()
                .copied()
                .map(Self::ServerKeepAlive),
            AuthenticationMethod => rval
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| rval.downcast_ref::<String>().map(String::clone))
                .map(Self::AuthenticationMethod),
            AuthenticationData => rval
                .downcast_ref::<&[u8]>()
                .map(|v| (*v).to_vec())
                .or_else(|| rval.downcast_ref::<Vec<u8>>().map(Vec::clone))
                .map(Self::AuthenticationData),
            RequestProblemInformation => rval
                .downcast_ref::<u8>()
                .copied()
                .map(Self::RequestProblemInformation),
            WillDelayInterval => rval
                .downcast_ref::<u32>()
                .copied()
                .map(Self::WillDelayInterval),
            RequestResponseInformation => rval
                .downcast_ref::<u8>()
                .copied()
                .map(Self::RequestResponseInformation),
            ResponseInformation => rval
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| rval.downcast_ref::<String>().map(String::clone))
                .map(Self::ResponseInformation),
            ServerReference => rval
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| rval.downcast_ref::<String>().map(String::clone))
                .map(Self::ServerReference),
            ReasonString => rval
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| rval.downcast_ref::<String>().map(String::clone))
                .map(Self::ReasonString),
            ReceiveMaximum => rval
                .downcast_ref::<u16>()
                .copied()
                .map(Self::ReceiveMaximum),
            TopicAliasMaximum => rval
                .downcast_ref::<u16>()
                .copied()
                .map(Self::TopicAliasMaximum),
            TopicAlias => rval.downcast_ref::<u16>().copied().map(Self::TopicAlias),
            MaximumQos => rval.downcast_ref::<u8>().copied().map(Self::MaximumQos),
            RetainAvailable => rval
                .downcast_ref::<u8>()
                .copied()
                .map(Self::RetainAvailable),
            UserProperty => rval
                .downcast_ref::<(&str, &str)>()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .or_else(|| {
                    rval.downcast_ref::<(&str, String)>()
                        .map(|(k, v)| ((*k).to_string(), v.clone()))
                })
                .or_else(|| {
                    rval.downcast_ref::<(String, &str)>()
                        .map(|(k, v)| (k.clone(), (*v).to_string()))
                })
                .or_else(|| {
                    rval.downcast_ref::<(String, String)>()
                        .map(|(k, v)| (k.clone(), v.clone()))
                })
                .map(Self::User),
            MaximumPacketSize => rval
                .downcast_ref::<u32>()
                .copied()
                .map(Self::MaximumPacketSize),
            WildcardSubscriptionAvailable => rval
                .downcast_ref::<u8>()
                .copied()
                .map(Self::WildcardSubscriptionAvailable),
            SubscriptionIdentifiersAvailable => rval
                .downcast_ref::<u8>()
                .copied()
                .map(Self::SubscriptionIdentifiersAvailable),
            SharedSubscriptionAvailable => rval
                .downcast_ref::<u8>()
                .copied()
                .map(Self::SharedSubscriptionAvailable),
        }
    }

    /// Gets the property code as the raw byte value.
    fn property_code_raw(&self) -> u8 {
        unsafe { *(self as *const Self as *const u8) }
    }

    /// Gets the property code corresponding to the property.
    pub fn property_code(&self) -> PropertyCode {
        PropertyCode::try_from(self.property_code_raw()).unwrap()
    }

    /// Get the property type from the code identifier.
    pub fn property_type(&self) -> PropertyType {
        self.property_code().property_type()
    }

    /// Gets the any::TypeId that corresponds to the property type.
    pub fn type_of(&self) -> TypeId {
        self.property_code().property_type().type_of()
    }

    /// Gets the property value as a a u8 byte.
    pub fn get_byte(&self) -> Option<&u8> {
        use Property::*;
        match *self {
            PayloadFormatIndicator(ref by) => Some(by),
            RequestProblemInformation(ref by) => Some(by),
            RequestResponseInformation(ref by) => Some(by),
            MaximumQos(ref by) => Some(by),
            RetainAvailable(ref by) => Some(by),
            WildcardSubscriptionAvailable(ref by) => Some(by),
            SubscriptionIdentifiersAvailable(ref by) => Some(by),
            SharedSubscriptionAvailable(ref by) => Some(by),
            _ => None,
        }
    }

    /// Gets the property value as a 16-bit uint.
    pub fn get_u16(&self) -> Option<&u16> {
        use Property::*;
        match *self {
            ServerKeepAlive(ref val) => Some(val),
            ReceiveMaximum(ref val) => Some(val),
            TopicAliasMaximum(ref val) => Some(val),
            TopicAlias(ref val) => Some(val),
            _ => None,
        }
    }

    /// Gets the property value as a 32-bit uint.
    pub fn get_u32(&self) -> Option<&u32> {
        use Property::*;
        match *self {
            MessageExpiraryInterval(ref val) => Some(val),
            SubscriptionIdentifier(ref val) => Some(val),
            SessionExpiryInterval(ref val) => Some(val),
            WillDelayInterval(ref val) => Some(val),
            MaximumPacketSize(ref val) => Some(val),
            _ => None,
        }
    }

    /// Gets the property value as an unsigned integer.
    /// This extracts an integer value from the property. It works with
    /// any of the integer types, either one, two, or four bytes.
    /// Returns None if the Property contains something other than an
    /// integer.
    pub fn get_uint(&self) -> Option<u32> {
        use Property::*;
        match *self {
            PayloadFormatIndicator(val) => Some(val as u32),
            MessageExpiraryInterval(val) => Some(val),
            SubscriptionIdentifier(val) => Some(val),
            SessionExpiryInterval(val) => Some(val),
            ServerKeepAlive(val) => Some(val as u32),
            RequestProblemInformation(val) => Some(val as u32),
            WillDelayInterval(val) => Some(val),
            RequestResponseInformation(val) => Some(val as u32),
            ReceiveMaximum(val) => Some(val as u32),
            TopicAliasMaximum(val) => Some(val as u32),
            TopicAlias(val) => Some(val as u32),
            MaximumQos(val) => Some(val as u32),
            RetainAvailable(val) => Some(val as u32),
            MaximumPacketSize(val) => Some(val),
            WildcardSubscriptionAvailable(val) => Some(val as u32),
            SubscriptionIdentifiersAvailable(val) => Some(val as u32),
            SharedSubscriptionAvailable(val) => Some(val as u32),
            _ => None,
        }
    }

    /// Gets the property value as a string reference.
    pub fn get_string(&self) -> Option<&str> {
        use Property::*;
        match self {
            ContentType(s) => Some(s.as_str()),
            ResponseTopic(s) => Some(s.as_str()),
            AssignedClientIdentifier(s) => Some(s.as_str()),
            AuthenticationMethod(s) => Some(s.as_str()),
            ResponseInformation(s) => Some(s.as_str()),
            ServerReference(s) => Some(s.as_str()),
            ReasonString(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Converts the property value into a string.
    pub fn into_string(self) -> Option<String> {
        use Property::*;
        match self {
            ContentType(s) => Some(s),
            ResponseTopic(s) => Some(s),
            AssignedClientIdentifier(s) => Some(s),
            AuthenticationMethod(s) => Some(s),
            ResponseInformation(s) => Some(s),
            ServerReference(s) => Some(s),
            ReasonString(s) => Some(s),
            _ => None,
        }
    }

    /// Gets the property value as a binary blob.
    pub fn get_binary(&self) -> Option<&[u8]> {
        use Property::*;
        match self {
            CorrelationData(val) => Some(val.as_slice()),
            AuthenticationData(val) => Some(val.as_slice()),
            _ => None,
        }
    }

    /// Gets the property value as a binary blob.
    pub fn into_binary(self) -> Option<Binary> {
        use Property::*;
        match self {
            CorrelationData(val) => Some(val),
            AuthenticationData(val) => Some(val),
            _ => None,
        }
    }

    /// Gets the property value as a string pair.
    pub fn get_string_pair(&self) -> Option<&(String, String)> {
        use Property::*;
        match self {
            User(val) => Some(val),
            _ => None,
        }
    }

    /// Converts the property value into a string pair.
    pub fn into_string_pair(self) -> Option<(String, String)> {
        use Property::*;
        match self {
            User(val) => Some(val),
            _ => None,
        }
    }
}

macro_rules! property_value {
    ( $props:expr, $variant:path ) => {
        $props
            .iter()
            .filter_map(|val| match *val {
                $variant(ref data) => Some(data),
                _ => None,
            })
            .next()
    };
}

macro_rules! property_values {
    ( $props:expr, $variant:path ) => {
        $props.iter().filter_map(|val| match *val {
            $variant(ref data) => Some(data),
            _ => None,
        })
    };
}

/*
impl TryFrom<&ffi::MQTTProperty> for Property {
    type Error = Error;

    fn try_from(cprop: &ffi::MQTTProperty) -> Result<Self> {
        // TODO: Implement this
        Ok(Self::default())
    }
}
*/

/////////////////////////////////////////////////////////////////////////////
//                              Properties
/////////////////////////////////////////////////////////////////////////////

/// A collection of property values.
#[derive(Debug, Default, Clone)]
pub struct Properties(Vec<Property>);

impl Properties {
    /// Creates a new, empty collection of properties.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new collection with a single property.
    pub fn with_property(prop: Property) -> Self {
        Self::from(vec![prop])
    }

    /// Converts the property collection into a vector of properties.
    pub fn into_vec(self) -> Vec<Property> {
        self.0
    }

    /// Gets the property collection as a slice of properties.
    pub fn as_slice(&self) -> &[Property] {
        self.0.as_slice()
    }

    /// Determines if the collection of properties is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Gets the number of properties in the collection.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Inserts a property into the collection.
    pub fn insert(&mut self, idx: usize, prop: Property) {
        self.0.insert(idx, prop);
    }

    /// Pushes a property onto the end of the collection
    pub fn push(&mut self, prop: Property) {
        self.0.push(prop);
    }

    /// Adds a property to the collection given the property code and value.
    pub fn push_val<T>(&mut self, code: PropertyCode, val: T) -> Result<()>
    where
        T: Any + 'static,
    {
        self.push(Property::new(code, val).ok_or(Error::BadProperty)?);
        Ok(())
    }

    /// Searches for a property with the specified code
    pub fn get(&self, code: PropertyCode) -> Option<&Property> {
        self.0.iter().find(|&p| p.property_code() == code)
    }

    /// Gets the payload format indicator, if any
    pub fn payload_format_indicator(&self) -> Option<&u8> {
        property_value!(self, Property::PayloadFormatIndicator)
    }

    /// Gets the byte value for the requested property code, if any
    pub fn get_byte(&self, code: PropertyCode) -> Option<&u8> {
        self.get(code).and_then(|p| p.get_byte())
    }

    /// Gets the byte value for the requested property code, if any
    pub fn get_u16(&self, code: PropertyCode) -> Option<&u16> {
        self.get(code).and_then(|p| p.get_u16())
    }

    /// Gets the byte value for the requested property code, if any
    pub fn get_u32(&self, code: PropertyCode) -> Option<&u32> {
        self.get(code).and_then(|p| p.get_u32())
    }

    /// Gets the byte value for the requested property code, if any
    pub fn get_uint(&self, code: PropertyCode) -> Option<u32> {
        self.get(code).and_then(|p| p.get_uint())
    }

    /// Gets the string value for the requested property code, if any
    pub fn get_string(&self, code: PropertyCode) -> Option<&str> {
        self.get(code).and_then(|p| p.get_string())
    }

    /// Gets the binary value for the requested property code, if any
    pub fn get_binary(&self, code: PropertyCode) -> Option<&[u8]> {
        self.get(code).and_then(|p| p.get_binary())
    }

    /// Gets the string value for the requested property code, if any
    pub fn get_string_pair(&self, code: PropertyCode) -> Option<&(String, String)> {
        self.get(code).and_then(|p| p.get_string_pair())
    }

    /// Find the user value for the requested user key, if any
    pub fn find_user_property(&self, key: &str) -> Option<&str> {
        property_values!(self.0, Property::User)
            .filter_map(|(k, v)| if k == key { Some(v.as_str()) } else { None })
            .next()
    }

    /// Extract the user values into a vector of string tuples.
    pub fn user_vec(&self) -> Vec<(&str, &str)> {
        property_values!(self.0, Property::User)
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect()
    }

    /// Extract the user values into a map of string kay/value pairs.
    pub fn user_values(&self) -> HashMap<&str, &str> {
        property_values!(self.0, Property::User)
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect()
    }

    /// Create an iterator through the properties.
    pub fn iter(&self) -> Iter<'_, Property> {
        self.0.iter()
    }

    /// Create a mutable iterator through the properties.
    pub fn iter_mut(&mut self) -> IterMut<'_, Property> {
        self.0.iter_mut()
    }
}

impl From<Vec<Property>> for Properties {
    fn from(v: Vec<Property>) -> Self {
        Self(v)
    }
}

impl From<&[Property]> for Properties {
    fn from(props: &[Property]) -> Self {
        Self(props.to_vec())
    }
}

impl From<&ffi::MQTTProperties> for Properties {
    fn from(cprops: &ffi::MQTTProperties) -> Self {
        use PropertyType::*;

        let mut props = Self::new();
        let ffi_props = PropertiesFfi::from(cprops);

        for prop in ffi_props.iter() {
            let code = prop.property_code();

            match prop.property_type() {
                Byte => {
                    let val = prop.get_byte().unwrap();
                    props.push(Property::new(code, val).unwrap());
                }
                TwoByteInteger => {
                    let val = prop.get_u16().unwrap();
                    props.push(Property::new(code, val).unwrap());
                }
                FourByteInteger | VariableByteInteger => {
                    let val = prop.get_u32().unwrap();
                    props.push(Property::new(code, val).unwrap());
                }
                BinaryData => {
                    let val = prop.get_binary().unwrap();
                    props.push(Property::new(code, val).unwrap());
                }
                Utf8EncodedString => {
                    let val = prop.get_string().unwrap();
                    props.push(Property::new(code, val).unwrap());
                }
                Utf8StringPair => {
                    let (key, val) = prop.get_string_pair().unwrap();
                    props.push(Property::new(code, (key, val)).unwrap());
                }
            }
        }
        props
    }
}

/////////////////////////////////////////////////////////////////////////////
// FFI

/// A single MQTT v5 property.
///
/// An MQTT v5 property consists of both a property "code"  and a value. The
/// code indicates what the property contains (Response Topic, Will Delay
/// Interval, etc), and also the data type for the value. Each copde
/// corresponds to a single, specific data type as described in the v5
/// spec, here:
/// <https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901027>
///
/// There are only a limited number of data types that are possible for
/// properties:
///   "Byte"  - `u8`
///   "Two Byte Integer"  - `u16`
///   "Four Byte Integer"  - `u32`
///   "Binary Data"  - `Vec<u8>`
///   "UTF-8 Encoded String"  - `String`
///   "UTF-8 String Pair"  - `(String,String)`
///
#[derive(Debug)]
pub struct PropertyFfi {
    pub(crate) cprop: ffi::MQTTProperty,
}

impl PropertyFfi {
    /// Creates a new property for a given code and value.
    ///
    /// The type for the value must match the type expected for the given
    /// property code exactly, otherwise it will be rejected and return None.
    pub fn new<T>(code: PropertyCode, val: T) -> Result<Self>
    where
        T: Any + 'static,
    {
        let rval: &(dyn Any + 'static) = &val;

        // Try some manual mappings first

        if code.type_of() == TypeId::of::<Binary>() {
            // A binary type can accept strings
            if let Some(v) = rval.downcast_ref::<&str>() {
                return Self::new_binary(code, v.as_bytes());
            }
            else if let Some(v) = rval.downcast_ref::<String>() {
                return Self::new_binary(code, v.as_bytes());
            }
        }

        // Note that we could potentially insist that the types must
        // match exactly, but this seems too restrictive:
        //
        // if code.type_of() != TypeId::of::<T>() {
        //     return Err(INVALID_PROPERTY_ID.into());
        // }

        if let Some(v) = rval.downcast_ref::<u8>() {
            Self::new_byte(code, *v)
        }
        else if let Some(v) = rval.downcast_ref::<u16>() {
            Self::new_u16(code, *v)
        }
        else if let Some(v) = rval.downcast_ref::<i16>() {
            Self::new_u16(code, *v as u16)
        }
        else if let Some(v) = rval.downcast_ref::<u32>() {
            Self::new_u32(code, *v)
        }
        else if let Some(v) = rval.downcast_ref::<i32>() {
            Self::new_int(code, *v)
        }
        else if let Some(v) = rval.downcast_ref::<Binary>() {
            Self::new_binary(code, v.clone())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8]>() {
            Self::new_binary(code, *v)
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 1]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 2]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 3]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 4]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 5]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 6]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 7]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 8]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 9]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 10]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 11]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 12]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 13]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 14]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 15]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 16]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 17]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 18]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 19]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 20]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 21]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 22]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 23]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 24]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 25]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 26]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 27]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 28]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 29]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 30]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 31]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 32]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<String>() {
            Self::new_string(code, v)
        }
        else if let Some(v) = rval.downcast_ref::<&str>() {
            Self::new_string(code, v)
        }
        else if let Some(v) = rval.downcast_ref::<(String, String)>() {
            Self::new_string_pair(code, &v.0, &v.1)
        }
        else if let Some(v) = rval.downcast_ref::<(&str, &str)>() {
            Self::new_string_pair(code, v.0, v.1)
        }
        else if let Some(v) = rval.downcast_ref::<(&str, String)>() {
            Self::new_string_pair(code, v.0, &v.1)
        }
        else if let Some(v) = rval.downcast_ref::<(String, &str)>() {
            Self::new_string_pair(code, &v.0, v.1)
        }
        else {
            Err(INVALID_PROPERTY_ID.into())
        }
    }

    /// Creates a single-byte property
    pub fn new_byte(code: PropertyCode, val: u8) -> Result<Self> {
        match code.property_type() {
            PropertyType::Byte => Self::new_int(code, val as i32),
            _ => Err(INVALID_PROPERTY_ID.into()),
        }
    }

    /// Creates a 2-byte integer property
    pub fn new_u16(code: PropertyCode, val: u16) -> Result<Self> {
        match code.property_type() {
            PropertyType::TwoByteInteger => Self::new_int(code, val as i32),
            _ => Err(INVALID_PROPERTY_ID.into()),
        }
    }

    /// Creates a 4-byte integer property
    pub fn new_u32(code: PropertyCode, val: u32) -> Result<Self> {
        match code.property_type() {
            PropertyType::FourByteInteger => Self::new_int(code, val as i32),
            _ => Err(INVALID_PROPERTY_ID.into()),
        }
    }

    /// Creates a new integer property.
    ///
    /// This works for any sized integer type, from byte on up.
    pub fn new_int(code: PropertyCode, val: i32) -> Result<Self> {
        let value = match code.property_type() {
            PropertyType::Byte => {
                if val & !0xFF != 0 {
                    bail!(INVALID_PROPERTY_ID);
                }
                Value { byte: val as u8 }
            }
            PropertyType::TwoByteInteger => {
                if val & !0xFFFF != 0 {
                    return Err(INVALID_PROPERTY_ID.into());
                }
                Value {
                    integer2: val as u16,
                }
            }
            PropertyType::FourByteInteger | PropertyType::VariableByteInteger => Value {
                integer4: val as u32,
            },
            _ => return Err(INVALID_PROPERTY_ID.into()),
        };

        Ok(Self {
            cprop: ffi::MQTTProperty {
                identifier: code as Code,
                value,
            },
        })
    }

    /// Creates a new binary property.
    pub fn new_binary<V>(code: PropertyCode, bin: V) -> Result<Self>
    where
        V: Into<Binary>,
    {
        if code.property_type() != PropertyType::BinaryData {
            return Err(INVALID_PROPERTY_ID.into());
        }

        let mut v = bin.into();
        v.shrink_to_fit();

        let n = v.len();
        let p = v.as_mut_ptr() as *mut c_char;
        mem::forget(v);

        Ok(Self::new_string_binary(code, p, n, ptr::null_mut(), 0))
    }

    /// Creates a new string property.
    pub fn new_string(code: PropertyCode, s: &str) -> Result<Self> {
        if code.property_type() != PropertyType::Utf8EncodedString {
            return Err(INVALID_PROPERTY_ID.into());
        }

        let n = s.len();
        let p = CString::new(s).unwrap().into_raw();

        Ok(Self::new_string_binary(code, p, n, ptr::null_mut(), 0))
    }

    /// Creates a new string pair property.
    pub fn new_string_pair(code: PropertyCode, key: &str, val: &str) -> Result<Self> {
        if code.property_type() != PropertyType::Utf8StringPair {
            return Err(INVALID_PROPERTY_ID.into());
        }

        let nkey = key.len();
        let pkey = CString::new(key).unwrap().into_raw();

        let nval = val.len();
        let pval = CString::new(val).unwrap().into_raw();

        Ok(Self::new_string_binary(code, pkey, nkey, pval, nval))
    }

    /// Creates a property from a C lib MQTTProperty struct.
    fn from_c_property(cprop: &ffi::MQTTProperty) -> Result<Self> {
        use PropertyType::*;

        let mut cprop = *cprop;
        let typ = match PropertyCode::new(cprop.identifier).map(|c| c.property_type()) {
            Some(typ) => typ,
            None => return Err(INVALID_PROPERTY_ID.into()),
        };

        unsafe {
            let mut pdata = cprop.value.__bindgen_anon_1.data.data;
            let n = cprop.value.__bindgen_anon_1.data.len as usize;

            match typ {
                BinaryData => {
                    if pdata.is_null() {
                        return Err(INVALID_PROPERTY_ID.into());
                    }
                    let v = Vec::from_raw_parts(pdata, n, n);
                    let mut vc = v.clone();
                    pdata = vc.as_mut_ptr() as *mut c_char;
                    mem::forget(v);
                    mem::forget(vc);
                }
                Utf8EncodedString => {
                    if pdata.is_null() {
                        return Err(INVALID_PROPERTY_ID.into());
                    }
                    let v = Vec::from_raw_parts(pdata as *mut u8, n, n);
                    let sr = CString::new(v.clone());
                    if sr.is_err() {
                        return Err(INVALID_PROPERTY_ID.into());
                    }
                    pdata = sr.unwrap().into_raw();
                    mem::forget(v);
                }
                Utf8StringPair => {
                    let pvalue = cprop.value.__bindgen_anon_1.value.data;
                    if pdata.is_null() || pvalue.is_null() {
                        return Err(INVALID_PROPERTY_ID.into());
                    }

                    let v = Vec::from_raw_parts(pdata as *mut u8, n, n);
                    let sr = CString::new(v.clone());
                    if sr.is_err() {
                        return Err(INVALID_PROPERTY_ID.into());
                    }
                    pdata = sr.unwrap().into_raw();
                    mem::forget(v);

                    let n = cprop.value.__bindgen_anon_1.value.len as usize;
                    let v = Vec::from_raw_parts(pvalue as *mut u8, n, n);
                    let sr = CString::new(v.clone());
                    if sr.is_err() {
                        return Err(INVALID_PROPERTY_ID.into());
                    }
                    cprop.value.__bindgen_anon_1.value.data = sr.unwrap().into_raw();
                    mem::forget(v);
                }
                _ => (),
            }

            // Lengths are the same as the originals
            cprop.value.__bindgen_anon_1.data.data = pdata;
        }
        Ok(Self { cprop })
    }

    /// Creates a new string, string pair, or binary property given the raw
    /// pointers and sizes.
    /// This is a low-level, internal call to create a preperty that contains
    /// dynamic data. It does no error checking; it simply assembles the
    /// struct.
    fn new_string_binary(
        code: PropertyCode,
        pdata: *mut c_char,
        ndata: usize,
        pval: *mut c_char,
        nval: usize,
    ) -> Self {
        Self {
            cprop: ffi::MQTTProperty {
                identifier: code as Code,
                value: Value {
                    __bindgen_anon_1: ffi::MQTTProperty__bindgen_ty_1__bindgen_ty_1 {
                        data: LenString {
                            len: ndata as c_int,
                            data: pdata,
                        },
                        value: LenString {
                            len: nval as c_int,
                            data: pval,
                        },
                    },
                },
            },
        }
    }

    /// Gets the MQTT code for the property.
    pub fn property_code(&self) -> PropertyCode {
        PropertyCode::new(self.cprop.identifier).unwrap()
    }

    /// Gets the type of this property.
    pub fn property_type(&self) -> PropertyType {
        self.property_code().property_type()
    }

    /// Gets the any::TypeId of this property.
    pub fn type_of(&self) -> TypeId {
        self.property_type().type_of()
    }

    /// Gets the property value
    pub fn get<T>(&self) -> Option<T>
    where
        T: Any + 'static + Send + Default,
    {
        let mut v = T::default();
        let x: &mut dyn Any = &mut v;

        if let Some(val) = x.downcast_mut::<u8>() {
            if let Some(n) = self.get_byte() {
                *val = n;
                return Some(v);
            }
        }
        else if let Some(val) = x.downcast_mut::<u16>() {
            if let Some(n) = self.get_u16() {
                *val = n;
                return Some(v);
            }
        }
        else if let Some(val) = x.downcast_mut::<u32>() {
            if let Some(n) = self.get_u32() {
                *val = n;
                return Some(v);
            }
        }
        else if let Some(val) = x.downcast_mut::<i32>() {
            if let Some(n) = self.get_int() {
                *val = n;
                return Some(v);
            }
        }
        else if let Some(val) = x.downcast_mut::<Binary>() {
            if let Some(n) = self.get_binary() {
                *val = n;
                return Some(v);
            }
        }
        else if let Some(val) = x.downcast_mut::<String>() {
            if let Some(n) = self.get_string() {
                *val = n;
                return Some(v);
            }
        }
        else if let Some(val) = x.downcast_mut::<(String, String)>() {
            if let Some(n) = self.get_string_pair() {
                *val = n;
                return Some(v);
            }
        }
        None
    }

    /// Gets the property value as a byte.
    pub fn get_byte(&self) -> Option<u8> {
        match self.property_type() {
            PropertyType::Byte => Some(unsafe { self.cprop.value.byte }),
            _ => None,
        }
    }

    /// Gets the property value as a u16.
    pub fn get_u16(&self) -> Option<u16> {
        match self.property_type() {
            PropertyType::TwoByteInteger => Some(unsafe { self.cprop.value.integer2 }),
            _ => None,
        }
    }

    /// Gets the property value as a u32.
    pub fn get_u32(&self) -> Option<u32> {
        match self.property_type() {
            PropertyType::FourByteInteger => Some(unsafe { self.cprop.value.integer4 }),
            _ => None,
        }
    }

    /// Gets the property value as an integer.
    /// This extracts an integer value from the property. It works with any
    /// of the int types, one, two, or four bytes.
    /// If the Property contains an integer type it will be returned as
    /// Some(val), otherwise it will return None.
    pub fn get_int(&self) -> Option<i32> {
        unsafe {
            match self.property_type() {
                PropertyType::Byte => Some(self.cprop.value.byte as i32),
                PropertyType::TwoByteInteger => Some(self.cprop.value.integer2 as i32),
                PropertyType::FourByteInteger | PropertyType::VariableByteInteger => {
                    Some(self.cprop.value.integer4 as i32)
                }
                _ => None,
            }
        }
    }

    /// Gets the property value as a binary blob.
    pub fn get_binary(&self) -> Option<Binary> {
        unsafe {
            if self.property_type() == PropertyType::BinaryData {
                let n = self.cprop.value.__bindgen_anon_1.data.len as usize;
                let p = self.cprop.value.__bindgen_anon_1.data.data as *mut u8;
                let v = Vec::from_raw_parts(p, n, n);
                let vc = v.clone();
                mem::forget(v);
                Some(vc)
            }
            else {
                None
            }
        }
    }

    /// Gets the property value as a string.
    pub fn get_string(&self) -> Option<String> {
        unsafe {
            if self.property_type() == PropertyType::Utf8EncodedString {
                let s = CString::from_raw(self.cprop.value.__bindgen_anon_1.data.data);
                let sc = s.clone();
                let _ = s.into_raw();
                sc.into_string().ok()
            }
            else {
                None
            }
        }
    }

    /// Gets the property value as a string pair.
    pub fn get_string_pair(&self) -> Option<(String, String)> {
        unsafe {
            if self.property_type() == PropertyType::Utf8StringPair {
                let s = CString::from_raw(self.cprop.value.__bindgen_anon_1.data.data);
                let sc = s.clone();
                let _ = s.into_raw();
                let keyopt = sc.into_string().ok();

                let s = CString::from_raw(self.cprop.value.__bindgen_anon_1.value.data);
                let sc = s.clone();
                let _ = s.into_raw();
                let valopt = sc.into_string().ok();
                keyopt.and_then(|key| valopt.map(|val| (key, val)))
            }
            else {
                None
            }
        }
    }
}

impl Drop for PropertyFfi {
    /// Drops the property.
    /// For string any binary types, the heap memory will be freed.
    fn drop(&mut self) {
        unsafe {
            match self.property_type() {
                PropertyType::BinaryData => {
                    trace!(
                        "Dropping binary property: {:?}",
                        self.cprop.value.__bindgen_anon_1.data.data
                    );
                    let n = self.cprop.value.__bindgen_anon_1.data.len as usize;
                    let _ = Vec::from_raw_parts(
                        self.cprop.value.__bindgen_anon_1.data.data,
                        n,
                        n,
                    );
                }
                PropertyType::Utf8EncodedString => {
                    trace!(
                        "Dropping string property: {:?}",
                        self.cprop.value.__bindgen_anon_1.data.data
                    );
                    let _ = CString::from_raw(self.cprop.value.__bindgen_anon_1.data.data);
                }
                PropertyType::Utf8StringPair => {
                    trace!(
                        "Dropping string pair property: {:?}, {:?}",
                        self.cprop.value.__bindgen_anon_1.data.data,
                        self.cprop.value.__bindgen_anon_1.value.data
                    );
                    let _ = CString::from_raw(self.cprop.value.__bindgen_anon_1.data.data);
                    let _ = CString::from_raw(self.cprop.value.__bindgen_anon_1.value.data);
                }
                _ => (),
            }
        }
    }
}

impl Clone for PropertyFfi {
    /// Creates a clone of the property.
    /// For string any binary properties, this also clones the heap memory
    /// so that each property is managing separate allocations.
    fn clone(&self) -> Self {
        let mut cprop = self.cprop;

        unsafe {
            match self.property_type() {
                PropertyType::BinaryData => {
                    // TODO: Can we just do a low-level mem copy?
                    let n = cprop.value.__bindgen_anon_1.data.len as usize;
                    let v =
                        Vec::from_raw_parts(cprop.value.__bindgen_anon_1.data.data, n, n);
                    let mut vc = v.clone();
                    let p = vc.as_mut_ptr() as *mut c_char;
                    cprop.value.__bindgen_anon_1.data.data = p;
                    mem::forget(v);
                    mem::forget(vc);
                }
                PropertyType::Utf8EncodedString => {
                    let s = CString::from_raw(cprop.value.__bindgen_anon_1.data.data);
                    let sc = s.clone();
                    let _ = s.into_raw();
                    cprop.value.__bindgen_anon_1.data.data = sc.into_raw();
                }
                PropertyType::Utf8StringPair => {
                    let s = CString::from_raw(cprop.value.__bindgen_anon_1.data.data);
                    let sc = s.clone();
                    cprop.value.__bindgen_anon_1.data.data = sc.into_raw();
                    let _ = s.into_raw();

                    let s = CString::from_raw(cprop.value.__bindgen_anon_1.value.data);
                    let sc = s.clone();
                    cprop.value.__bindgen_anon_1.value.data = sc.into_raw();
                    let _ = s.into_raw();
                }
                _ => (),
            }
        }
        Self { cprop }
    }
}

/////////////////////////////////////////////////////////////////////////////
// Properties

/// A collection of MQTT v5 properties.
///
/// This is a collection of properties that can be added to outgoing packets
/// or retrieved from incoming packets.
#[derive(Debug, Default)]
pub struct PropertiesFfi {
    pub(crate) cprops: ffi::MQTTProperties,
}

impl PropertiesFfi {
    /// Creates a new, empty collection of properties.
    pub fn new() -> Self {
        Self::default()
    }

    /// Determines if the property list has no items in it.
    pub fn is_empty(&self) -> bool {
        self.cprops.count == 0
    }

    /// Gets the number of property items in the collection.
    pub fn len(&self) -> usize {
        self.cprops.count as usize
    }

    /// Gets the number of bytes required for the serialized list on
    /// the wire.
    pub fn byte_len(&self) -> usize {
        let p = &self.cprops as *const ffi::MQTTProperties;
        unsafe { ffi::MQTTProperties_len(p) as usize }
    }

    /// Removes all the items from the property list.
    pub fn clear(&mut self) {
        unsafe { ffi::MQTTProperties_free(&mut self.cprops) };
        self.cprops = ffi::MQTTProperties::default();
    }

    /// Adds a property to the collection.
    pub fn push(&mut self, prop: PropertyFfi) -> Result<()> {
        match unsafe { ffi::MQTTProperties_add(&mut self.cprops, &prop.cprop) } {
            0 => Ok(()),
            rc => Err(rc.into()),
        }
    }

    /// Adds a property to the collection given the property code and value.
    pub fn push_val<T>(&mut self, code: PropertyCode, val: T) -> Result<()>
    where
        T: Any + 'static,
    {
        self.push(PropertyFfi::new(code, val)?)
    }

    /// Adds an single-byte property to the collection.
    pub fn push_byte(&mut self, code: PropertyCode, val: u8) -> Result<()> {
        self.push(PropertyFfi::new_byte(code, val)?)
    }

    /// Adds an two-byte integer property to the collection.
    pub fn push_u16(&mut self, code: PropertyCode, val: u16) -> Result<()> {
        self.push(PropertyFfi::new_u16(code, val)?)
    }

    /// Adds a four-byte integer property to the collection.
    pub fn push_u32(&mut self, code: PropertyCode, val: u32) -> Result<()> {
        self.push(PropertyFfi::new_u32(code, val)?)
    }

    /// Adds an integer property to the collection.
    ///
    /// This works for any integer type.
    pub fn push_int(&mut self, code: PropertyCode, val: i32) -> Result<()> {
        self.push(PropertyFfi::new_int(code, val)?)
    }

    /// Adds a binary property to the collection
    pub fn push_binary<V>(&mut self, code: PropertyCode, bin: V) -> Result<()>
    where
        V: Into<Binary>,
    {
        self.push(PropertyFfi::new_binary(code, bin)?)
    }

    /// Adds a string property to the collection
    pub fn push_string(&mut self, code: PropertyCode, s: &str) -> Result<()> {
        self.push(PropertyFfi::new_string(code, s)?)
    }

    /// Adds a string pair property to the collection
    pub fn push_string_pair(
        &mut self,
        code: PropertyCode,
        key: &str,
        val: &str,
    ) -> Result<()> {
        self.push(PropertyFfi::new_string_pair(code, key, val)?)
    }

    /// Gets a property instance
    pub fn get(&self, code: PropertyCode) -> Option<PropertyFfi> {
        self.get_at(code, 0)
    }

    /// Gets a property instance at the specified index in the collection.
    pub fn get_idx(&self, idx: usize) -> Option<PropertyFfi> {
        let ps = &self.cprops as *const _ as *mut ffi::MQTTProperties;
        unsafe {
            if idx < (*ps).count as usize {
                let p = (*ps).array.add(idx);
                PropertyFfi::from_c_property(&*p).ok()
            }
            else {
                None
            }
        }
    }

    /// Gets a property instance when there are possibly multiple values.
    pub fn get_at(&self, code: PropertyCode, idx: usize) -> Option<PropertyFfi> {
        let ps = &self.cprops as *const _ as *mut ffi::MQTTProperties;
        unsafe {
            let p = ffi::MQTTProperties_getPropertyAt(ps, code as Code, idx as c_int);
            if !p.is_null() {
                PropertyFfi::from_c_property(&*p).ok()
            }
            else {
                None
            }
        }
    }

    /// Gets an iterator for all the properties
    pub fn iter(&self) -> PropertyIterator<'_> {
        PropertyIterator {
            props: self,
            code: None,
            idx: 0,
        }
    }

    /// Gets an iterator for a property instance
    pub fn code_iter(&self, code: PropertyCode) -> PropertyIterator<'_> {
        PropertyIterator {
            props: self,
            code: Some(code),
            idx: 0,
        }
    }

    /// Gets a value from the collection when there may be more than one
    /// for the code.
    pub fn get_val_at<T>(&self, code: PropertyCode, idx: usize) -> Option<T>
    where
        T: Any + 'static + Send + Default,
    {
        self.get_at(code, idx).and_then(|prop| prop.get())
    }

    /// Gets a value from the collection when there may be more than one
    /// for the code.
    pub fn get_val<T>(&self, code: PropertyCode) -> Option<T>
    where
        T: Any + 'static + Send + Default,
    {
        self.get_val_at(code, 0)
    }

    /// Gets an integer value of a specific property.
    pub fn get_int(&self, code: PropertyCode) -> Option<i32> {
        self.get(code).and_then(|prop| prop.get_int())
    }

    /// Gets an integer value of a specific value when there may be more than one.
    pub fn get_int_at(&self, code: PropertyCode, idx: usize) -> Option<i32> {
        self.get_at(code, idx).and_then(|prop| prop.get_int())
    }

    /// Gets a binary value of a specific property.
    pub fn get_binary(&self, code: PropertyCode) -> Option<Binary> {
        self.get(code).and_then(|prop| prop.get_binary())
    }

    /// Gets a binary value of a specific value when there may be more than one.
    pub fn get_binary_at(&self, code: PropertyCode, idx: usize) -> Option<Binary> {
        self.get_at(code, idx).and_then(|prop| prop.get_binary())
    }

    /// Gets a string value of a specific property.
    pub fn get_string(&self, code: PropertyCode) -> Option<String> {
        self.get(code).and_then(|prop| prop.get_string())
    }

    /// Gets a binary value of a specific value when there may be more than one.
    pub fn get_string_at(&self, code: PropertyCode, idx: usize) -> Option<String> {
        self.get_at(code, idx).and_then(|prop| prop.get_string())
    }

    /// Gets a string pair for a specific property.
    pub fn get_string_pair(&self, code: PropertyCode) -> Option<(String, String)> {
        self.get(code).and_then(|prop| prop.get_string_pair())
    }

    /// Gets a string pair for a specific property when there may be more than one.
    pub fn get_string_pair_at(
        &self,
        code: PropertyCode,
        idx: usize,
    ) -> Option<(String, String)> {
        self.get_at(code, idx)
            .and_then(|prop| prop.get_string_pair())
    }

    /// Gets an iterator into the user property string pairs.
    pub fn user_iter(&self) -> StringPairIterator<'_> {
        StringPairIterator {
            props: self,
            code: PropertyCode::UserProperty,
            idx: 0,
        }
    }

    /// Searches for the specified key in the user properties and returns
    /// the value if found.
    pub fn find_user_property(&self, key: &str) -> Option<String> {
        for (k, v) in self.user_iter() {
            if k == key {
                return Some(v);
            }
        }
        None
    }
}

unsafe impl Send for Properties {}
unsafe impl Sync for Properties {}

impl Clone for PropertiesFfi {
    /// Creates a clone of the property.
    /// For string any binary properties, this also clones the heap memory
    /// so that each property is managing separate allocations.
    fn clone(&self) -> Self {
        // This does a deep copy in the C lib
        let cprops = unsafe { ffi::MQTTProperties_copy(&self.cprops) };
        Self { cprops }
    }
}

impl Drop for PropertiesFfi {
    fn drop(&mut self) {
        // This deletes the collection and all the elements in it.
        unsafe { ffi::MQTTProperties_free(&mut self.cprops) };
    }
}

impl From<ffi::MQTTProperties> for PropertiesFfi {
    /// Creates a set of properties from an underlying C struct.
    fn from(cprops: ffi::MQTTProperties) -> Self {
        Self { cprops }
    }
}

impl From<&ffi::MQTTProperties> for PropertiesFfi {
    /// Creates a set of properties from an underlying C struct.
    /// This does a deep copy of the properties in the C lib and then keeps
    /// the copy.
    fn from(cprops: &ffi::MQTTProperties) -> Self {
        let cprops = unsafe { ffi::MQTTProperties_copy(cprops) };
        Self { cprops }
    }
}

/// Iterator over the values for a specific property
pub struct PropertyIterator<'a> {
    props: &'a PropertiesFfi,
    code: Option<PropertyCode>,
    idx: usize,
}

impl<'a> Iterator for PropertyIterator<'a> {
    type Item = PropertyFfi;

    fn next(&mut self) -> Option<Self::Item> {
        let prop = match self.code {
            Some(code) => self.props.get_at(code, self.idx),
            None => self.props.get_idx(self.idx),
        };
        self.idx += 1;
        prop
    }
}

/// Iterator over the values for a specific property
pub struct StringPairIterator<'a> {
    props: &'a PropertiesFfi,
    code: PropertyCode,
    idx: usize,
}

impl<'a> Iterator for StringPairIterator<'a> {
    type Item = (String, String);

    fn next(&mut self) -> Option<Self::Item> {
        let pair = self.props.get_string_pair_at(self.code, self.idx);
        self.idx += 1;
        pair
    }
}

#[cfg(feature = "serde")]
impl Serialize for PropertiesFfi {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use PropertyType::*;

        let mut map = serializer.serialize_map(Some(self.len()))?;
        for prop in self.iter() {
            let name = prop.property_code().to_string();
            match prop.property_type() {
                Byte | TwoByteInteger | FourByteInteger | VariableByteInteger => {
                    map.serialize_entry(&name, &prop.get_int())?;
                }
                _ => {
                    map.serialize_entry(&name, &0)?;
                }
            }
        }
        map.end()
    }
}

impl From<&Properties> for PropertiesFfi {
    fn from(props: &Properties) -> Self {
        use PropertyType::*;

        let mut ffi_props = Self::new();
        for prop in props.iter() {
            let code = prop.property_code();

            match prop.property_type() {
                Byte => {
                    let val = prop.get_byte().unwrap();
                    ffi_props.push_byte(code, *val).unwrap();
                }
                TwoByteInteger => {
                    let val = prop.get_u16().unwrap();
                    ffi_props.push_u16(code, *val).unwrap();
                }
                FourByteInteger | VariableByteInteger => {
                    let val = prop.get_u32().unwrap();
                    ffi_props.push_u32(code, *val).unwrap();
                }
                BinaryData => {
                    let val = prop.get_binary().unwrap();
                    ffi_props.push_binary(code, val).unwrap();
                }
                Utf8EncodedString => {
                    let val = prop.get_string().unwrap();
                    ffi_props.push_string(code, val).unwrap();
                }
                Utf8StringPair => {
                    let (key, val) = prop.get_string_pair().unwrap();
                    ffi_props.push_string_pair(code, key, val).unwrap();
                }
            }
        }
        ffi_props
    }
}

impl From<Properties> for PropertiesFfi {
    fn from(props: Properties) -> Self {
        Self::from(&props)
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_property_type_new() {
        assert_eq!(
            PropertyType::new(ffi::MQTTPropertyTypes_MQTTPROPERTY_TYPE_BYTE),
            Some(PropertyType::Byte)
        );
        assert_eq!(
            PropertyType::new(ffi::MQTTPropertyTypes_MQTTPROPERTY_TYPE_BINARY_DATA),
            Some(PropertyType::BinaryData)
        );

        assert_eq!(PropertyType::new(7), None);
    }

    #[test]
    fn test_property_code_new() {
        assert_eq!(
            PropertyCode::new(
                ffi::MQTTPropertyCodes_MQTTPROPERTY_CODE_PAYLOAD_FORMAT_INDICATOR
            ),
            Some(PropertyCode::PayloadFormatIndicator)
        );
        assert_eq!(
            PropertyCode::new(
                ffi::MQTTPropertyCodes_MQTTPROPERTY_CODE_AUTHENTICATION_METHOD
            ),
            Some(PropertyCode::AuthenticationMethod)
        );
        assert_eq!(
            PropertyCode::new(ffi::MQTTPropertyCodes_MQTTPROPERTY_CODE_USER_PROPERTY),
            Some(PropertyCode::UserProperty)
        );
    }

    #[test]
    fn test_property_generic_new() {
        // ResponseTopic expects String/&str, not u16 — should fail
        let prop = Property::new(PropertyCode::ResponseTopic, 42u16);
        assert!(prop.is_none());

        // MaximumQos expects u8 — should succeed
        let prop = Property::new(PropertyCode::MaximumQos, 2u8);
        assert!(prop.is_some());

        let topic = "data/temp".to_string();
        let prop = Property::new(PropertyCode::ResponseTopic, topic.clone()).unwrap();
        assert_eq!(prop.get_string(), Some(topic.as_str()));

        let topic = "data/temp";
        let prop = Property::new(PropertyCode::ResponseTopic, topic).unwrap();
        assert_eq!(prop.get_string(), Some(topic));
    }

    #[test]
    fn test_new_property_byte() {
        let val = 1u8;

        let prop = Property::PayloadFormatIndicator(val);
        assert_eq!(prop.get_byte(), Some(&val));
        assert_eq!(prop.get_uint(), Some(val as u32));

        // Property::new() also works
        let prop = Property::new(PropertyCode::PayloadFormatIndicator, val).unwrap();
        assert_eq!(prop.get_byte(), Some(&val));

        // Wrong type returns None
        let res = Property::new(PropertyCode::PayloadFormatIndicator, 0x100u16);
        assert!(res.is_none());
    }

    #[test]
    fn test_new_property_u16() {
        let val = 1024u16;

        let prop = Property::ReceiveMaximum(val);
        assert_eq!(prop.get_u16(), Some(&val));
        assert_eq!(prop.get_uint(), Some(val as u32));

        // Property::new() also works
        let prop = Property::new(PropertyCode::ReceiveMaximum, val).unwrap();
        assert_eq!(prop.get_u16(), Some(&val));
    }

    #[test]
    fn test_new_property_u32() {
        let val = 1024u32 * 1024;

        let prop = Property::MessageExpiraryInterval(val);
        assert_eq!(prop.get_u32(), Some(&val));
        assert_eq!(prop.get_uint(), Some(val));

        // Property::new() also works
        let prop = Property::new(PropertyCode::MessageExpiryInterval, val).unwrap();
        assert_eq!(prop.get_u32(), Some(&val));
    }

    #[test]
    fn test_new_property_binary() {
        let val = b"12345";

        let prop = Property::CorrelationData(val.to_vec());
        assert_eq!(prop.get_binary(), Some(val.as_ref()));

        // Property::new() also works
        let prop = Property::new(PropertyCode::CorrelationData, val.as_ref()).unwrap();
        assert_eq!(prop.get_binary(), Some(val.as_ref()));
    }

    #[test]
    fn test_new_property_string() {
        let val = "replies/myqueue";

        let prop = Property::ResponseTopic(val.to_string());
        assert_eq!(prop.get_string(), Some(val));

        // Property::new() also works
        let prop = Property::new(PropertyCode::ResponseTopic, val).unwrap();
        assert_eq!(prop.get_string(), Some(val));
    }

    #[test]
    fn test_new_property_string_pair() {
        let key = "mykey";
        let val = "myvalue";

        let prop = Property::User((key.to_string(), val.to_string()));
        assert_eq!(
            prop.get_string_pair(),
            Some(&(key.to_string(), val.to_string()))
        );

        // Property::new() also works
        let prop = Property::new(PropertyCode::UserProperty, (key, val)).unwrap();
        assert_eq!(
            prop.get_string_pair(),
            Some(&(key.to_string(), val.to_string()))
        );
    }

    #[test]
    fn test_move_property_int() {
        let val = 1024u32 * 1024;
        let org_prop = Property::MessageExpiraryInterval(val);
        assert_eq!(org_prop.get_u32(), Some(&val));

        let prop = org_prop;
        assert_eq!(prop.get_u32(), Some(&val));
    }

    #[test]
    fn test_move_property_binary() {
        let val = b"12345";
        let org_prop = Property::CorrelationData(val.to_vec());
        let prop = org_prop;
        assert_eq!(prop.get_binary(), Some(val.as_ref()));
    }

    #[test]
    fn test_move_property_string() {
        let val = "replies/myqueue";
        let org_prop = Property::ResponseTopic(val.to_string());
        let prop = org_prop;
        assert_eq!(prop.get_string(), Some(val));
    }

    #[test]
    fn test_clone_property_binary() {
        let val = b"12345";
        let org_prop = Property::CorrelationData(val.to_vec());
        let prop = org_prop.clone();

        assert_eq!(prop.get_binary(), Some(val.as_ref()));
        // Cloned Vec should be independent
        if let (Property::CorrelationData(a), Property::CorrelationData(b)) =
            (&org_prop, &prop)
        {
            assert!(!std::ptr::eq(a.as_ptr(), b.as_ptr()));
        }
    }

    #[test]
    fn test_clone_property_string() {
        let val = "replies/myqueue";
        let org_prop = Property::ResponseTopic(val.to_string());
        let prop = org_prop.clone();
        assert_eq!(prop.get_string(), Some(val));
    }

    /////////////////////////////////////////////////////////////////////////

    #[test]
    fn test_properties_default() {
        let props = Properties::default();
        assert!(props.is_empty());
        assert_eq!(props.len(), 0);
    }

    #[test]
    fn test_properties_push() {
        let mut props = Properties::new();

        let prop = Property::PayloadFormatIndicator(1);
        props.push(prop);

        assert_eq!(props.len(), 1);
        assert!(!props.is_empty());
    }

    #[test]
    fn test_properties_push_val() {
        let mut props = Properties::new();

        props
            .push_val(PropertyCode::ResponseTopic, "responses/somewhere")
            .unwrap();

        assert!(!props.is_empty());
        assert_eq!(props.len(), 1);

        props
            .push_val(PropertyCode::CorrelationData, b"12345".as_ref())
            .unwrap();
        assert_eq!(props.len(), 2);
    }

    #[test]
    fn test_properties_get() {
        let code = PropertyCode::PayloadFormatIndicator;
        let val = 1u8;

        let mut props = Properties::new();
        props.push(Property::PayloadFormatIndicator(val));

        let prop = props.get(code).unwrap();
        assert_eq!(prop.get_byte(), Some(&val));
        assert_eq!(prop.get_uint(), Some(val as u32));
    }

    #[test]
    fn test_properties_multiple() {
        let code = PropertyCode::UserProperty;

        let mut props = Properties::new();

        props.push(Property::User(("user0".to_string(), "val0".to_string())));
        props.push(Property::User(("user1".to_string(), "val1".to_string())));
        props.push(Property::User(("user2".to_string(), "val2".to_string())));

        assert!(!props.is_empty());
        assert_eq!(props.len(), 3);

        // get_string_pair() returns the first match
        assert_eq!(
            props.get_string_pair(code),
            Some(&("user0".to_string(), "val0".to_string()))
        );

        // Iterate to access all
        let pairs: Vec<&(String, String)> =
            props.iter().filter_map(|p| p.get_string_pair()).collect();
        assert_eq!(pairs[0], &("user0".to_string(), "val0".to_string()));
        assert_eq!(pairs[1], &("user1".to_string(), "val1".to_string()));
        assert_eq!(pairs[2], &("user2".to_string(), "val2".to_string()));
    }

    #[test]
    fn test_properties_iterator() {
        let code = PropertyCode::UserProperty;

        let mut props = Properties::new();

        props.push(Property::User(("user0".to_string(), "val0".to_string())));
        props.push(Property::User(("user1".to_string(), "val1".to_string())));
        props.push(Property::User(("user2".to_string(), "val2".to_string())));

        let mut it = props.iter().filter(|p| p.property_code() == code);

        assert_eq!(
            it.next().and_then(|p| p.get_string_pair()),
            Some(&("user0".to_string(), "val0".to_string()))
        );
        assert_eq!(
            it.next().and_then(|p| p.get_string_pair()),
            Some(&("user1".to_string(), "val1".to_string()))
        );
        assert_eq!(
            it.next().and_then(|p| p.get_string_pair()),
            Some(&("user2".to_string(), "val2".to_string()))
        );
        assert_eq!(it.next().and_then(|p| p.get_string_pair()), None);
    }

    #[test]
    fn test_properties_user_iterator() {
        let mut props = Properties::new();

        props.push(Property::User(("user0".to_string(), "val0".to_string())));
        props.push(Property::User(("user1".to_string(), "val1".to_string())));
        props.push(Property::User(("user2".to_string(), "val2".to_string())));

        let mut it = props.iter().filter_map(|p| p.get_string_pair());

        assert_eq!(it.next(), Some(&("user0".to_string(), "val0".to_string())));
        assert_eq!(it.next(), Some(&("user1".to_string(), "val1".to_string())));
        assert_eq!(it.next(), Some(&("user2".to_string(), "val2".to_string())));
        assert_eq!(it.next(), None);
    }

    #[test]
    fn test_properties_user_find() {
        let code = PropertyCode::UserProperty;

        let mut props = Properties::new();

        props.push(Property::new(code, ("user0", "val0")).unwrap());
        props.push(Property::new(code, ("user1", "val1")).unwrap());
        props.push(Property::new(code, ("user2", "val2")).unwrap());

        assert_eq!(Some("val0"), props.find_user_property("user0"));
        assert_eq!(Some("val1"), props.find_user_property("user1"));
        assert_eq!(Some("val2"), props.find_user_property("user2"));
        assert_eq!(None, props.find_user_property("user3"));
    }

    #[test]
    fn test_properties_macro() {
        // Zero length
        let props = properties![];
        assert!(props.is_empty());

        // Some different properties
        let props = properties! {
            ResponseTopic("responses/somewhere".into()),
            CorrelationData(b"12345".into()),
            MaximumQos(1),
        };

        assert!(!props.is_empty());
        assert_eq!(3, props.len());
        assert_eq!(
            Some(b"12345".as_ref()),
            props.get_binary(PropertyCode::CorrelationData)
        );

        let props = properties! {
            SessionExpiryInterval(60u32),
        };

        assert_eq!(1, props.len());
        assert_eq!(
            Some(60),
            props.get_uint(PropertyCode::SessionExpiryInterval)
        );
    }
}
