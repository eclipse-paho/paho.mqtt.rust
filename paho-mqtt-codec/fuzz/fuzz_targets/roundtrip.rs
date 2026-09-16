// paho-mqtt-codec/fuzz/fuzz_targets/roundtrip.rs
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

//! Structure-aware fuzz target: builds *valid* packets from arbitrary bytes
//! and checks the encoder's invariants.
//!
//! Where `decode` mutates wire bytes and mostly exercises rejection paths,
//! this target starts from well-formed `Packet` values so every iteration
//! reaches the encoder and a successful decode. It checks that:
//!
//! - `encode` produces exactly `encoded_size()` bytes,
//! - `encode_to_bytes` and `encode_split` (header + payload) produce the
//!   same bytes as `encode`,
//! - framing consumes exactly the encoded bytes, and
//! - decoding yields a packet equal to the one encoded.
//!
//! Generated values stay inside what the specification allows (string
//! lengths, no U+0000, valid property ranges, no duplicate non-repeatable
//! properties, packet IDs ≠ 0, …) so a decode failure is a real bug.

#![no_main]

use arbitrary::{Result as AResult, Unstructured};
use bytes::{Bytes, BytesMut};
use libfuzzer_sys::fuzz_target;
use paho_mqtt_codec::{
    Auth, ConnAck, Connect, Disconnect, MqttVersion, Properties, Property, PropertyId,
    PropertyType, PropertyValue, PubAck, Publish, QoS, ReasonCode, RetainHandling, SubAck,
    Subscribe, Subscription, UnsubAck, Unsubscribe, Will,
    packet::{Packet, RawPacket},
};

// ---- primitive generators ------------------------------------------------

/// Payload sizes are kept modest so the target spends its time on
/// structure, not on copying megabytes.
const MAX_BLOB: usize = 4096;

fn version(u: &mut Unstructured) -> AResult<MqttVersion> {
    Ok(if u.arbitrary()? { MqttVersion::V5 } else { MqttVersion::V3_1_1 })
}

fn qos(u: &mut Unstructured) -> AResult<QoS> {
    Ok(match u.int_in_range(0u8..=2)? {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    })
}

/// A UTF-8 string that is legal on the wire: ≤ 65 535 bytes, no U+0000.
fn utf8(u: &mut Unstructured, max: usize) -> AResult<String> {
    let s: &str = u.arbitrary()?;
    let mut s: String = s.chars().filter(|&c| c != '\0').collect();
    while s.len() > max {
        s.pop();
    }
    Ok(s)
}

/// A topic *name*: non-empty, no wildcards.
fn topic_name(u: &mut Unstructured) -> AResult<String> {
    let mut t: String = utf8(u, 256)?
        .chars()
        .filter(|&c| c != '+' && c != '#')
        .collect();
    if t.is_empty() {
        t.push('t');
    }
    Ok(t)
}

/// A topic filter: non-empty, wildcards allowed.
fn topic_filter(u: &mut Unstructured) -> AResult<String> {
    let mut t = utf8(u, 256)?;
    if t.is_empty() {
        t.push_str("a/#");
    }
    Ok(t)
}

fn blob(u: &mut Unstructured, max: usize) -> AResult<Bytes> {
    let n = u.int_in_range(0..=max)?;
    Ok(Bytes::from(u.bytes(n.min(u.len()))?.to_vec()))
}

fn packet_id(u: &mut Unstructured) -> AResult<u16> {
    u.int_in_range(1..=u16::MAX)
}

fn reason_code(u: &mut Unstructured) -> AResult<ReasonCode> {
    // Every defined code; unknown bytes are not representable.
    const CODES: [u8; 44] = [
        0x00, 0x01, 0x02, 0x04, 0x10, 0x11, 0x18, 0x19, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86,
        0x87, 0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x8D, 0x8E, 0x8F, 0x90, 0x91, 0x92, 0x93, 0x94, 0x95,
        0x96, 0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0xA0, 0xA1, 0xA2, 0x00,
    ];
    Ok(ReasonCode::try_from(*u.choose(&CODES)?).unwrap())
}

// ---- properties ------------------------------------------------------------

const ALL_IDS: [PropertyId; 27] = [
    PropertyId::PayloadFormatIndicator,
    PropertyId::MessageExpiryInterval,
    PropertyId::ContentType,
    PropertyId::ResponseTopic,
    PropertyId::CorrelationData,
    PropertyId::SubscriptionIdentifier,
    PropertyId::SessionExpiryInterval,
    PropertyId::AssignedClientIdentifier,
    PropertyId::ServerKeepAlive,
    PropertyId::AuthenticationMethod,
    PropertyId::AuthenticationData,
    PropertyId::RequestProblemInformation,
    PropertyId::WillDelayInterval,
    PropertyId::RequestResponseInformation,
    PropertyId::ResponseInformation,
    PropertyId::ServerReference,
    PropertyId::ReasonString,
    PropertyId::ReceiveMaximum,
    PropertyId::TopicAliasMaximum,
    PropertyId::TopicAlias,
    PropertyId::MaximumQoS,
    PropertyId::RetainAvailable,
    PropertyId::UserProperty,
    PropertyId::MaximumPacketSize,
    PropertyId::WildcardSubscriptionAvailable,
    PropertyId::SubscriptionIdentifierAvailable,
    PropertyId::SharedSubscriptionAvailable,
];

/// A property whose value is valid for its identifier.
fn property(u: &mut Unstructured, id: PropertyId) -> AResult<Property> {
    use PropertyId::*;
    let value = match id.value_type() {
        PropertyType::Byte => PropertyValue::Byte(match id {
            PayloadFormatIndicator
            | RequestProblemInformation
            | RequestResponseInformation
            | MaximumQoS
            | RetainAvailable
            | WildcardSubscriptionAvailable
            | SubscriptionIdentifierAvailable
            | SharedSubscriptionAvailable => u.int_in_range(0..=1)?,
            _ => u.arbitrary()?,
        }),
        PropertyType::U16 => PropertyValue::U16(match id {
            ReceiveMaximum | TopicAlias => u.int_in_range(1..=u16::MAX)?,
            _ => u.arbitrary()?,
        }),
        PropertyType::U32 => PropertyValue::U32(match id {
            MaximumPacketSize => u.int_in_range(1..=u32::MAX)?,
            _ => u.arbitrary()?,
        }),
        PropertyType::VarInt => {
            PropertyValue::VarInt(u.int_in_range(1..=paho_mqtt_codec::varlen::MAX_VARLEN)?)
        }
        PropertyType::Binary => PropertyValue::Binary(blob(u, 512)?),
        PropertyType::String => PropertyValue::String(match id {
            ResponseTopic => topic_name(u)?,
            _ => utf8(u, 512)?,
        }),
        PropertyType::StringPair => PropertyValue::StringPair(utf8(u, 128)?, utf8(u, 128)?),
    };
    Ok(Property::new(id, value))
}

/// A property list with no repeated non-repeatable identifiers. Empty for
/// v3.1.1, where properties are never on the wire.
fn properties(u: &mut Unstructured, ver: MqttVersion) -> AResult<Properties> {
    let mut props = Properties::new();
    if ver != MqttVersion::V5 {
        return Ok(props);
    }
    let n = u.int_in_range(0..=8)?;
    let mut used = [false; 64];
    for _ in 0..n {
        let id = *u.choose(&ALL_IDS)?;
        if !id.is_repeatable() {
            if used[id as usize] {
                continue;
            }
            used[id as usize] = true;
        }
        props.push(property(u, id)?);
    }
    Ok(props)
}

// ---- packets ---------------------------------------------------------------

fn connect(u: &mut Unstructured, ver: MqttVersion) -> AResult<Connect> {
    let mut c = Connect::new(ver, u.arbitrary()?, utf8(u, 256)?);
    c.keep_alive = u.arbitrary()?;
    c.username = if u.arbitrary()? { Some(utf8(u, 256)?) } else { None };
    // v3.1.1 forbids a password without a username.
    c.password = if u.arbitrary()? && (ver == MqttVersion::V5 || c.username.is_some()) {
        Some(blob(u, 256)?)
    }
    else {
        None
    };
    if u.arbitrary()? {
        let mut will = Will::new(topic_name(u)?, blob(u, 1024)?, qos(u)?, u.arbitrary()?);
        will.properties = properties(u, ver)?;
        c.will = Some(will);
    }
    c.properties = properties(u, ver)?;
    Ok(c)
}

fn connack(u: &mut Unstructured, ver: MqttVersion) -> AResult<ConnAck> {
    // v3.1.1 has only six return codes; session_present must be false on
    // a refused connection.
    let rc = match ver {
        MqttVersion::V5 => reason_code(u)?,
        _ => *u.choose(&[
            ReasonCode::Success,
            ReasonCode::UnsupportedProtocolVersion,
            ReasonCode::ClientIdentifierNotValid,
            ReasonCode::ServerUnavailable,
            ReasonCode::BadUserNameOrPassword,
            ReasonCode::NotAuthorized,
        ])?,
    };
    let session_present = rc == ReasonCode::Success && u.arbitrary()?;
    let mut a = ConnAck::new(session_present, rc);
    a.properties = properties(u, ver)?;
    Ok(a)
}

fn publish(u: &mut Unstructured, ver: MqttVersion) -> AResult<Publish> {
    let mut p = Publish::new(topic_name(u)?, blob(u, MAX_BLOB)?);
    p.qos = qos(u)?;
    p.retain = u.arbitrary()?;
    if p.qos != QoS::AtMostOnce {
        p.packet_id = Some(packet_id(u)?);
        p.dup = u.arbitrary()?;
    }
    p.properties = properties(u, ver)?;
    Ok(p)
}

fn puback(u: &mut Unstructured, ver: MqttVersion) -> AResult<PubAck> {
    let mut a = PubAck::new(packet_id(u)?);
    if ver == MqttVersion::V5 {
        a.reason_code = reason_code(u)?;
        a.properties = properties(u, ver)?;
    }
    Ok(a)
}

fn subscribe(u: &mut Unstructured, ver: MqttVersion) -> AResult<Subscribe> {
    let n = u.int_in_range(1..=6)?;
    let mut subs = Vec::with_capacity(n);
    for _ in 0..n {
        let mut s = Subscription::new(topic_filter(u)?, qos(u)?);
        if ver == MqttVersion::V5 {
            s.no_local = u.arbitrary()?;
            s.retain_as_published = u.arbitrary()?;
            s.retain_handling = *u.choose(&[
                RetainHandling::SendOnSubscribe,
                RetainHandling::SendIfNew,
                RetainHandling::DoNotSend,
            ])?;
        }
        subs.push(s);
    }
    let mut s = Subscribe::new(packet_id(u)?, subs);
    s.properties = properties(u, ver)?;
    Ok(s)
}

fn suback(u: &mut Unstructured, ver: MqttVersion) -> AResult<SubAck> {
    let n = u.int_in_range(1..=6)?;
    let mut codes = Vec::with_capacity(n);
    for _ in 0..n {
        codes.push(match ver {
            MqttVersion::V5 => reason_code(u)?,
            _ => *u.choose(&[
                ReasonCode::GrantedQos0,
                ReasonCode::GrantedQos1,
                ReasonCode::GrantedQos2,
                ReasonCode::UnspecifiedError,
            ])?,
        });
    }
    let mut s = SubAck::new(packet_id(u)?, codes);
    s.properties = properties(u, ver)?;
    Ok(s)
}

fn unsubscribe(u: &mut Unstructured, ver: MqttVersion) -> AResult<Unsubscribe> {
    let n = u.int_in_range(1..=6)?;
    let mut filters = Vec::with_capacity(n);
    for _ in 0..n {
        filters.push(topic_filter(u)?);
    }
    let mut s = Unsubscribe::new(packet_id(u)?, filters);
    s.properties = properties(u, ver)?;
    Ok(s)
}

fn unsuback(u: &mut Unstructured, ver: MqttVersion) -> AResult<UnsubAck> {
    let codes = match ver {
        MqttVersion::V5 => {
            let n = u.int_in_range(1..=6)?;
            let mut v = Vec::with_capacity(n);
            for _ in 0..n {
                v.push(reason_code(u)?);
            }
            v
        }
        _ => Vec::new(),
    };
    let mut s = UnsubAck::new(packet_id(u)?, codes);
    s.properties = properties(u, ver)?;
    Ok(s)
}

fn disconnect(u: &mut Unstructured, ver: MqttVersion) -> AResult<Disconnect> {
    let mut d = Disconnect::default();
    if ver == MqttVersion::V5 {
        d.reason_code = reason_code(u)?;
        d.properties = properties(u, ver)?;
    }
    Ok(d)
}

fn auth(u: &mut Unstructured) -> AResult<Auth> {
    let mut a = Auth::new(reason_code(u)?);
    a.properties = properties(u, MqttVersion::V5)?;
    Ok(a)
}

fn packet(u: &mut Unstructured, ver: MqttVersion) -> AResult<Packet> {
    let n_types = if ver == MqttVersion::V5 { 15 } else { 14 };
    Ok(match u.int_in_range(0..=n_types - 1)? {
        0 => Packet::Connect(connect(u, ver)?),
        1 => Packet::ConnAck(connack(u, ver)?),
        2 => Packet::Publish(publish(u, ver)?),
        3 => Packet::PubAck(puback(u, ver)?),
        4 => Packet::PubRec(puback(u, ver)?),
        5 => Packet::PubRel(puback(u, ver)?),
        6 => Packet::PubComp(puback(u, ver)?),
        7 => Packet::Subscribe(subscribe(u, ver)?),
        8 => Packet::SubAck(suback(u, ver)?),
        9 => Packet::Unsubscribe(unsubscribe(u, ver)?),
        10 => Packet::UnsubAck(unsuback(u, ver)?),
        11 => Packet::PingReq,
        12 => Packet::PingResp,
        13 => Packet::Disconnect(disconnect(u, ver)?),
        _ => Packet::Auth(auth(u)?),
    })
}

// ---- the target ------------------------------------------------------------

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let Ok(ver) = version(&mut u) else { return };
    let Ok(pkt) = packet(&mut u, ver) else { return };

    // encode
    let mut wire = BytesMut::new();
    pkt.encode(&mut wire, ver)
        .unwrap_or_else(|e| panic!("valid packet failed to encode: {e}\n{pkt:#?}"));
    assert_eq!(
        wire.len(),
        pkt.encoded_size(ver).unwrap(),
        "encoded_size disagrees with encode\n{pkt:#?}"
    );

    // encode_to_bytes
    let via_bytes = pkt.encode_to_bytes(ver).unwrap();
    assert_eq!(&via_bytes[..], &wire[..], "encode_to_bytes differs from encode");

    // encode_split: header + payload must equal encode
    let mut head = BytesMut::new();
    let tail = pkt.encode_split(&mut head, ver).unwrap();
    let mut joined = head.clone();
    if let Some(p) = &tail {
        joined.extend_from_slice(p);
    }
    assert_eq!(&joined[..], &wire[..], "encode_split differs from encode");
    if let Packet::Publish(p) = &pkt {
        assert_eq!(tail.is_some(), !p.payload.is_empty());
    }
    else {
        assert!(tail.is_none());
    }

    // frame + decode: must consume exactly the encoded bytes and give the
    // same packet back
    let raw = RawPacket::decode(&mut wire)
        .expect("framing failed on valid encoding")
        .expect("encoding incomplete");
    assert!(wire.is_empty(), "framing left {} trailing bytes", wire.len());
    let back = Packet::from_raw(raw, ver)
        .unwrap_or_else(|e| panic!("valid encoding failed to decode: {e}\n{pkt:#?}"));
    assert_eq!(back, pkt, "packet changed across a round trip");
});
