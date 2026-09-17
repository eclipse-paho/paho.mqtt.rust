// paho-mqtt-codec/benches/codec.rs
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

//! Encode and decode benchmarks for every packet type.
//!
//! Run with `cargo bench`, or `cargo bench -- encode/publish` to filter.
//! To compare against a saved baseline:
//!
//!     cargo bench -- --save-baseline before
//!     ... make changes ...
//!     cargo bench -- --baseline before
//!
//! Encode benchmarks reuse one output buffer across iterations, which is
//! what a network runtime does; they measure the codec, not the allocator.
//! Decode benchmarks take a fresh copy of the wire bytes per iteration (the
//! copy is excluded from the timing) because framing consumes its input.

use bytes::{Bytes, BytesMut};
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use paho_mqtt_codec::{
    Auth, ConnAck, Connect, Disconnect, MqttVersion, Packet, Properties, Property,
    PropertyId, PropertyValue, PubAck, Publish, QoS, RawPacket, ReasonCode, RetainHandling,
    SubAck, Subscribe, Subscription, UnsubAck, Unsubscribe, Will,
};
use std::hint::black_box;

const VERSIONS: [(MqttVersion, &str); 2] =
    [(MqttVersion::V3_1_1, "v311"), (MqttVersion::V5, "v5")];

/// v3.1 differs from v3.1.1 only in CONNECT/CONNACK, so only those two are
/// benchmarked for it.
const V31: (MqttVersion, &str) = (MqttVersion::V3_1, "v31");

/// The packets to benchmark for `ver`: everything, or just CONNECT/CONNACK
/// for v3.1.
fn packets_for(ver: MqttVersion) -> Vec<(&'static str, Packet)> {
    let pkts = packets(ver);
    if ver == MqttVersion::V3_1 {
        pkts.into_iter().take(2).collect()
    }
    else {
        pkts
    }
}

/// A representative v5 property set: two user properties and a few scalars.
fn props() -> Properties {
    let mut p = Properties::new();
    p.push(Property::new(
        PropertyId::SessionExpiryInterval,
        PropertyValue::U32(3600),
    ));
    p.push(Property::new(
        PropertyId::ReceiveMaximum,
        PropertyValue::U16(20),
    ));
    p.push(Property::new(
        PropertyId::UserProperty,
        PropertyValue::StringPair("client".into(), "bench".into()),
    ));
    p.push(Property::new(
        PropertyId::UserProperty,
        PropertyValue::StringPair("region".into(), "us-east-1".into()),
    ));
    p
}

/// v5 gets the property set; v3.1.1 packets carry none on the wire.
fn props_for(ver: MqttVersion) -> Properties {
    match ver {
        MqttVersion::V5 => props(),
        _ => Properties::new(),
    }
}

fn publish(ver: MqttVersion, payload_len: usize) -> Packet {
    let mut p = Publish::new(
        "sensors/building-7/floor-3/room-12/temperature",
        Bytes::from(vec![0xABu8; payload_len]),
    );
    p.qos = QoS::AtLeastOnce;
    p.packet_id = Some(1234);
    if ver == MqttVersion::V5 {
        p.properties.push(Property::new(
            PropertyId::MessageExpiryInterval,
            PropertyValue::U32(60),
        ));
        p.properties.push(Property::new(
            PropertyId::ContentType,
            PropertyValue::String("application/json".into()),
        ));
    }
    Packet::Publish(p)
}

/// One instance of every packet type for the given version.
fn packets(ver: MqttVersion) -> Vec<(&'static str, Packet)> {
    let mut connect = Connect::new(ver, true, "bench-client-0001");
    connect.keep_alive = 60;
    connect.username = Some("user".into());
    connect.password = Some(Bytes::from_static(b"secret"));
    connect.will = Some(Will::new(
        "clients/bench/status",
        Bytes::from_static(b"offline"),
        QoS::AtLeastOnce,
        true,
    ));
    connect.properties = props_for(ver);

    let mut connack = ConnAck::new(false, ReasonCode::Success);
    connack.properties = props_for(ver);

    let mut puback_rc = PubAck::new(1234);
    puback_rc.reason_code = ReasonCode::NoMatchingSubscribers;

    let mut subscribe = Subscribe::new(
        77,
        vec![
            Subscription::new("sensors/+/temperature", QoS::AtLeastOnce),
            Subscription::new("alerts/#", QoS::ExactlyOnce),
            Subscription::new("config/bench", QoS::AtMostOnce),
        ],
    );
    if ver == MqttVersion::V5 {
        subscribe.subscriptions[0].no_local = true;
        subscribe.subscriptions[1].retain_handling = RetainHandling::DoNotSend;
        subscribe.properties.push(Property::new(
            PropertyId::SubscriptionIdentifier,
            PropertyValue::VarInt(5),
        ));
    }

    let suback = SubAck {
        packet_id: 77,
        reason_codes: vec![
            ReasonCode::GrantedQos1,
            ReasonCode::GrantedQos2,
            ReasonCode::Success,
        ],
        properties: Properties::new(),
    };

    let unsubscribe = Unsubscribe {
        packet_id: 78,
        topic_filters: vec!["sensors/+/temperature".into(), "alerts/#".into()],
        properties: Properties::new(),
    };

    let unsuback = UnsubAck {
        packet_id: 78,
        reason_codes: match ver {
            MqttVersion::V5 => vec![ReasonCode::Success, ReasonCode::NoSubscriptionFound],
            _ => vec![],
        },
        properties: Properties::new(),
    };

    let mut disconnect_rc = Disconnect::new(ReasonCode::SessionTakenOver);
    if ver == MqttVersion::V5 {
        disconnect_rc.properties.push(Property::new(
            PropertyId::SessionExpiryInterval,
            PropertyValue::U32(0),
        ));
        disconnect_rc.properties.push(Property::new(
            PropertyId::ReasonString,
            PropertyValue::String("session taken over".into()),
        ));
        disconnect_rc.properties.push(Property::new(
            PropertyId::UserProperty,
            PropertyValue::StringPair("client".into(), "bench".into()),
        ));
        disconnect_rc.properties.push(Property::new(
            PropertyId::UserProperty,
            PropertyValue::StringPair("region".into(), "us-east-1".into()),
        ));
    }

    let mut pkts = vec![
        ("connect", Packet::Connect(connect)),
        ("connack", Packet::ConnAck(connack)),
        ("publish_qos0_16B", {
            let mut p = Publish::new("t", Bytes::from_static(b"0123456789abcdef"));
            p.qos = QoS::AtMostOnce;
            Packet::Publish(p)
        }),
        ("puback", Packet::PubAck(PubAck::new(1234))),
        ("puback_reason", Packet::PubAck(puback_rc)),
        ("pubrec", Packet::PubRec(PubAck::new(1234))),
        ("pubrel", Packet::PubRel(PubAck::new(1234))),
        ("pubcomp", Packet::PubComp(PubAck::new(1234))),
        ("subscribe", Packet::Subscribe(subscribe)),
        ("suback", Packet::SubAck(suback)),
        ("unsubscribe", Packet::Unsubscribe(unsubscribe)),
        ("unsuback", Packet::UnsubAck(unsuback)),
        ("pingreq", Packet::PingReq),
        ("pingresp", Packet::PingResp),
        ("disconnect", Packet::Disconnect(Disconnect::default())),
    ];
    if ver == MqttVersion::V5 {
        pkts.push(("disconnect_reason", Packet::Disconnect(disconnect_rc)));
        let mut auth = Auth::new(ReasonCode::ContinueAuthentication);
        auth.properties.push(Property::new(
            PropertyId::AuthenticationMethod,
            PropertyValue::String("SCRAM-SHA-256".into()),
        ));
        auth.properties.push(Property::new(
            PropertyId::AuthenticationData,
            PropertyValue::Binary(Bytes::from_static(&[0u8; 32])),
        ));
        pkts.push(("auth", Packet::Auth(auth)));
    }
    pkts
}

const PAYLOAD_SIZES: [usize; 5] = [16, 256, 4 * 1024, 64 * 1024, 1024 * 1024];

fn wire(pkt: &Packet, ver: MqttVersion) -> Bytes {
    let mut buf = BytesMut::new();
    pkt.encode(&mut buf, ver).unwrap();
    buf.freeze()
}

fn bench_encode(c: &mut Criterion) {
    let mut g = c.benchmark_group("encode");
    for (ver, tag) in VERSIONS.into_iter().chain([V31]) {
        for (name, pkt) in packets_for(ver) {
            let mut buf = BytesMut::with_capacity(1024);
            g.bench_function(BenchmarkId::new(name, tag), |b| {
                b.iter(|| {
                    buf.clear();
                    black_box(&pkt).encode(&mut buf, ver).unwrap();
                    black_box(&buf);
                })
            });
        }
    }
    g.finish();

    // PUBLISH across payload sizes, reported as throughput so the copy cost
    // is visible as bytes/second.
    let mut g = c.benchmark_group("encode/publish");
    for (ver, tag) in VERSIONS {
        for len in PAYLOAD_SIZES {
            let pkt = publish(ver, len);
            let mut buf = BytesMut::with_capacity(len + 256);
            g.throughput(Throughput::Bytes(len as u64));
            g.bench_function(BenchmarkId::new(format!("{tag}/encode"), len), |b| {
                b.iter(|| {
                    buf.clear();
                    black_box(&pkt).encode(&mut buf, ver).unwrap();
                    black_box(&buf);
                })
            });
            // Header-only encode; the payload is returned by reference.
            g.bench_function(BenchmarkId::new(format!("{tag}/encode_split"), len), |b| {
                b.iter(|| {
                    buf.clear();
                    let payload = black_box(&pkt).encode_split(&mut buf, ver).unwrap();
                    black_box((&buf, payload));
                })
            });
        }
    }
    g.finish();
}

fn bench_decode(c: &mut Criterion) {
    let mut g = c.benchmark_group("decode");
    for (ver, tag) in VERSIONS.into_iter().chain([V31]) {
        for (name, pkt) in packets_for(ver) {
            let bytes = wire(&pkt, ver);
            g.bench_function(BenchmarkId::new(name, tag), |b| {
                b.iter_batched(
                    || BytesMut::from(&bytes[..]),
                    |mut src| {
                        let raw = RawPacket::decode(&mut src).unwrap().unwrap();
                        black_box(Packet::from_raw(raw, ver).unwrap())
                    },
                    BatchSize::SmallInput,
                )
            });
        }
    }
    g.finish();

    let mut g = c.benchmark_group("decode/publish");
    for (ver, tag) in VERSIONS {
        for len in PAYLOAD_SIZES {
            let bytes = wire(&publish(ver, len), ver);
            g.throughput(Throughput::Bytes(len as u64));
            g.bench_function(BenchmarkId::new(tag, len), |b| {
                b.iter_batched(
                    || BytesMut::from(&bytes[..]),
                    |mut src| {
                        let raw = RawPacket::decode(&mut src).unwrap().unwrap();
                        black_box(Packet::from_raw(raw, ver).unwrap())
                    },
                    BatchSize::LargeInput,
                )
            });
        }
    }
    g.finish();
}

/// Framing alone: how quickly a read buffer holding many small packets is
/// split into individual frames, independent of parsing.
fn bench_framing(c: &mut Criterion) {
    let mut stream = BytesMut::new();
    let ack = Packet::PubAck(PubAck::new(1));
    for _ in 0..1000 {
        ack.encode(&mut stream, MqttVersion::V5).unwrap();
    }
    let stream = stream.freeze();

    let mut g = c.benchmark_group("framing");
    g.throughput(Throughput::Elements(1000));
    g.bench_function("1000_pubacks", |b| {
        b.iter_batched(
            || BytesMut::from(&stream[..]),
            |mut src| {
                let mut n = 0;
                while let Some(raw) = RawPacket::decode(&mut src).unwrap() {
                    black_box(raw);
                    n += 1;
                }
                n
            },
            BatchSize::LargeInput,
        )
    });
    g.finish();
}

criterion_group!(benches, bench_encode, bench_decode, bench_framing);
criterion_main!(benches);
