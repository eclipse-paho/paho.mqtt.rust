// paho-mqtt-codec/examples/round_trip.rs
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

//! Encodes a PUBLISH packet to its wire form, then frames and decodes it
//! again — the same two steps a client performs on each side of a socket.

use bytes::{Bytes, BytesMut};
use paho_mqtt_codec::{
    MqttVersion, Packet, Properties, Property, PropertyId, PropertyValue, Publish, QoS, RawPacket,
    Result,
};

fn main() -> Result<()> {
    let ver = MqttVersion::V5;

    // Build a QoS 1 PUBLISH carrying an MQTT v5 property.
    let mut props = Properties::new();
    props.push(Property::new(
        PropertyId::MessageExpiryInterval,
        PropertyValue::U32(60),
    ));
    let publish = Publish::builder()
        .topic("sensors/temp")
        .payload(Bytes::from_static(b"21.5"))
        .qos_with_id(QoS::AtLeastOnce, 42)
        .properties(props)
        .finalize();

    // Encode it. This is what would be written to the network.
    let mut wire = BytesMut::new();
    Packet::Publish(publish).encode(&mut wire, ver)?;
    println!("encoded {} bytes: {:02x?}", wire.len(), &wire[..]);

    // On the receiving side: frame complete packets out of the read buffer,
    // then parse each one for the negotiated protocol version.
    while let Some(raw) = RawPacket::decode(&mut wire)? {
        match Packet::from_raw(raw, ver)? {
            Packet::Publish(p) => {
                println!(
                    "PUBLISH id={:?} qos={} topic={:?} payload={:?} props={}",
                    p.packet_id,
                    p.qos,
                    p.topic,
                    p.payload,
                    p.properties.len(),
                );
            }
            other => println!("{other:?}"),
        }
    }
    Ok(())
}
