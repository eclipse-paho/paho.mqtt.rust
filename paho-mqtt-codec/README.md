# Eclipse Paho MQTT Rust Codec

A pure-Rust encoder and decoder for MQTT control packets, covering protocol versions 3.1, 3.1.1, and 5.0, for the [Eclipse Paho](http://eclipse.org/paho) project.

This crate is a wire-format layer and nothing else: it converts between typed Rust structs and the bytes that go on the network, with no dependency on I/O, on an async runtime, or on any particular role in the protocol. Every packet type can be both encoded and decoded, so it serves a client, a broker, a proxy, a protocol analyzer, or an embedded device equally. It is the codec used by the [Eclipse Paho MQTT Rust client](https://github.com/eclipse/paho.mqtt.rust), and is usable in a `no_std` environment. Each packet has a single representation that is encoded or decoded according to the connection's negotiated version (v3.1, v3.1.1, or v5).

## Features

- Every control packet in MQTT v3.1, v3.1.1, and v5.0: CONNECT, CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, PINGREQ, PINGRESP, DISCONNECT, and AUTH. v3.1 (protocol name `MQIsdp`, level 3) differs from v3.1.1 only in CONNECT and CONNACK and is handled alongside it, so a server can still accept the older clients found in the field.
- One set of packet types for both protocol versions. Fields that only exist in v5 (reason codes, properties) are present on every packet and are simply not written to the wire on a v3.1.1 connection. CONNECT carries its own version and is decoded without knowing it in advance.
- Full MQTT v5 property support with typed values, including the shorthand encodings the specification permits when a reason code or property list is absent.
- Two-phase decoding: a version-independent framing step that pulls one complete packet out of a read buffer (returning `None` until enough bytes have arrived), followed by version-aware parsing.
- Zero-copy where the protocol allows it. Decoded PUBLISH payloads are slices of the receive buffer, and a PUBLISH can be encoded with its header and payload kept separate so the payload need never be copied into the send buffer.
- Strict on input, and classified. Malformed packets (bad lengths, reserved bits, invalid UTF-8) and protocol errors (a property or reason code that the specification does not allow in that packet, a misplaced wildcard in a topic filter, a repeated property, a zero packet identifier) are distinguished as the specification distinguishes them, and `Error::reason_code()` gives the DISCONNECT reason to send back. The same rules are applied when encoding, so a badly built packet fails locally instead of at the peer. The few violations the decoder tolerates by default because rejecting them would only lose information (a refused CONNACK that also sets Session Present) can be made errors with `DecodeOptions::STRICT`, for conformance testing and analysis.
- A configurable maximum packet size that rejects an oversized frame as soon as its header is read, before any of its body is buffered.
- `no_std` compatible. Only `alloc` is required; `std` is an optional feature.
- Built on the [`bytes`](https://crates.io/crates/bytes) crate, so buffers interoperate with the rest of the Rust networking ecosystem.

## Using the Crate

To use the library, add this to your application's `Cargo.toml` dependencies list:

    paho-mqtt-codec = "0.1"

By default it enables the `std` feature. For a `no_std` target, disable the default features:

    paho-mqtt-codec = { version = "0.1", default-features = false }

The crate still requires a global allocator in that configuration.

### Build Features

The default behaviour can be altered by enabling or disabling the features:

- _"default"_ - `[std]`
- _"std"_ - Enables the standard library in this crate and its dependencies. Without it the crate builds against `core` and `alloc` only.
- _"serde"_ - Derives `Serialize` and `Deserialize` for the property and enumeration types, so they can be stored or forwarded through `serde`-based formats.

No feature pulls in any I/O or runtime dependency.

### Minimum Supported Rust Version (MSRV)

**v1.89.0**

This package uses Rust Edition 2024, and requires an MSRV of 1.89.0. Although it may build and work with slightly older versions of the compiler, this is the oldest version being tested and maintained by the developers.

## Developing the Crate

The library is a standard Rust "crate" using the _Cargo_ build tool. From this directory, it uses the standard cargo commands for building:

    $ cargo build

Builds the library.

    $ cargo test

Builds and runs the unit tests. The tests are written to pass in both the `std` and `no_std` configurations, so it is worth also running:

    $ cargo test --no-default-features

To confirm the crate really has no dependency on the standard library, build it for a bare-metal target that has none, for example:

    $ rustup target add riscv32imac-unknown-none-elf
    $ cargo build --no-default-features --target riscv32imac-unknown-none-elf

    $ cargo doc

Generates reference documentation.

    $ cargo clippy --no-deps --all-targets -- -D warnings

Runs the linter with warnings treated as errors, as the CI does.

Formatting uses nightly-only options, so format with:

    $ cargo +nightly fmt

### Benchmarks

`benches/codec.rs` is a [Criterion](https://github.com/bheisler/criterion.rs) suite that times encoding and decoding of every packet type for both protocol versions, PUBLISH at payload sizes from 16 bytes to 1 MiB (including the header-only `encode_split` path), and framing of a buffer holding many small packets. Run all of it, or a subset by name:

    $ cargo bench
    $ cargo bench -- decode/publish

To see how a change affects performance, save a baseline before making it and compare afterwards; Criterion reports the difference for each benchmark with a significance estimate:

    $ cargo bench -- --save-baseline before
    ... make changes ...
    $ cargo bench -- --baseline before

Encode benchmarks reuse one output buffer across iterations, as a network runtime would, so they measure the codec rather than the allocator. Decode benchmarks work on a fresh copy of the wire bytes each iteration (the copy is excluded from the timing) because framing consumes its input; freeing that buffer is included, so small-packet decode times carry a fixed allocation cost that is part of any real decode as well.

### Fuzzing

The decoder is the crate's untrusted-input surface: whatever is on the other end of the socket, or anything in between, can put arbitrary bytes on it. The _fuzz_ directory holds two [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) targets:

- `decode` feeds random, coverage-guided bytes through framing and parsing (`RawPacket::decode` then `Packet::from_raw`) for both protocol versions and checks that nothing panics. Anything that decodes successfully is also re-encoded and decoded again, which must reproduce the same packet.
- `roundtrip` is structure-aware: it builds *valid* packets of every type from the random input and checks the encoder's invariants — `encode`, `encode_to_bytes`, and `encode_split` agree byte for byte, `encoded_size` is exact, and decoding gives back an equal packet. It reaches the encoder paths that mutating bytes rarely does.

They need a nightly toolchain and the `cargo-fuzz` tool:

    $ cargo install cargo-fuzz
    $ cargo +nightly fuzz run decode
    $ cargo +nightly fuzz run roundtrip

It runs until stopped or until it finds a failing input, which it saves under _fuzz/artifacts/_ along with the command to reproduce it. A few minutes is enough for a first pass; `-- -max_total_time=300` runs for five minutes. The fuzzer keeps the inputs it finds interesting in _fuzz/corpus/_, so later runs pick up where earlier ones left off. That directory is not committed — it regenerates itself within a minute or two of running — and can be trimmed with `cargo +nightly fuzz cmin decode`. The CI runs both targets for a short, fixed time on every push and pull request.

## Example

A small sample can be found in the _examples_ directory. Here is what encoding a packet and decoding it again looks like:

```rust
use bytes::{Bytes, BytesMut};
use paho_mqtt_codec::{
    MqttVersion, Packet, Property, PropertyId, PropertyValue, Publish, QoS, RawPacket,
    Result,
};

fn main() -> Result<()> {
    let ver = MqttVersion::V5;

    // Build a QoS 1 PUBLISH carrying an MQTT v5 property.
    let mut publish = Publish::new("sensors/temp", Bytes::from_static(b"21.5"));
    publish.qos = QoS::AtLeastOnce;
    publish.packet_id = Some(42);
    publish.properties.push(Property::new(
        PropertyId::MessageExpiryInterval,
        PropertyValue::U32(60),
    ));

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
```

Running it prints the 28 bytes of the encoded packet followed by the decoded fields:

    $ cargo run --example round_trip
    encoded 28 bytes: [32, 1a, 00, 0c, 73, 65, 6e, 73, 6f, 72, 73, 2f, 74, 65, 6d, 70, 00, 2a, 05, 02, 00, 00, 00, 3c, 32, 31, 2e, 35]
    PUBLISH id=Some(42) qos=1 topic="sensors/temp" payload=b"21.5" props=1

For sending, `Packet::encode_split` does the same as `encode` but returns the PUBLISH payload as a separate `Bytes` instead of copying it into the buffer, so the header and payload can be handed to a vectored write.

## Using it in a server

Nothing in the crate assumes which side of the connection it is on. A server decodes the same way a client does, with one wrinkle: the protocol version is not known until the first packet arrives. CONNECT is self-describing, so decode it without a version and read the one it declares:

```rust
let raw = RawPacket::decode(&mut read_buf)?.expect("complete packet");
let ver = match Packet::from_raw(raw, MqttVersion::V5)? {   // version is ignored for CONNECT
    Packet::Connect(c) => c.version,                           // use this for the rest of the session
    _ => return Err(/* first packet must be CONNECT */),
};
```

A CONNECT whose protocol name and level do not name a version the crate speaks decodes to `Error::UnsupportedProtocolVersion`, distinct from a malformed packet, so a server can answer with a CONNACK carrying `UnsupportedProtocolVersion` rather than dropping the connection. (MQTT v3.1 clients — protocol name `MQIsdp`, level 3 — are supported and decode to `MqttVersion::V3_1`.) `RawPacket::decode_max` takes the server's own maximum packet size for the same reason a client would pass its own.

`RawPacket::packet_type()` gives the type before the body is parsed, for routing or for rejecting a packet the session state does not permit. When a decode fails, `Error::reason_code()` is the code for the DISCONNECT (or CONNACK) to send before closing. The `topic` module exposes the filter and name validation the decoder uses — including `topic::split_shared` for `$share/{name}/{filter}` subscriptions — so a server can apply the same rules to its own subscription table.

## Relationship to the Paho Client

Version 2 of the [`paho-mqtt`](https://crates.io/crates/paho-mqtt) crate uses this codec for all of its wire I/O and re-exports the shared types — `QoS`, `MqttVersion`, `ReasonCode`, and the property types — so applications using that client don't normally depend on this crate directly. (Version 1 of the Paho Rust client wraps the Paho C library and uses it for all wire I/O, not this crate.)

The codec is developed in the Paho repository but is not tied to the Paho client: any client library, broker, or tool can use it alone.
