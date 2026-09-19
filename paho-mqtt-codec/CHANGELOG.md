# Change Log
#
# paho-mqtt-codec: MQTT packet codec for Rust.
#

All notable changes to this crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] - v0.1.0

Initial release. A pure-Rust, `no_std`-compatible encoder and decoder for every MQTT control packet in protocol versions 3.1, 3.1.1, and 5.0, for use by any MQTT client library, broker, proxy, or tool. Developed alongside, and used by, the Eclipse Paho MQTT Rust client v2, but with no dependency on it.

### Design

- One struct per packet type, shared across protocol versions; v5-only fields (reason codes, properties) are present everywhere and simply not written to the wire on a v3 connection. `Connect` carries its own `version` and is decoded without knowing it in advance, as a server must.
- Two-phase decoding: `RawPacket::decode` frames one packet out of a `BytesMut` read buffer (returning `None` until it is complete) and `Packet::from_raw` parses it for the negotiated version. `RawPacket::decode_max` enforces a maximum packet size before the body is buffered. `RawPacket::packet_type()` gives the type before parsing.
- Zero-copy where the protocol allows: decoded PUBLISH payloads are slices of the receive buffer; `Packet::encode_split` returns the payload separately so it can be written with a vectored write instead of copied. `Packet::encode` reserves its exact output size once and leaves the buffer untouched on error.
- Strict decoding, classified as the specification classifies it: `Error::MalformedPacket` (cannot be parsed: lengths, reserved bits, invalid UTF-8, unknown types, non-minimal variable-byte integers) versus `Error::ProtocolError` (parses but breaks a rule: packet ID 0, DUP on QoS 0, a property out of range, repeated, or not allowed in that packet, a reason code not allowed in that packet, empty subscription or acknowledgement lists), plus `TopicNameInvalid`, `TopicFilterInvalid`, `PacketTooLarge`, `UnsupportedProtocolVersion`. `Error::reason_code()` gives the DISCONNECT (or CONNACK) reason code to send back.
- The same rules apply to encoding, so a badly built packet fails locally rather than at the peer: string and binary fields over 65 535 bytes, U+0000 in strings, wildcards in a topic name, misplaced wildcards or a malformed `$share/…` prefix in a topic filter, and properties that are invalid for their packet. A packet identifier of zero, an empty SUBSCRIBE/UNSUBSCRIBE filter list, an empty SUBACK/UNSUBACK code list, and a repeated Subscription Identifier outside a PUBLISH are likewise rejected on both encode and decode.
- A packet that a v3 connection cannot express is refused rather than truncated: non-empty properties, reason codes in an UNSUBACK, or a SUBACK return code outside the four v3 values (0x00–0x02, 0x80) fail to encode for v3, and the last is also a Malformed Packet on decode. A QoS 0 PUBLISH with a packet identifier set is likewise refused.
- Every packet must account for exactly its remaining length: bytes left after the last field — a PINGREQ with a body, a v3 DISCONNECT with a payload, a CONNACK longer than its fields — are a Malformed Packet rather than silently discarded.
- `Properties` validates on `push` (value range, wire type, repeated non-repeatable identifiers) and remembers the outcome, so encoding costs one check rather than one per property; it caches its wire size. `PropertyId::is_allowed_in`, `ReasonCode::is_allowed_in`, and `PropertyContext` expose the specification's per-packet tables.
- The `topic` module exposes the name and filter validation the codec uses, including `split_shared` for shared subscriptions.
- Lenient in one place by default, deliberately: a CONNACK that refuses the connection but sets Session Present decodes with the flag cleared and the reason code kept, since the reason code is what the receiver needs. `DecodeOptions::STRICT` (via `Packet::from_raw_with` / `ConnAck::decode_with`) reports it as a `ProtocolError` instead, for conformance testers and analyzers; the struct is `#[non_exhaustive]` so further strictness controls can be added.
- `no_std` with `alloc`; the `std` feature is opt-in and forwards to dependencies. The `serde` feature derives `Serialize`/`Deserialize` for every public data type. Verified in CI on a bare-metal target.
- Criterion benchmarks covering encode and decode of every packet type, and two `cargo-fuzz` targets: byte-level (`decode`) and structure-aware (`roundtrip`, which builds valid packets and checks encode → decode is the identity).
