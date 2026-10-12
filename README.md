# Eclipse Paho MQTT Rust Client Library

[![docs.rs](https://docs.rs/paho-mqtt/badge.svg)](https://docs.rs/paho-mqtt) [![crates.io](https://img.shields.io/crates/d/paho-mqtt?label=crates.io%20downloads)](https://crates.io/crates/paho-mqtt) [![GitHub contributors](https://img.shields.io/github/contributors/eclipse/paho.mqtt.rust?label=GitHub%20contributors)](https://github.com/eclipse/paho.mqtt.rust)

The [Eclipse Paho](http://eclipse.org/paho) MQTT client library for the Rust language, for memory-managed operating systems such as Linux, macOS, and Windows.

**Version 2.0 is a pure-Rust rewrite.** The library no longer wraps the Paho C library: it implements MQTT itself, so building it needs no C compiler, no CMake, and no OpenSSL development package — just Cargo. The public API is kept as close to v1.x as the rewrite allows; see [Migrating from v1.x](#migrating-from-v1x) for what changed.

> Version 1.x remains available and is documented on the `master` branch. It wraps the Paho C library and is a different crate lineage from what follows here.

## Features

- MQTT v5, v3.1.1, and v3.1, selectable per client
- QoS 0, 1, and 2, with full session state and re-delivery
- Transports: TCP, TLS, WebSocket, secure WebSocket, Unix-domain sockets, and tunnelling through an HTTP proxy
- Two runtimes to choose from: [tokio](https://tokio.rs) (default) or [smol](https://github.com/smol-rs/smol) — with the same public API either way
- Three ways to use it: `async`/`.await`, the token/wait style, or a fully blocking client
- Last Will and Testament, retained messages, automatic reconnect, offline buffering, and a high-availability server list
- MQTT v5 throughout: properties, reason codes, topic aliases, subscription options, shared subscriptions, and enhanced authentication (AUTH)
- Message persistence: built-in file or in-memory stores, or your own through the `ClientPersistence` trait

## Quick Start

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
paho-mqtt = "2.0"
```

### Asynchronous

```rust
use paho_mqtt as mqtt;
use std::time::Duration;

#[tokio::main]
async fn main() -> mqtt::Result<()> {
    let mut cli = mqtt::AsyncClient::new("mqtt://localhost:1883")?;

    // Take the message stream before connecting, so nothing is missed.
    let strm = cli.get_stream(25);

    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .keep_alive_interval(Duration::from_secs(20))
        .clean_start(true)
        .finalize();

    cli.connect(conn_opts).await?;
    cli.subscribe("test/#", mqtt::QOS_1).await?;

    cli.publish(mqtt::Message::new("test/hello", "Hello world!", mqtt::QOS_1))
        .await?;

    // `None` arrives if the connection drops; the client reconnects if
    // configured to, and the stream continues.
    while let Ok(Some(msg)) = strm.recv().await {
        println!("{}: {}", msg.topic(), msg.payload_str());
    }

    cli.disconnect(None).await?;
    Ok(())
}
```

### Blocking

```rust
use paho_mqtt as mqtt;

fn main() -> mqtt::Result<()> {
    let cli = mqtt::Client::new("mqtt://localhost:1883")?;

    cli.connect(None)?;
    cli.publish(mqtt::Message::new("test/hello", "Hello world!", mqtt::QOS_1))?;
    cli.disconnect(None)?;
    Ok(())
}
```

`Client` runs the protocol on the library's own runtime, so its calls can block the caller safely. Inside async code, prefer `AsyncClient`.

### Tokens

Every `AsyncClient` operation returns a `Token<T>`, which is a `Future` that has **already started** — the operation is under way whether or not you `.await` it. The same token can be awaited, waited on synchronously, or polled:

```rust
let tok = cli.publish(msg);   // already on its way
// ... do something else ...
tok.wait()?;                  // or: tok.await?;  or: tok.try_wait()
```

## Transports

The URI scheme selects the transport:

| Scheme | Transport | Needs feature |
|---|---|---|
| `mqtt://`, `tcp://` | TCP (default port 1883) | — |
| `mqtts://`, `ssl://`, `tls://` | TLS (default port 8883) | `tls` or `openssl` (`smol-tls`) |
| `ws://` | WebSocket | `websocket` (`smol-websocket`) |
| `wss://` | WebSocket over TLS | `websocket` + `tls`/`openssl` (`smol-websocket` + `smol-tls`) |
| `unix://` | Unix-domain socket (\*nix only) | — |

A URI naming a transport the build does not support is refused when connecting, rather than quietly falling back to a different one.

`ws://` and `wss://` can be tunnelled through an HTTP proxy with `ConnectOptionsBuilder::http_proxy` / `https_proxy`, which accept `http://proxy:3128` or `http://user:pass@proxy:3128`.

## Build Features

Default: `tokio`.

### Tokio backend (default)

| Feature | Description |
|---|---|
| `tokio` *(default)* | Tokio-based internal async runtime |
| `tls` | TLS via [rustls](https://docs.rs/rustls); implies `tokio` |
| `openssl` | TLS via the system OpenSSL; implies `tokio` |
| `openssl-vendored` | As `openssl`, but builds OpenSSL from source; implies `openssl` |
| `websocket` | WebSocket transport; implies `tokio` |
| `serde` | `Serialize`/`Deserialize` for `Message`, the response types, the options types, and the codec's data types (properties, reason codes, QoS, versions) |

Exactly one backend at a time: enabling both `tokio` and `smol` is a compile error, since an application could not tell which runtime it was talking to.

With `--no-default-features` there is no backend and so no client: `AsyncClient`, `Client`, `Topic` and `SyncTopic` are absent, along with `CreateOptionsBuilder::create_async_client`. Everything that does not need a client remains — the codec types, `Message`, `Properties`, `TopicMatcher`, `TopicFilter`, `Token`, the response and options types, persistence and `Error` — which is the build for a tool that handles MQTT data without connecting, or a crate that reads and writes client configuration for an application that links the runtime itself.

### Smol backend

| Feature | Description |
|---|---|
| `smol` | Smol-based internal async runtime |
| `smol-tls` | TLS via rustls; implies `smol` |
| `smol-websocket` | WebSocket transport; implies `smol` |

Select the smol backend by turning the default off:

```toml
paho-mqtt = { version = "2.0", default-features = false, features = ["smol", "smol-tls"] }
```

OpenSSL is tokio-only; use `smol-tls` for TLS with smol.

## Runtimes

The library runs the MQTT protocol on an internal async task, and `CreateOptions::runtime_mode` decides where that task lives:

- **`Auto`** *(default)* — inside an async context, borrow the caller's runtime; outside one, use a process-wide runtime the library creates and shares.
- **`Shared(n)`** — always the library's own runtime, with `n` worker threads. One worker suits most applications; two can lower tail latency under a sustained heavy publishing load, at the cost of a thread and some CPU.
- **`ApplicationRuntime`** — borrow the caller's runtime, and fail loudly if there is none. (Tokio only; smol has no ambient executor to borrow.)

No public API takes or returns an `async fn`, and tokens are plain futures, so the crate works with any executor regardless of which backend drives its I/O.

One caveat worth knowing: with `Auto` inside an application runtime, *blocking* on a token from that same runtime can stall the thread the protocol needs. Inside async code, `.await` the token. `Client`, whose whole API blocks, avoids the question by using the library's own runtime.

## TLS

TLS is provided by [rustls](https://docs.rs/rustls) by default — pure Rust, nothing to install:

```rust
let tls = mqtt::TlsOptionsBuilder::new()
    .trust_store("/path/to/ca.crt")
    .finalize();

let conn_opts = mqtt::ConnectOptionsBuilder::new()
    .tls_options(tls)
    .finalize();
```

`TlsOptions` covers the trust store (a file or a directory of PEMs), client certificates for mutual TLS, ALPN protocols, the cipher-suite list, and the minimum protocol version. Two switches control verification, and they are independent:

- `enable_server_cert_auth` — whether the certificate chain is verified at all. Leave it on.
- `verify` — whether the certificate must name the host you connected to. Turning it off keeps the chain check and waives only the identity check, for a broker reached by IP address or through a tunnel.

The `openssl` feature swaps in the system OpenSSL with the same `TlsOptions` API. It exists for environments that require a FIPS-validated or organisation-supplied TLS stack. Password-protected private keys work only with OpenSSL; rustls cannot decrypt them.

## MQTT v5

Version 5 is the default protocol level. Properties travel on every packet that allows them:

```rust
let mut props = mqtt::Properties::new();
props.push(mqtt::Property::SessionExpiryInterval(3600));

let conn_opts = mqtt::ConnectOptionsBuilder::new_v5()
    .clean_start(false)
    .properties(props)
    .finalize();
```

The client honours what the broker says in its CONNACK — Receive Maximum, Server Keep Alive, Assigned Client Identifier, Maximum Packet Size, and Topic Alias Maximum — and `ConnectResponse` exposes all of it. Enhanced authentication is supported through `ConnectOptionsBuilder::auth_handler`; see the `paho_mqtt::auth` module.

To connect with an older protocol level, use `ConnectOptionsBuilder::new_v3()` or set `mqtt_version` explicitly.

## Persistence

By default the client keeps in-flight QoS 1 and QoS 2 messages in files next to the working directory, so they survive a crash. The store is selectable:

```rust
let cli = mqtt::CreateOptionsBuilder::new()
    .server_uri("mqtt://localhost:1883")
    .persistence(mqtt::PersistenceType::File)   // or MemPersistence, or None
    .create_async_client()?;
```

A custom store — a database, a key/value service, an embedded flash region — implements the `ClientPersistence` trait and is passed to `CreateOptionsBuilder::user_persistence`.

In-flight messages survive a dropped connection whether or not a store is configured: the client carries them across the reconnect and re-sends them when the broker resumes the session. Persistence is what carries them across a *process*.

## Minimum Supported Rust Version (MSRV)

**Rust 1.89.0**, Edition 2024. Older compilers may work but are not tested.

## Repository Layout

Three crates live here, released independently:

| Crate | What it is |
|---|---|
| `paho-mqtt` (root) | The client library — this README |
| [`paho-mqtt-codec`](paho-mqtt-codec/) | The MQTT packet encoder/decoder: `no_std`, no I/O, usable by any client, broker, or tool |
| [`paho-mqtt-sys`](paho-mqtt-sys/) | Unsafe bindings to the Paho **C** library, used by v1.x and maintained for its users. Version 2 does not use it |

## Migrating from v1.x

Most application code needs little or no change. The client types, builders, tokens, and callbacks keep their shapes. What to look out for:

**Build changes**

- The `bundled`, `build_bindgen`, `ssl`, and `vendored-ssl` features are gone, along with the C toolchain they needed. TLS is now `tls` (rustls) or `openssl`.
- MSRV is 1.89 and the edition is 2024.

**API changes**

- `QOS_0`/`QOS_1`/`QOS_2` are `QoS` values and `MQTT_VERSION_*` are `MqttVersion` values, rather than integers. Code passing the named constants is unaffected; code passing integers needs `QoS::try_from(n)?` or `MqttVersion::try_from(n)?`. The old lossy conversions, which turned an invalid number into a default, are gone.
- `Properties`, `Property`, `PropertyId`, `PropertyType`, `ReasonCode`, `QoS`, and `MqttVersion` are re-exported from `paho-mqtt-codec` and documented there.
- Per-operation success/failure callbacks are removed. Await the token, or use the connection-state callbacks and the event stream.
- An acknowledgement carrying an error reason code now fails the operation's token with `Error::ReasonCode`, for `publish`, `subscribe`, and `unsubscribe`. The `*_many` calls still return `Ok` with one code per topic, since they can succeed partially.
- `client_id()` returns `String` rather than `&str`, so it can report a broker-assigned identifier.
- `Error::Publish` holds a `Box<Message>`.

The `[Breaking]` entries in the [CHANGELOG](CHANGELOG.md) list these in full.

## Logging

The library logs through the [`log`](https://docs.rs/log) crate under the target `paho_mqtt`. The examples use `env_logger`, configured by the `RUST_LOG` environment variable:

```
$ RUST_LOG=debug cargo run --example async_publish
DEBUG paho_mqtt::async_client > Create options: CreateOptions { ... }
DEBUG paho_mqtt::async_client > Connecting with opts: ...
```

Since the protocol is implemented in Rust, everything on the wire is visible at `trace` level — there is no separate C-library trace to enable.

## Examples

The `examples/` directory has small programs for most of what the library does: `async_publish`, `async_subscribe`, `sync_publish`, `sync_consume`, `topic_publish`, `topic_alias_publish`, `ws_publish`, `tls_publish`, `async_persist_publish`, a request/response pair in `rpc_math_cli`/`rpc_math_srvr`, and runtime-specific variants prefixed `tokio_` and `smol_`.

```
$ cargo run --example async_subscribe
$ cargo run --no-default-features --features smol --example smol_publish
```

## Developing the Crate

```bash
cargo build                                            # build
cargo test                                             # unit tests (no broker needed)
cargo fmt --all                                        # format (stable toolchain)
cargo clippy --no-deps --all-targets -- -D warnings    # lints
cargo doc --no-deps                                    # docs
```

`./buildtst.sh` runs what CI runs: the feature matrix on stable and the MSRV, clippy, docs, the codec's `no_std` build, and the benchmark compile. Add `--integration`, `--fuzz`, or `--bench` for the slower checks.

The integration tests need a Mosquitto broker on localhost:

```
listener 1883
allow_anonymous true
socket_domain ipv4

# Unix-domain socket
listener 0
socket_domain unix
bind_address /tmp/mosquitto.sock

# WebSocket
listener 9001
protocol websockets
allow_anonymous true
```

```bash
cargo test --test integration --features tls,websocket
```

Tests that need behaviour a stock broker will not produce — a refused subscription, an imposed keep-alive, enhanced authentication, a session the broker declines to resume, a TLS certificate naming the wrong host, an HTTP proxy — run against scripted servers built on `paho-mqtt-codec` (see `tests/common/`). The TLS tests generate their own certificates, so they need no broker configuration.

## External Libraries and Utilities

### Redis Persistence

The `mqtt-redis` crate uses Redis as a persistence store, and is a worked example of implementing `ClientPersistence`:

https://github.com/fpagliughi/mqtt.rust.redis

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Bugs and feature requests go to [GitHub Issues](https://github.com/eclipse/paho.mqtt.rust/issues).
