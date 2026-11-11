# Change Log
#
# Eclipse Paho MQTT Rust library.
#

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] - v2.0.0

Version 2.0 is a rewrite of the library in pure Rust. It no longer depends on the Paho C library or on the `paho-mqtt-sys` FFI crate; the MQTT protocol is implemented by a new, separately published codec crate, `paho-mqtt-codec`, which has its own [CHANGELOG](paho-mqtt-codec/CHANGELOG.md). (`paho-mqtt-sys` itself continues to be maintained and released for v1.x users.) The public API is kept as close to v1.x as the rewrite allows; the items marked **[Breaking]** below are the places application code may need to change.

### Architecture

- Pure Rust; no C compiler, CMake, or OpenSSL build step. The `bundled`, `build_bindgen`, `ssl`, and `vendored-ssl` features no longer exist.
- Selectable internal async runtime: `tokio` (default) or `smol`. The public API is unchanged by the choice and contains no `async fn`; `AsyncClient` still returns eagerly started `Token<T>` futures and `Client` still blocks on them.
- Transports: TCP, Unix-domain sockets, TLS via rustls (`tls` / `smol-tls`) or OpenSSL (`openssl`, `openssl-vendored`; tokio only), WebSocket and WebSocket-over-TLS (`websocket` / `smol-websocket`).
- **[Breaking]** MSRV is Rust 1.89; edition 2024.
- **[Breaking]** Per-operation success/failure callbacks are removed. The connection-state callbacks (`connected`, `connection_lost`, `disconnected`) and the message callback remain, as do the sync and async event streams.

### Protocol

- MQTT v3.1 (`MQIsdp`, protocol level 3) is supported alongside v3.1.1 and v5: `MqttVersion::V3_1`, `MQTT_VERSION_3_1`, `ConnectOptions::with_mqtt_version(MQTT_VERSION_3_1)`.
- The client advertises and enforces a maximum inbound packet size (v5 `MaximumPacketSize` in the connect properties), rejecting an oversized packet at the framing layer before its body is buffered.
- The client honours the server's `MaximumPacketSize` from CONNACK: a publish that would exceed it fails its own token with `Error::Protocol(PacketTooLarge)` instead of being sent and causing the server to disconnect. A publish that cannot be encoded (for example, an invalid topic) likewise fails only its own token, leaving the connection up.
- **A broker-initiated DISCONNECT (v5) is reported as `disconnected`, not as `connection_lost`.** The two are alternatives: `connection_lost` means the connection was lost *unexpectedly*, while `disconnected` means the server closed it and said why. Every established connection now ends with exactly one of them, never both and never neither. The message routes are told either way — a message callback, stream or consumer gets its `None` from whichever one fires, since it cannot carry a reason and only needs to know the connection is over. Auto-reconnect runs after both. This is also what v1.x did: the Paho C library calls its `disconnected` callback for the DISCONNECT and skips `connectionLost`, and the v1.x wrapper pushed the `None` from there. **The case to check in an upgrade is a session taken over**: when another client connects with the same identifier, a v5 broker announces it with `SessionTakenOver`, so the notice arrives on `disconnected` and not on `connection_lost`. On a v3 connection, which has no server DISCONNECT, the same takeover still arrives as `connection_lost`.
- Packets a broker sends right behind its CONNACK — the queued messages of a session it has just resumed, say — are no longer lost when they arrive in the same read as the CONNACK; they are handled before the client waits on the connection again.
- Large publish payloads (over 16 KiB) are written to the transport from their own buffer, without being copied into the send buffer; QoS 1/2 messages are encoded once for both persistence and transmission.

### MQTT v5

- The client now honours the properties the server sends in its CONNACK. **Receive Maximum** caps the in-flight window (narrowed further by `max_inflight`), **Server Keep Alive** replaces the requested keep-alive, **Assigned Client Identifier** becomes the client's identity — reported by `client_id()` and used by later reconnects — **Maximum Packet Size** and **Topic Alias Maximum** bound what is sent, and **Maximum QoS** and **Retain Available** are enforced: a publish above the server's Maximum QoS, or retained when it has no retained messages, fails its token with `Error::ReasonCode(QosNotSupported)` or `(RetainNotSupported)` without being sent (it is not downgraded), where it used to go out and earn a DISCONNECT. A message in flight is held to the limits of the server a resumed session continues on.
- **Topic aliases** work in both directions. An inbound PUBLISH carrying an alias is delivered with the topic name the alias stands for; an outbound alias outside the server's range fails that publish with `Error::ReasonCode(TopicAliasInvalid)` instead of provoking a disconnect, as does a publish with only an alias that the client has never sent along with a topic name. An outbound alias lasts one connection, as the specification says, and the client handles that itself: a message re-sent after a reconnect, or restored from the store, goes out with its topic name and no alias, and a publish with only an alias that was set up on an earlier connection — as `Topic` makes after a reconnect — goes out with the topic name, setting the alias up again.
- **Enhanced authentication** (AUTH) is supported: name an `AuthenticationMethod` in the connect properties and supply `ConnectOptionsBuilder::auth_handler`, and the client answers the broker's challenges until it accepts or refuses. See the new `paho_mqtt::auth` module. Client-initiated re-authentication is not implemented.
- When a protocol fault ends a v5 session, the client sends a DISCONNECT carrying the reason code before closing, rather than dropping the socket silently.
- `ConnectResponse` gained accessors for everything the CONNACK can report: `receive_maximum()`, `maximum_packet_size()`, `topic_alias_maximum()`, `maximum_qos()`, `retain_available()`, `wildcard_subscriptions_available()`, `subscription_identifiers_available()`, `shared_subscriptions_available()`, `server_keep_alive()`, `assigned_client_id()`, `server_reference()`, and `reason_string()`. Each falls back to the specification's default when the property is absent.
- `AsyncClient::client_id()` and `Client::client_id()` return `String`, as in v0.14, and now report a server-assigned identifier: after a v5 connect with an empty client ID, they give the identifier the server chose.

### TLS, proxies, and runtime

- **`TlsOptions` verifies the broker by default**, as v1.x did (`enableServerCertAuth = 1`): `TlsOptions::default()`, `TlsOptions::new()` and `TlsOptionsBuilder::new()` set both `enable_server_cert_auth` and `verify` to `true`. Turning either off is a deliberate, explicit act.
- `TlsOptions::verify` is implemented: it controls whether the broker's certificate must name the host connected to, independently of `enable_server_cert_auth`, which controls whether the chain is checked at all. Turning it off keeps full chain verification and waives only the identity check — for a broker reached by IP address, through a tunnel, or on a test rig.
- `ConnectOptions::http_proxy` and `https_proxy` are implemented: a `ws://` or `wss://` connection is tunnelled through the named HTTP proxy with a `CONNECT` request, including `Proxy-Authorization` when the URI carries credentials. A proxy that refuses reports its status line to the caller.
- The smol backend honours `RuntimeMode`: `Shared(n)` runs the shared executor on `n` threads. `ApplicationRuntime` is refused there by `AsyncClient::new` with `Error::InvalidArgument` and an explanation, as smol has no ambient executor to borrow. With tokio it is refused the same way when the client is created outside a Tokio runtime. Neither panics, and neither does a failure to start the shared runtime or executor threads, which is `Error::Io`.
- **`Client` (the blocking API) now uses the library's own runtime** when left on `RuntimeMode::Auto`, instead of borrowing the caller's. Blocking on a token from the runtime that is driving the event loop could otherwise deadlock. An explicitly chosen mode is unchanged.
- Dropping the last client that uses the library's own tokio runtime no longer panics when it happens inside async code ("Cannot drop a runtime in a context where blocking is not allowed"). That runtime is now shut down in the background in that case, and dropped as before — waiting for its threads — anywhere else.
- `is_topic_filter_valid` applies the codec's rules, including shared subscriptions (`$share/{name}/{filter}`). The same rules are re-exported as `paho_mqtt::topic_rules`, so an application can check a filter before subscribing.
- Pre-shared keys (v1.x `ssl_psk`) are not supported; rustls, the default TLS backend, has no external-PSK support.
- TLS failures are reported as `Error::Tls` whatever their source. Errors building the TLS configuration from `TlsOptions` — a certificate or key the backend rejects, no recognised cipher suite, an encrypted key with rustls, an invalid server name, and every OpenSSL setup error — used to come back as `Error::General` / `Error::GeneralString`, and so never reached the TLS error callback; they now fail the connect with `Error::Tls` and are reported to `on_tls_error` like a refused handshake. A certificate or key file that cannot be opened or read is still `Error::Io`.
- `Error::Tls` carries a `TlsError`, which classifies the failure in the same terms for rustls and OpenSSL. `TlsError::kind()` returns a `TlsErrorKind` — `UntrustedCertificate`, `HostnameMismatch`, `CertificateExpired`, `CertificateNotYetValid`, `HandshakeFailed`, `InvalidServerName`, `Config`, or `Unsupported` — and `Display` and `message()` give the backend's own description. The `on_tls_error` callback receives the same `&TlsError`, so a multi-server connect can see what went wrong on each server, where the connect token has only the last server's error.
- The TLS error callback (`TlsOptions::on_tls_error`) is called for every server whose TLS handshake fails during a connect to a list of servers. It used to be called only when the *last* server tried failed that way, so a handshake failure on any earlier server went unreported.

### Sessions and persistence

- In-flight QoS 1/2 messages now belong to the session rather than to one connection: they survive a dropped connection and are re-sent, in publish order and with DUP set, on the reconnect that resumes the session — with or without a persistence store. Previously every reconnect dropped them and failed their tokens. That holds for a reconnect the application makes, too, after a `disconnect()` or a loss without auto-reconnect: the publish tokens stay pending until the next `connect()` answers them (or fails them with `Error::SessionNotResumed` if the server did not keep the session), and inbound QoS 2 messages awaiting PUBREL are delivered when it arrives. A session that ends with its connection — a v3 clean session, or a v5 session whose Session Expiry Interval is 0 (the default) when the connection closes, as set by the CONNECT, the server's CONNACK, or the client's DISCONNECT — takes its state with it: in-flight publishes fail with `Error::SessionNotResumed` as the connection closes, and nothing of it is re-sent on a later connection, as v3.1.1 §3.1.2.4 and v5 §3.1.2.11.2 require.
- A QoS 2 message that the server has taken delivery of (PUBREC) is completed by re-sending the **PUBREL**, never the message. Its persisted entry is replaced with the PUBREL at that moment, so a crash cannot resurrect a message the server already holds.
- The CONNACK's Session Present flag is honoured. When the server did not resume the session — and a clean start never resumes one, whatever the server says — the client discards its half of it, as the specification requires (§3.1.2.4; v5 §3.2.2.2): outstanding QoS 1 and QoS 2 publishes fail with the new `Error::SessionNotResumed`, rather than being re-sent into a session the server says does not exist or hanging on an acknowledgement that cannot come, and inbound QoS 2 state is dropped. Whether such a message arrived is unknown; publish it again if it must be delivered. This is the same with or without a persistence store.
- A `subscribe()` or `unsubscribe()` still waiting for its acknowledgement when the connection drops is sent again once auto-reconnect succeeds, whether or not the server resumed the session, and its token resolves with the new connection's answer. It used to fail with `Error::Disconnected`; v1.x did not re-send it either. Both are safe to repeat: the server replaces an existing subscription to the same filter, and acknowledges an unsubscribe that matches nothing (v5 reason code `NoSubscriptionFound`). If the server had already processed the first one, matching retained messages may be delivered a second time. A `disconnect()`, or a lost connection without auto-reconnect, still fails it.
- Connecting with `clean_start` clears the persistence store, instead of restoring state for a session the server has already discarded.
- Restoring a persistence store says what it could not restore. Each entry that cannot be used — a key this client does not write (such as a v1.x store's), an unreadable value, a corrupt or wrong-type packet, a missing or mismatched packet identifier — is logged as a warning with its reason and skipped, and so is any other file in a `FilePersistence` directory, whatever its name. A summary then says how many messages were restored and how many lost. A store that cannot be listed fails the connect with `Error::Persistence`, before anything is sent to the broker, rather than looking like an empty one; its messages are still there once the fault is fixed. To connect without them, turn off `restore_messages` or use a clean start, neither of which reads the store. An inbound QoS 2 entry with no packet identifier is now refused; it used to be restored under the invalid identifier 0.
- `FilePersistence` names each client's store directory `<client_id>-<server_uri>-<hash>`: the two made safe for a path and cut short if long, then a hash of both exactly as given. Two clients whose IDs and URIs differed only in characters replaced to make the name safe, or in where a `-` fell between the two, used to share a store, restoring and publishing each other's messages and clearing each other's store on a clean start; and a long client ID made the connect fail. The store is opened under the URI the client was created with, so a client with several servers keeps one store for all of them.
- A `ClientPersistence` that panics no longer brings the client down. The call fails with `Error::Persistence` carrying the panic's message — an outbound message that could not be stored is logged and sent anyway, as for any store error — and the store goes on being used, its lock recovered if the panic poisoned it. A store that hangs holds up a connection attempt for no longer than `persist_timeout`, after which the attempt fails with `Error::Persistence`.
- New `MemPersistence`, a `HashMap`-backed `ClientPersistence` for tests and for applications that want the bookkeeping without touching a filesystem.
- `CreateOptions::persist_qos0` is documented: it decides whether a QoS 0 publish made while disconnected is queued for the next connection or dropped. QoS 0 messages are never written to a persistence store.

### Behaviour

- A `subscribe()` or `unsubscribe()` the client cannot send fails its own token and leaves the connection up. A topic filter that isn't valid (`a/#/b`, `a+`, an empty string) fails with `Error::BadTopicFilter`, and an empty list, or a property the packet cannot carry or an invalid value, with `Error::InvalidArgument`, all before anything is queued; a request over the server's `MaximumPacketSize` fails with `Error::Protocol(PacketTooLarge)`, as a publish already did. Each of these used to end the connection, with a v5 DISCONNECT telling the broker that *it* had broken the protocol, and fail the token with `Error::Disconnected`. More generally, a session that ends because the client cannot send a packet of its own now says "unspecified error" (0x80) in its DISCONNECT, rather than a reason code that blames the server. Likewise, a message or (un)subscribe re-sent on reconnecting that is too large for the new server now fails alone; it used to fail every attempt to resume the session.
- **[Breaking]** An acknowledgement carrying an error reason code fails the operation's token with `Error::ReasonCode(code, properties)` — for `publish()` (PUBACK or PUBREC/PUBCOMP), `subscribe()`, and `unsubscribe()` — matching how a refused CONNACK fails `connect()`. `subscribe_many()` and `unsubscribe_many()` still resolve `Ok` with one code per filter, since they may succeed partially. Previously PUBACK, SUBACK and UNSUBACK errors resolved `Ok` with the code inside the response while PUBREC errors failed the token.
- `ConnectOptions::connect_timeout` is honoured (default 30 s) and bounds the whole attempt — TCP connect, TLS/WebSocket handshake, CONNECT and CONNACK — failing the token with `Error::Timeout`. Zero disables it. `disconnect()` called during a pending connect cancels the attempt at once; a second `connect()` while one is in progress fails with `Error::ConnectInProgress`.
- **[Breaking]** The timing options are used as given, where v1.x clamped them. `keep_alive_interval`, `connect_timeout` and the disconnect `timeout` all accept `Duration::ZERO`, which **disables** the keep-alive or the timeout — v1.x clamped zero to 1 s (1 ms for disconnect) and so gave no way to switch either off. The two timeouts are the client's own timers, so sub-second values work; the keep-alive travels as a `u16` of seconds, so it is rounded, with a non-zero interval under a second becoming 1 s rather than 0, which would have switched keep-alive off instead of speeding it up.
- Auto-reconnect backs off correctly from a zero `min_retry_interval`. The delay doubled on each failure and was capped at `max_retry_interval`, but doubling zero is zero, so `automatic_reconnect(Duration::ZERO, ..)` retried with no pause at all for as long as the broker stayed down. A zero minimum still means the first retry waits for nothing; the backoff then starts at 100 ms and doubles from there.
- **[Breaking]** `automatic_reconnect` with a `max_retry_interval` below `min_retry_interval` is refused by `ConnectOptions::check()`, and so fails the connect token: the delay would shrink on the first failure, and there is no sensible way to guess which value was meant. Equal values are accepted, as a fixed retry interval with no backoff.
- `Token<T>` is now a proper shared handle: every clone resolves with the same result, `try_wait()` no longer consumes it, and a completed token can be awaited again. `Error` implements `Clone` to make this possible (an `io::Error` is cloned as a new error of the same kind and message).
- A Will's MQTT v5 properties (Will Delay Interval, Message Expiry, Content Type, user properties, …) are sent; they were dropped. Setting them on a v3 connection is `Error::WrongMqttVersion`.
- `ws://` and `wss://` URIs are refused with `Error::WebSocketNotSupported` when the client is built without a WebSocket feature, rather than being silently treated as plain TCP or TLS.
- A failure while restoring persisted messages on (re)connect is now reported as a lost connection (`connection_lost` callback, auto-reconnect if enabled) instead of leaving the client reporting itself connected with no session.
- Persisted QoS 1/2 messages re-sent on connect respect `max_inflight`, queuing behind the limit like new publishes.
- With all 65 535 packet identifiers in use, a QoS 1/2 publish, `subscribe()` or `unsubscribe()` now waits until an acknowledgement frees one. Previously the event loop could search for a free identifier forever. Identifiers are 16 bits with 0 reserved, so no more than 65 535 exchanges can ever be outstanding; accordingly `max_inflight(0)` now means 65 535 on every protocol version (v3 used to treat it as unlimited).
- Packet identifiers are never reused while the previous operation with that identifier is still unacknowledged.
- A QoS 2 publish refused at PUBREC is removed from persistence, so it is not re-sent on the next connect.
- `AsyncClient` no longer relies on `unsafe impl Send/Sync`; `UserPersistence` holds its store behind a `Mutex` so the types are thread-safe by construction.
- **[Breaking]** **Streams and callbacks no longer displace each other.** In v1.x every stream was a callback underneath — the only way to get anything out of the Paho C library — so `get_stream()` and `set_message_callback()` quietly cancelled one another, `start_consuming_events()` claimed all four callback slots, `stop_consuming()` removed a message callback it had never installed, and `remove_message_callback()` closed the application's message stream, whose sender lived inside the callback. The runtime now feeds the application's channels itself, and the callbacks are the application's alone. Every stream, every consumer and the callback receive each message; several streams may be live at once, where a second `get_stream()` used to close the first; and a stream ends when its receiver is dropped, or on `stop_stream()`.
- `stop_stream()` closes every stream and consumer the client feeds and leaves the callbacks alone. It reaches the event loop as a message, so an async stream's channel closes a moment later rather than synchronously; the synchronous consumers close at once.
- `Event` implements `Clone` and `Debug`.
- While a `disconnect()` with a timeout waits for in-flight operations, new operations are answered at once instead of waiting for it to finish: a `publish()` is buffered for the next connection if offline buffering applies and fails with `Error::Disconnected` otherwise, a `subscribe()` or `unsubscribe()` fails with `Error::Disconnected`, a `connect()` runs as soon as the disconnect completes, and a second `disconnect()` completes with the first. They used to sit unanswered until the disconnect's timeout ran out. `is_connected()` is false from the moment the disconnect starts; `state()` reports `Disconnecting`.
- `is_connected()` no longer reports a connected client as disconnected after an automatic reconnect. The callback thread also cleared the connected flag when it handled a lost connection, and when it was busy — a slow message callback, say — that could happen after the client had already reconnected, leaving the flag wrong until the next connection change. The connection state now has one writer, the client's event loop.
- The `None` with which `disconnect()` ends the message callback's stream is delivered from the client's dispatcher thread once the disconnect has completed, after every message the connection delivered, as for a lost connection or a server's DISCONNECT. It used to be called from the thread calling `disconnect()`, before it returned, where it raced the dispatcher: a message arriving meanwhile could find the callback missing and be dropped although acknowledged, or the `None` itself was skipped.
- `disconnect()` refuses DISCONNECT properties the server would not accept, with `Error::InvalidArgument`: one a DISCONNECT cannot carry, or a non-zero Session Expiry Interval when the CONNECT asked for 0, which the server does not take for a DISCONNECT at all. A DISCONNECT too large for the server's Maximum Packet Size is sent without its Reason String and User Properties, as the specification says; one that cannot be sent even then fails the token. Each of these used to report success while no valid DISCONNECT reached the server, which then published the Will.
- A message carrying a Subscription Identifier is refused by `publish()` / `try_publish()` with `Error::InvalidArgument`, since a client may not send one (MQTT-3.3.4-6); a server answered it with a DISCONNECT. Messages received from a subscription carry them, so remove the property (`msg.props.remove(PropertyId::SubscriptionIdentifier)`) before publishing one on.
- WebSocket and proxy settings are no longer mangled in silence. An HTTP header that HTTP does not allow fails `connect()` with `Error::InvalidArgument` naming it; it used to be dropped from the WebSocket handshake. Percent-escapes in a proxy URI's user name and password (`http://user%40corp:p%3Ass@proxy`) are decoded before they go into `Proxy-Authorization`; they used to be sent as written, and a malformed one is now `Error::InvalidArgument`.
- TLS options that cannot work fail `connect()` with `Error::InvalidArgument`, instead of being ignored or garbled: a `private_key` without the `key_store` holding its certificate (both backends used to connect without a client certificate), and an ALPN protocol name that is empty or over 255 bytes (OpenSSL used to send a malformed list). With OpenSSL, `ca_path` now uses every certificate in each file, as with rustls, rather than only the first.
- Server URIs are parsed as URIs: the host and port end at the first `/`, `?` or `#`. A WebSocket URI keeps its query for the HTTP request (`ws://host:9001?x` used to be refused, the query taken for part of the port). Anything after the host and port of a plain MQTT URI other than a bare `/` — a path, a query — is refused with `Error::InvalidArgument`, as is a fragment in any URI, since none could be sent; a path there used to be ignored. So is user information (`tcp://user@host`, which used to be taken for the host name): MQTT credentials go in `ConnectOptions`.
- Server and proxy URIs can name an IPv6 address, in brackets as in any URI: `tcp://[::1]:1883`, `ssl://[2001:db8::1]`, `ws://[::1]:9001/mqtt`, `http://[::1]:3128`. The brackets come off for the connection and the TLS certificate check (against the address), and go back on in a WebSocket URL, the HTTP `Host` header, and a proxy's `CONNECT` line. An IPv6 address without brackets is refused with `Error::InvalidArgument`, since its colons cannot be told from the port's; so is a URI with no host.
- `TopicMatcher` fixes. `matches()` yields both a filter's own value and that of a `#` below it (with `a/b` and `a/b/#` both present, the topic `a/b` matches both, as `topic_matches()` already said), and is no longer cut short by an entry that had been removed. `remove()` removes the nodes it leaves empty, so `is_empty()` is true once every entry has been removed; `prune()` is no longer needed for that.

### Performance

- **Write pipeline.** Outbound packets are queued and written by the event loop as soon as the socket accepts them, so whatever piles up during one write goes out together in the next, as a single vectored write. A packet sent on an idle connection still goes out at once, with the same syscalls as before: there is no timer and no Nagle-style delay. Against a broker that acknowledges instantly, pipelined QoS 0 publishing went from about 110K to 1.4–1.6M messages per second, and pipelined QoS 1/2 (with the in-flight change below) from 50–100K to 450–630K; see `bench/`. Against a real broker the broker sets the pace, but the client's CPU per message falls several-fold.
- A QoS 0 publish token now resolves once its bytes have been handed to the transport *and flushed*. Over plain TCP and Unix sockets, whose flush does nothing, that is the write itself, as before; over TLS and WebSocket it is after the record or frame has gone out, which is stricter. If the connection fails before then, the token fails with `Error::Disconnected`.
- Several MQTT packets may share one WebSocket frame, as the MQTT specification allows (§6 in v3.1.1 and v5). Frames are capped at about 64 KiB. Note that mosquitto 2.0 handles a whole frame at once and may drop QoS 0 messages it cannot queue for slow subscribers; a sender that bursts QoS 0 over WebSocket can see more of that than before simply because it now sends faster.
- A write blocked on a full socket no longer stops the client reading: inbound messages, acknowledgements and keep-alive carry on, and a peer that is itself blocked writing to the client can no longer deadlock the connection.
- **Reads are 16 KiB.** The inbound buffer used to settle at about 4 KiB, so a burst cost a system call every few packets. Receiving 1 KiB messages is now about 50% faster (`sub`: +53% on tokio, +44% on smol, CPU per message −30%), and small messages a few percent; single-message latency is unchanged. The smol backend also stopped zeroing a fresh 4 KiB array on every read, keeping one buffer per connection instead.
- **The runtime delivers to the application's streams directly.** A message bound for `get_stream()` or `get_event_stream()` no longer travels through the event channel and the callback-dispatcher thread to reach it: the event loop sends it to the stream, one thread hop and one channel fewer per message. Callbacks and the synchronous consumers of `start_consuming*()` are still fed by the dispatcher thread, since a callback may block and a `crossbeam` receiver cannot be awaited; while neither exists the event loop does not touch that path at all.
- **No bounded consumer drops messages any more.** A full `get_stream(Some(n))` or `get_event_stream(Some(n))` used to discard the message with a log line, *after* its PUBACK had gone to the broker — a QoS 1 or 2 message acknowledged but never delivered. The client now stops reading from the broker until the application takes what is there, and acknowledges only what it has handed over. A full bounded `start_consuming_events(Some(n))` blocks the dispatcher thread, which reaches the session the same way. An unbounded stream never blocks the client but grows behind a consumer that falls behind, as before.
- A stall is logged: when the application stops taking messages the client records it, and reports how long it lasted when reading resumes.
- **The persistence store runs on its own thread.** `FilePersistence` wrote each QoS 1/2 message from the event loop, which on the tokio backend with `RuntimeMode::Auto` meant blocking one of the *application's* worker threads on a disk. A store that may block now gets a thread of its own and an ordered queue, and the client holds a message's bytes back until the store has it — so "persist before send" is unchanged, and a burst of publishes reaches the store as one batched call. A store that cannot block says so with the new `ClientPersistence::is_blocking`, and keeps running inline; `MemPersistence` does. Note that throughput with file persistence is still bounded by the store itself, which writes one file per message.
- A persistence store that stops responding no longer stops the client silently: after `CreateOptions::persist_timeout` (30 seconds by default) the session ends with an error and reconnects, as it would for a broker that went quiet.
- **Inbound backpressure.** At most 1024 received messages wait for the application at a time. Beyond that the client stops reading the socket, so TCP flow control slows the broker down instead of the client buffering without limit, and a QoS 1/2 message is acknowledged only once it has been handed to the application rather than on arrival. Reading continues while one of the client's own operations is waiting for an acknowledgement, so a message callback that publishes and waits — a forwarder or bridge — cannot deadlock behind its own backlog. What it reads then is held for the application; QoS 1/2 messages held stay unacknowledged, so the broker's in-flight window bounds them, and QoS 0 messages are bounded by the new `CreateOptionsBuilder::max_held_qos0_messages()` (default 1024) and `max_held_qos0_bytes()` (default 16 MiB, topics and payloads), beyond which newly arrived QoS 0 messages are dropped with a warning, and the number dropped is logged when reading resumes; 0 means no limit. Connection events (`connected`, `connection_lost`, `disconnected`) are never held up by a slow message callback. Note that how deep a backlog survives is the broker's decision, not the client's: with Mosquitto's default `max_queued_messages` of 1000, a client that stops reading is sent about 2000 messages and the broker discards the rest.
- The in-flight window's occupancy is kept as a count instead of being recounted on every publish and acknowledgement, which made deep windows collapse: pipelined QoS 1 with 20 000 in flight went from 20K to about 475K messages per second on tokio.
- The library's own tokio runtime (used when the application has none) checks for socket readiness every couple of task polls while busy, instead of every 61. With its single worker kept busy by a heavy publishing load, acknowledgements could otherwise wait unseen for milliseconds: at 100K msg/s the 99th-percentile publish latency fell from 5.6 to 1.2 ms. `CreateOptionsBuilder::shared_runtime(2)` lowers it a little further, at the cost of a thread.
- `Token::wait()` and `wait_for()` sleep on a condition variable that completion signals, instead of running an executor and, for `wait_for()`, a timer thread. A synchronous `Client` call now costs the same context switches as a plain `wait()`, and pipelined publishing that waits on tokens got about 50% faster. The `futures-timer` dependency is gone.
- Without a persistence store, no persistence work is done at all: no key strings, no extra encoding. With one, the PUBREL that replaces a PUBREC'd message is stored from the bytes already queued for sending, and an inbound QoS 2 payload is stored without being copied. `FilePersistence::remove` is one `unlink`, treating an already-missing file as removed.
- Under sustained load the event loop periodically yields to its executor, so one busy client can no longer starve the others sharing smol's executor thread (their keep-alives stopped).
- The final writes when a session ends — a zero-timeout `disconnect()`, the DISCONNECT sent after a protocol error, or the client being dropped — are bounded at about a second. A zero-timeout `disconnect()` used to make one unbounded write attempt.

### API

- `ClientPersistence` gained two methods, both with defaults, so existing implementations compile unchanged: `put_many`, which a store may implement to write a batch more cheaply than one entry at a time, and `is_blocking`, which tells the client whether the store needs a thread of its own. The default for `is_blocking` is `true`, the safe answer.
- New `CreateOptionsBuilder::persistence_sync`: whether the file store flushes each batch to the disk before a message counts as stored. Defaults to `false`, which is what the store has always done — an entry survives the process dying, but not the machine losing power.
- New `CreateOptionsBuilder::persist_timeout`: how long to wait for the store before giving up on the connection. Defaults to 30 seconds; zero means forever.

- `ConnectOptions::check` rejects a property set the specification does not allow — an out-of-range value, or a repeat of an identifier that may not repeat — instead of letting the connect token fail when the CONNECT is encoded. A configuration mistake is reported where the rest of them are.

- **The crate builds with `--no-default-features`.** Without a runtime backend there is no client — `AsyncClient`, `Client`, `Topic`, `SyncTopic`, `CreateOptionsBuilder::create_async_client` / `create_client`, `TlsOptions::on_tls_error` and `TlsOptionsBuilder::error_callback` are gated on `any(feature = "tokio", feature = "smol")` — but everything that does not need one builds and is tested: the codec re-exports, `Message`, `Properties`, `TopicMatcher`, `TopicFilter`, `Token`, the response types, the options types and their builders, persistence, and `Error`. That is the build for a tool that handles MQTT data without connecting, or (with `serde`) a crate that reads and writes client configuration for an application that links the runtime itself. Previously this configuration failed with six errors about missing items in `crate::runtime`.
- **Exactly one runtime backend at a time.** Enabling both `tokio` and `smol` is a `compile_error!`, since an application could not then tell which runtime it was talking to. `tls`, `openssl` and `websocket` imply `tokio`; `smol-tls` and `smol-websocket` imply `smol`. One consequence for contributors: `cargo doc --all-features` and `cargo test --all-features` do not work at the workspace level — name a feature set instead.

- **`try_publish` hands the message back again.** A synchronous failure returns `Error::Publish`, which now carries the cause and the message: `Err(Error::Publish(cause, msg))` where `cause` is the `Disconnected` or `WrongMqttVersion` that would otherwise have been returned alone. The message comes back whole, including its `dup` flag and packet identifier. **[Breaking]** the variant is `Publish(Box<Error>, Box<Message>)`, where v0.14 had `Publish(i32, Message)`: the cause is an `Error` rather than a C return code, and both halves are boxed to keep `Error` cheap to return by value. `Display` still prints the cause. `publish()` fails its token with the cause alone, unchanged, since it has taken the message.
- **New `try_` calls on `AsyncClient`**, following the pattern of `try_publish`. Each returns a `Result` holding the usual token: a failure found before anything is sent comes back at once as the `Err`, and the token tracks the operation itself. The token-returning calls are now built on them and fail their tokens with the same errors as before.
  - `try_connect`, `try_reconnect` → `Result<ConnectToken>`: invalid connect options, or a server or proxy URI that can't be parsed.
  - `try_disconnect` → `Result<Token<()>>`: a v5 reason code or v5 properties on an earlier-version connection (`WrongMqttVersion`). A refused disconnect leaves the message stream open.
  - `try_subscribe`, `try_subscribe_with_options`, `try_subscribe_many`, `try_subscribe_many_same_qos`, `try_subscribe_many_with_options` → `Result<SubscribeToken>` / `Result<SubscribeManyToken>`: a topic filter that isn't valid (`BadTopicFilter`), a QoS that isn't valid, an empty list or mismatched slice lengths (`InvalidArgument`), or v5 options or properties on an earlier-version connection (`WrongMqttVersion`).
  - `try_unsubscribe`, `try_unsubscribe_with_options`, `try_unsubscribe_many`, `try_unsubscribe_many_with_options` → `Result<UnsubscribeToken>` / `Result<UnsubscribeManyToken>`: a topic filter that isn't valid (`BadTopicFilter`), an empty list (`InvalidArgument`), or v5 properties on an earlier-version connection (`WrongMqttVersion`).

  All of them also fail with `Disconnected` when the client's background session has shut down. A connect refused this way no longer changes the client's MQTT version; previously the version in the rejected options was kept even though the connect never happened.
- **`bytes` is re-exported.** `Bytes` is unavoidable in this crate's API — a `Message` payload is one — so `paho_mqtt::bytes` lets an application name the type without adding its own `bytes` dependency and risking a different version, as it already can for `async_channel` and `crossbeam_channel`.
- **Payload arguments take the new `IntoPayload` trait**, not `Into<Bytes>`. `Bytes` can only borrow a slice or string that is `'static`, so an `Into<Bytes>` bound rejected a byte-string literal (`b"data"`), a slice of a local buffer (`&buf[..]`) and a `&str` taken from an owned `String` — all of which v0.14's `Into<Vec<u8>>` accepted. `IntoPayload` takes those (copying), along with `Bytes`, `BytesMut`, `Vec<u8>`, `String`, `Box<[u8]>`, `[u8; N]` and `Cow` (moving). It applies to `Message::new`, `Message::new_retained`, `MessageBuilder::payload`, and the `Topic` / `SyncTopic` publish helpers. A caller passing a type of their own that implements `Into<Bytes>` now needs an explicit `.into()`.
- **[Breaking]** `Error` is `#[non_exhaustive]`. A `match` on it from outside the crate now needs a wildcard arm, so that a variant added in a later release is something an application absorbs rather than a build failure. Note that this release also drops 16 of v0.14's variants and adds six, so an exhaustive `match` needs revisiting once; `#[non_exhaustive]` is what keeps the next addition from being a break.
- **`stop_stream()` removes only the stream's own callbacks.** It called `remove_callbacks()`, which cleared all four slots — so stopping a message stream silently unregistered the `connected`, `connection_lost` and `disconnected` handlers the application had installed, and they stopped firing with nothing said. Each stream now records which callbacks it installed (by slot generation), and `stop_stream` takes back exactly those: one for a message stream, four for an event stream, so the channel still closes. A callback the application replaced after the stream started is no longer the stream's, and is left in place. `remove_callbacks()` still clears everything.
- **A callback may remove or replace itself.** Each callback is held in a slot with a generation counter that every set and removal bumps; the dispatcher takes the callback out to run it outside the lock — so it may call back into the client — and puts it back only if the slot is untouched. `remove_*_callback()` and `remove_callbacks()` therefore work from inside a callback, as does installing a replacement.
- `disconnect()` signals the end of the message stream by calling the message callback with `None`. That call no longer happens while holding the callback lock, where a callback that registered or removed anything deadlocked.

- **[Breaking]** `QOS_0`, `QOS_1`, `QOS_2` are `QoS` values and `MQTT_VERSION_*` are `MqttVersion` values, rather than `i32` / `u8`. Code that passes the named constants is unaffected (`Message::new(topic, payload, QOS_1)` still works). Code that passed integer literals or variables must convert: `QoS::try_from(n)?`, `MqttVersion::try_from(n)?`. The lossy `From<i32>` for `QoS` and `From<u8>` for `MqttVersion` — which mapped invalid values to a default — are removed.
- **[Breaking]** **The default QoS is 0.** `QoS::default()` is `QoS0`, where v0.14's was `AtLeastOnce` (1), so a `MessageBuilder` that never calls `.qos()` now publishes at most once instead of at least once. This is deliberate: 0 is the QoS the MQTT specification treats as the base case and the default across MQTT client libraries generally, and a type whose `Default` is not its zero value is a trap of its own. It is called out here because it is the one change in this release that **compiles cleanly and silently weakens a delivery guarantee** — nothing fails, messages are simply no longer acknowledged, persisted or re-sent. Audit any `MessageBuilder` chain that relied on the old default and add `.qos(QOS_1)`.
- The QoS parameters of `subscribe()`, `subscribe_with_options()`, `subscribe_many()`, `subscribe_many_same_qos()` and `subscribe_many_with_options()`, on `AsyncClient` and `Client`, take `TryInto<QoS>`. Plain integers are accepted again, as in v1 (`cli.subscribe("a/b", 1)`), alongside `QoS` values and the `QOS_*` constants; an out-of-range value fails the token with `Error::Protocol(BadQoS)` instead of being sent.
- **[Breaking]** `Properties`, `Property`, `PropertyId` (v0.14's `PropertyCode`, kept as an alias), `PropertyType`, `ReasonCode`, `QoS`, and `MqttVersion` now come from `paho-mqtt-codec` and are re-exported here; their APIs are documented in that crate. They are reachable at the same paths, but they are new types rather than the C wrappers, so code that touches them needs changes:
    - **`Property` is an enum with a variant per identifier**, carrying the value that identifier's wire type calls for: `Property::ContentType(s)`, `Property::TopicAlias(7)`. v0.14's opaque wrapper, with `Property::new(code, val) -> Result<Property>`, `property_code()`, `property_type()` and the `get_*` readers, is gone — as is the possibility of a value whose type does not match its identifier, which no longer compiles.
    - **`Properties::push` and the `push_*` family return `()`**, not `Result`. A value the specification does not allow marks the collection, and encoding it fails; `try_push` reports it at the call instead. Remove the `?` or `.unwrap()` from v0.14 call sites.
    - **Reads are by identifier**, through `get(id)` for the property itself or the typed accessors (`get_u16`, `get_str`, `get_binary`, `get_user`, and a pair per property such as `get_content_type`). v0.14's `get_val::<T>`, `get_int` and `Property::get::<T>` are gone.
    - **The `properties!` macro takes a list of properties**, `properties![Property::MessageExpiryInterval(60)]`, rather than `properties!{ code => value }`.
- `AsyncClient::disconnect` / `Client::disconnect` return `Error::WrongMqttVersion` when `DisconnectOptions` carry v5 properties on a v3 connection, as `connect`, `publish`, and `subscribe` already did for theirs.
- New `AsyncClient::state()` and `Client::state()`, returning a `ConnectionState`: `Disconnected`, `Connecting`, `Connected`, `Disconnecting`, `Retrying` (the connection was lost and automatic reconnect is waiting out its back-off) or `Reconnecting` (an automatic reconnect attempt is in progress). The state has already changed when the token of the operation that changed it resolves and when the callback announcing it runs, so a `connection_lost` callback sees `Retrying` or `Disconnected`. `is_connected()` is `state() == Connected`. `ConnectionState` is `#[non_exhaustive]`.
- The library no longer returns `Error::General` or `Error::GeneralString`; each failure it reported that way now has its own variant, so an application can match on it rather than on a message:
    - **`Error::InvalidArgument(String)`** — an unparsable server or proxy URI (a proxy URI is now checked by `connect()` itself, rather than failing each WebSocket connection attempt, auto-reconnect's included), `subscribe_many` lists of different lengths, an empty list of topics to (un)subscribe, an out-of-range `RetainHandling`, and the `ConnectOptions` checks (retry intervals, invalid connect properties, an authentication method without a handler or the reverse).
    - **`Error::WebSocket(String)`** — a failed WebSocket upgrade, or a request that could not be built.
    - **`Error::Proxy(ProxyError)`** — the HTTP proxy refused the tunnel (`ProxyError::Refused`, with the HTTP status code and status line; `407` means credentials are needed or were rejected), closed the connection (`Closed`), sent data before the tunnel was open (`EarlyData`), or sent something other than a well-formed HTTP response (`OverlongResponse`, `NotUtf8`, `MalformedStatusLine`).
    - **`Error::ProtocolViolation(&'static str)`** — the server answered CONNECT with something other than CONNACK, or sent an AUTH the client did not expect.
    - A persistence store that stops responding ends the connection with `Error::Persistence` rather than a `GeneralString`.
    - `General` and `GeneralString` remain for application code with a failure of its own. `Failure`, `BadProperty` and `Conversion`, which nothing returns, are kept so that v0.14 code naming them compiles.
- **[Breaking]** Creating a client with file persistence (`PersistenceType::File` or `FilePath`) and an empty client ID fails with `Error::InvalidArgument`. v0.14 switched such a client to no persistence without saying so. The store is named after the client ID, and a client without one never resumes a session, so there is nothing to keep; every such client would also share one directory. A `User` store, or none, still works with an empty ID.
- New `Error::AlreadyConnected` and `Error::ConnectInProgress`. A `connect()` on a client that is already connected, or while another `connect()` is under way, fails with one of these instead of an `Error::General` string, so an application can match on it.
- `PersistenceType` can be created as a file path from an `OsString`, or from a reference to any string or path, including `&OsStr`, `&String`, and `&PathBuf`.
- New `AsyncClient::current_server_uri()` and `Client::current_server_uri()`: the URI of the server the client is connected to, or `None` when it is not connected. With a list of servers in `ConnectOptions` this is the one that accepted the connection, including after an automatic reconnect, and it is set by the time the connect token resolves or the connected callback runs. `server_uri()` still returns the URI the client was created with; its documentation now says so.
- `ConnectOptions::with_mqtt_version(ver)` builds options for the version asked for, the counterpart of `new_v3()` / `new_v5()` for a version held in a variable.
- `ConnectResponse::mqtt_version` reports the negotiated protocol level (3, 4, or 5) rather than collapsing every v3 to 4.
- **New `serde` feature.** `Serialize`/`Deserialize` for `Message`, the four response types (`ConnectResponse`, `PublishResponse`, `SubscribeResponse`, `UnsubscribeResponse`), the options types (`CreateOptions`, `ConnectOptions`, `DisconnectOptions`, `SubscribeOptions`, `TlsOptions`, with `RuntimeMode`, `PersistenceType`, `RetainHandling` and `TlsVersion`), and the codec types this crate re-exports. v0.14 had no serialization support.
- Serialized options are configuration, so every field is optional on the way in and takes its default when absent: `TlsOptions` read from `{"trust_store": "/ca.crt"}` still verifies the broker, and `Message` named in a configuration file need only give the topic. Three things cannot be represented and are skipped, coming back absent: `ConnectOptions::auth_handler` and `TlsOptions::on_tls_error` (callbacks are code, not configuration) and `CreateOptions::user_data` (a `Box<dyn Any>`). Options that name an `AuthenticationMethod` therefore fail `ConnectOptions::check` until the handler is set again. `PersistenceType::User` is skipped as a variant: serializing one is an error rather than a silent downgrade to `None`, while `None`, `File` and `FilePath` round-trip.

### Development

- `paho-mqtt-codec` has Criterion benchmarks (`cargo bench -p paho-mqtt-codec`) and two `cargo-fuzz` targets; CI runs the benchmarks' compile and a short fuzz pass on every change, and builds the codec for a bare-metal `no_std` target.
- `bench/` holds the benchmark harness: a client driver and a null broker (`bench/mqttbench`), a script that runs a configuration matrix with the builds being compared interleaved (`matrix.sh`), and a summarizer (`summarize.py`). See `bench/README.md`.
- `buildtst.sh` mirrors the CI matrix (feature sets × stable/MSRV, clippy, docs, no_std, benches) with `--integration`, `--fuzz`, and `--bench` options.
- Integration tests run against a local Mosquitto on both runtimes, including WebSocket, Unix sockets, and v3.1. Behaviour a stock broker will not produce — a refused subscription, an imposed keep-alive, enhanced authentication, a session it declines to resume, a TLS certificate naming the wrong host, an HTTP proxy — is tested against scripted servers built on `paho-mqtt-codec` (`tests/common/`). The TLS tests generate their own certificates, so no broker TLS configuration is needed and none of the suite is `#[ignore]`d.
- `README.md` is rewritten for v2, and its code examples are compiled by `tests/readme.rs` so they cannot fall out of date.
- The smol backend's `cfg` gates are plain `#[cfg(feature = "smol")]`. They read `all(feature = "smol", not(feature = "tokio"))` while both backends could be enabled at once and tokio took precedence; with that combination now refused, the negation said nothing and invited the reader to wonder what it was for. Six sites, in `runtime/mod.rs`, `runtime/connection.rs` and `async_client.rs`. The gates that turn on `not(feature = ...)` for combinations still possible — rustls winning over OpenSSL, and the per-backend transport features — are unchanged.

## [v0.14.0](https://github.com/eclipse/paho.mqtt.rust/compare/v0.13.4..v0.14.0) - (2026-03-25)

- Support for Paho C v1.3.16
    - Improved performance and lower latency for connect and publish operations.
- Added synchronous (blocking) and async event streams.
    - All events from the client flow through the stream:
      Connect, Connection Lost, Disconnected, Incoming Message
- Typeof Variable Byte Integer is u32 (not i32)
- `PropertyType` enum now supports `Ord`, `PartialOrd`, and `Hash` traits
- Added a _tokio_ build feature for cargo, and a few tokio example apps.
  Enabling the feature simply builds the _tokio_ examples. (The library is still fully runtime agnostic).
- Removed _async-std_ from examples. Using _smol_ instead.
- Properties::byte_len and Properties::get_at no longer cast *const MQTTProperties to *mut before calling C functions that are correctly declared const in the bindings.
- [#244](https://github.com/eclipse-paho/paho.mqtt.rust/issues/244) Error handling for MQTT5 seems inconsistent
    - [#264](https://github.com/eclipse-paho/paho.mqtt.rust/pull/264) Fix async MQTT5 error handling
    - Proper handling of v5 failures w/ Reason Codes
    - **[Breaking]** Reason Code Error variant now contains Properties from ACK packet
    - **[Breaking]** ACKs with a single reason code error generate an Error result, instead of an Ok() with a possible error code in it.
        - Subscribe Many & Unsubscribe Many still return Ok, possibly with a combination of success and error reason codes
- Bumped MSRV to Rust v1.75
- Bumped paho-mqtt-sys to v0.11
- Various new clippy warnings fixed
- [#257](https://github.com/eclipse-paho/paho.mqtt.rust/pull/257) initialize mqtt version from create options instead of assuming
- [#261](https://github.com/eclipse-paho/paho.mqtt.rust/pull/261) Remove undefined property from Cargo.toml
-


## [v0.13.4](https://github.com/eclipse/paho.mqtt.rust/compare/v0.13.3..v0.13.4) - (2025-09-02)

- Fixed some elided lifetime warnings
- Bumped dependencies:
    - thiserror to v2.0
    - toml to v0.9
- [#227](https://github.com/eclipse-paho/paho.mqtt.rust/issues/227) Bump async-channel to v2
- [#248](https://github.com/eclipse-paho/paho.mqtt.rust/pull/248) Implement binary password support


## [v0.13.3](https://github.com/eclipse/paho.mqtt.rust/compare/v0.13.2..v0.13.3) - (2025-04-28)

- `TopicMatcher` implements `From(HashMap)` for the value type.
- [#243](https://github.com/eclipse-paho/paho.mqtt.rust/issues/243) `TopicMatcher` not matching parent when using '#' wildcard.


## [v0.13.2](https://github.com/eclipse/paho.mqtt.rust/compare/v0.13.1..v0.13.2) - (2025-03-27)

- Added some methods to `TopicFilter`:
    - Queries `has_wildcards()` and `num_fields()`.
    - TryFrom for `&str` and `String`


## [v0.13.1](https://github.com/eclipse/paho.mqtt.rust/compare/v0.13.0..v0.13.1) - (2025-02-19)

- [#240](https://github.com/eclipse-paho/paho.mqtt.rust/issues/240) `Display` trait for `MqttVersion` enumeration.
- [#241](https://github.com/eclipse-paho/paho.mqtt.rust/issues/241) Restored the ability to turn off the `ssl` feature.
- Bumped -sys to v0.10.1 to get Paho C v1.3.13


## [v0.13.0](https://github.com/eclipse/paho.mqtt.rust/compare/v0.12.5..v0.13.0) - (2025-01-21)

- Updated License to EPL-v2.0
- Bumped MSRV to Rust v1.73.0
- Bumped -sys to v0.10.0
    - Wrapping Paho C v1.3.14
        - Support for UNIX-domain sockets on local machine (*nix only)
    - `build.rs` builds optional UNIX sockets into Paho C on non-Windows systems (*nix)
- Reworked the Error type
    - Remove `Paho` and `PahoDescr` errors. De-nested them into the top-level.
    - Parsing the error messages from PahoDescr for new error types.
    - Removed Paho error constants. Now errors can be matched easily/directly.
- `Token` simplified to create an `Option<Result<ServerResponse>>` instead of individual components.
- Created new enumeration types:
    - `MqttVersion`
    - `ConnectReturnCode` (for MQTT v3.x)
    - `QoS`
- [#181](https://github.com/eclipse-paho/paho.mqtt.rust/pull/181) Updated README.md with explanation on how to build for apple m1/x86_64
- [#216](https://github.com/eclipse-paho/paho.mqtt.rust/issues/216) Deref QoS pointers for SubscribeMany and UnsubscribeMany in server response
- [#224](https://github.com/eclipse-paho/paho.mqtt.rust/pull/224) Fix QoS 0 and 1 conversion
- [#236](https://github.com/eclipse-paho/paho.mqtt.rust/pull/236) Make from_c_parts only visible inside crate


## [v0.12.5](https://github.com/eclipse/paho.mqtt.rust/compare/v0.12.4..v0.12.5) - (2024-05-25)

- Updated the License to EPL-v2.0
- Added some missing Eclipse Paho legal documents.
- Added `TopicMatcher::insert_many()`
- [#216](https://github.com/eclipse/paho.mqtt.rust/issues/216) Deref QoS pointers for SubscribeMany and UnsubscribeMany in server response
- [#215](https://github.com/eclipse/paho.mqtt.rust/issues/215) Now `subscribe_many()` returns an error if slices not the same length. Also added `subscribe_many_same_qos()` to the clients.


## [v0.12.4](https://github.com/eclipse/paho.mqtt.rust/compare/v0.12.3..v0.12.4) - 2024-05-19

- Fixes for topic matching:
    - `TopicMatcher`
        - Fixed a number of corner cases
        - Iterator optimized
        - Added `prune()` and `shrink_to_fit()`, and `get_key_value()`
    - `TopicFilter` fixed corner cases
    - Added stand-alone `topic_matches()` and `topic_matches_iter()` functions from [PR #228](https://github.com/eclipse/paho.mqtt.rust/pull/228)


## [v0.12.3](https://github.com/eclipse/paho.mqtt.rust/compare/v0.12.2..v0.12.3) - 2023-10-25

- The -sys crate now wraps Paho C v1.3.13, fixing several issues, including crashes on reconnect callbacks.
- Made the C logs less verbose
- [#203](https://github.com/eclipse/paho.mqtt.rust/pull/203) `AsyncClient::server_uri()` getter.
- [#202](https://github.com/eclipse/paho.mqtt.rust/pull/202) Fix disconnect timeout (from sec to ms)


## [v0.12.2](https://github.com/eclipse/paho.mqtt.rust/compare/v0.12.1..v0.12.2) - 2023-09-12

- [#209](https://github.com/eclipse/paho.mqtt.rust/issues/209) Added trace/log statements from the Paho C library to the Rust logs
- Minor cleanup of subscriber examples.


## [v0.12.1](https://github.com/eclipse/paho.mqtt.rust/compare/v0.12.0..v0.12.1) - 2023-03-20

- [#191](https://github.com/eclipse/paho.mqtt.rust/pull/191) AsyncClient::get_stream() support unbounded channel
- [#194](https://github.com/eclipse/paho.mqtt.rust/issues/194) Bumped bindgen to latest version, v0.64, in -sys crate
- [#193](https://github.com/eclipse/paho.mqtt.rust/issues/193) Consmer notification when brokercleanly disconnects


## [v0.12.0](https://github.com/eclipse/paho.mqtt.rust/compare/v0.11.1..v0.12.0) - 2023-01-09

- Updated to Rust Edition 2021 w/ MSRV 1.63.0
- Upgrade to Paho C v1.3.12
    - Fixes a performance issue, particularly for receiving messages.
    - New URI protocol schemes: "mqtt://" for TCP and "mqtts://" for encrypted SSL/TLS.
- [**Breaking**] Updated `CreateOptions` and `ConnectOptions` behavior:
    - The `CreateOptions` default is for a "universal" client that can connect using v3.x or v5. (This was previously specified as the v5 option).
        - Can use `CreateOptions::new_v3()` for a client that can only connect using v3.x
        - Defaults to no message persistence (i.e. persistence is opt-in).
    - The v3.x vs v5 division is made when connecting.
    - `ConnectOptions::new()` still defaults to v3.x
    - New constructors for specific protocol version, `ConnectOptions::new_v5()`, `new_ws()`, and `new_ws_v5()`, for v5, websocket,  and v5 over websockets, respectively.
    - Connect options default to clean session/state, as appropriate for the constructed protocol version.
    - You select the MQTT protocol version with one of the new constructors, and can not change it after creation. (No longer a set `mqtt_version()` function).
- `AsyncClient::mqtt_version()` now returns the version for the current connection - or the most recent successful connection. Removed `AsyncClient::current_mqtt_version()`.
- Updated `SubscribeOptions` to be more usable.
- Created a new [example](https://github.com/eclipse/paho.mqtt.rust/blob/develop/examples/async_subscribe_v5.rs) for MQTT v5 subscriptions with subscribe options.
- [#182](https://github.com/eclipse/paho.mqtt.rust/issues/182) Callback must now be `Send` since they will be called from another thread.
- [#172](https://github.com/eclipse/paho.mqtt.rust/issues/172) Linking to `User32` library on Windows to try to avoid build problems.
- [#170](https://github.com/eclipse/paho.mqtt.rust/issues/170) Updated the cmake crate dependency in -sys to 0.1.49 to support both older CMake (pre v3.12) and newer systems like VS 2022.
- [#156](https://github.com/eclipse/paho.mqtt.rust/issues/156) (continued) Added a mutable iterator to TopicMatcher, with functions `remove()`, `get_mut()`, and `matches_mut()`
- [#170](https://github.com/eclipse/paho.mqtt.rust/issues/170) Upgraded cmake crate to v0.1.48 to support building with Visual Studio 2022.
- [#166](https://github.com/eclipse/paho.mqtt.rust/issues/166) Fix topic matches with single-level wildcard.
- [#151](https://github.com/eclipse/paho.mqtt.rust/issues/151) Fixed wrong documentation of QoS 1
- [#57](https://github.com/eclipse/paho.mqtt.rust/issues/57) Updated this README with more help for musl builds.
- Fixed clippy warnings


## [v0.11.1](https://github.com/eclipse/paho.mqtt.rust/compare/v0.11.0..v0.11.1) - 2021-05-03

- [#156](https://github.com/eclipse/paho.mqtt.rust/issues/156) Improvements to `TopicMatcher`:
    - Doesn't require item type to implement `Default` trait
    - Match iterator returns key/value tuple (not just value).
- [#154](https://github.com/eclipse/paho.mqtt.rust/pull/154) Add public interface to retrieve `client_id`.


## [v0.11.0](https://github.com/eclipse/paho.mqtt.rust/compare/v0.10.0..v0.11.0) - 2021-04-16

- Updated to support Paho C v1.3.10
- New client functions to stop consuming/streaming and to remove callbacks.
- Started a README for the -sys crate.
- Fixed a bunch of lints. Clippy report is clean.
- [#152](https://github.com/eclipse/paho.mqtt.rust/issues/152) Consumer won't panic when the receiver drops.
- [#113](https://github.com/eclipse/paho.mqtt.rust/issues/113) Build now respects the OPENSSL_STATIC flag (if OPENSSL_DIR or other path flags set).
- [#145](https://github.com/eclipse/paho.mqtt.rust/issues/145) `impl From<Error> for io::Error` An MQTT error can be easily converted back to an I/O error.

## [v0.10.0](https://github.com/eclipse/paho.mqtt.rust/compare/v0.9.1..v0.10.0) - 2021-01-25

- Updated to support Paho C v1.3.9
- Switched consumers/streams to use crossbeam channels and async_channel's, respectively.
- Added a `TopicFilter` type to match topics against an individual filter (typically containing wildcards).
- Added a `TopicMatcher` collection to iterate through a set of matching topic filters, such as to match callbacks to specific filters.
- _Finally_ ran `rustfmt` on source files.
- Fixed MQTT v5 topic alias support.
- [#118](https://github.com/eclipse/paho.mqtt.rust/issues/118) Added `CreateOptionsBuilder::send_while_disconnected(bool)` and detached the behavior somewhat from `max_buffered_messages()`. Now, setting the buffer size to a non-zero value will _not_ enable off-line buffering.
- [#120](https://github.com/eclipse/paho.mqtt.rust/issues/120), [#121](https://github.com/eclipse/paho.mqtt.rust/pull/121) Fixed `subscribe_many_with_options()` outbound opts.
- [#122](https://github.com/eclipse/paho.mqtt.rust/pull/122) Some _clippy_-recommended fixes
- [#139](https://github.com/eclipse/paho.mqtt.rust/issues/139) Added a `SyncClient` struct for repeated publishing to the synchronous client.
- [#140](https://github.com/eclipse/paho.mqtt.rust/issues/140) The MQTT protocol version used to create the client is now the default for connecting.

## [v0.9.1](https://github.com/eclipse/paho.mqtt.rust/compare/v0.9.0..v0.9.1) - 2021-01-01

- [#101](https://github.com/eclipse/paho.mqtt.rust/issues/101) `Token::try_wait()` to check for the result of a `Token` without blocking.
- [#101](https://github.com/eclipse/paho.mqtt.rust/issues/101) A `try_publish()` function for the `AsyncClient` and `Topic` which return a synchronous result that the message was created and queued for transmission successfully.
- [#28](https://github.com/eclipse/paho.mqtt.rust/issues/28) Some instructions for using the "cross" tool for cross-compiling.


## [v0.9.0](https://github.com/eclipse/paho.mqtt.rust/compare/v0.8.0..v0.9.0) - 2020-12-29

- Websocket HTTP/HTTPS proxy support
- Added missing MQTT v5 support:
    - Subscribe and Unsubscribe can now have v5 properties, thus enabling Subscription Identifiers.
- [Breaking] Persistence defaults to `None` if no Client ID specified in creation.
- Ability to specify a path when using File persistence
- Updated bindings to Paho C v1.3.8
- Ability to start publishing (queuing) messages before fir first successful connection.
- New offline buffering options:
    - Ability to start publishing (queuing) messages before first successful connection.
    - Option to delete the oldest messages first from the queue when it fills up.
- New persistence options:
    - The option to not restore messages from persistence on startup (fresh restart).
    - The option to not persist QoS 0 messages.
-
- [#110] Update to `futures-timer` v3.0
- [#95] Added Send bounds to `ClientPersistence`
- [#92] Vendored SSL with _openssl-sys_ crate (optional)
- New example apps:
    - _sync_consume_v5.rs_ - An MQTT v5 consumer that uses Subscription ID's to handle incoming messages.
    - _ws_publish.rs_ - Simple websocket example with optional proxy.


## [v0.8.0](https://github.com/eclipse/paho.mqtt.rust/compare/v0.7.1..v0.8.0) - 2020-11-20

- Upgraded Tokens to implement Futures 0.3. (async/await compatible!)
- Error type based on _thiserror_
- Added some missing/forgotten MQTT v5 support:
    - Connect and Will properties in connect options
    - Reason code and properties in disconnect options
- Ability to set additional HTTP headers in a Websocket opening handshake.
- Added MQTT v5 topic alias capability with an example.
- Examples using async/await
- Removed old asynchronous (futures 0.1-style) examples
- Message and option structs were reimplemented internally with pinned inner data structs.
- Removed `AsyncClientBuilder`. Use `CreateClientBuilder` instead, possibly with new `create_client()` function.
- `SslOptions` using `Path` and `PathBuf` for file names in the API instead of `String`.
- The reason code returned from the server moved into the `ServerResponse` struct.
- Added `ConnectResponse` as a struct instead of a tuple for the data returned in CONNACK.
- Upgraded crate to 2018 Edition

## [v0.7.1](https://github.com/eclipse/paho.mqtt.rust/compare/v0.7..v0.7.1) - 2020-04-28

It turned out that the update to the -sys crate in v0.7 was a breaking change. This just bumps the version numbers to indicate that, so as not to break v0.6 builds via crates.io

## [v0.7](https://github.com/eclipse/paho.mqtt.rust/compare/v0.6..v0.7) - 2020-04-27

Version 0.7 brings full support for MQTT v5, including:

- Ability to create an MQTT v5 client and request a v5 connection to the server.
- MQTT v5 `Properties` (for connect, publish, incoming messages, etc)
- `ReasonCode` and better error notifications.
- [Breaking] Restored the single `Token` type, getting rid of separate implementations of `ConnectToken`, `SubscribeToken`, etc.
- Subscribe options, such as "No Local," etc.
- `Topic` objects can now be used to subscribe.
- New callback `on_disconnect()` for when the client receives a disconnect packet from the server, complete with a reason code and properties.
- Example for a simple chat application _(mqttrs_chat)_ using the v5 "No Local" subscription option. The publisher does not get their own messages echoed back to them.
 - Examples for RPC using v5 _Properties_ for _ResponseTopic_ and _CorrelationData:_
     - A math RPC service/server _(rpc_math_srvr)_ that performs basic operations on a list of numbers.
     - A math RPC client  _(rpc_math_cli)_ that can send requests.

Also:

- Fix for #48: Sends a _None_ (and exits consumer) on manual disconnect.
- Fix for #49: Supporting `on_connect()` callback.
- Fix for #51: Segfault on `subscribe_many()` with a single topic.
- The build now uses the environment variable `OPENSSL_ROOT_DIR` to help find the SSL libraries in a non-standard install directory.

Note that v0.7 still targets Futures v0.1 and Rust Edition 2015. Support for async/await, and std Future (0.3) will be coming shortly in v0.8.


## [v0.6](https://github.com/eclipse/paho.mqtt.rust/compare/v0.5..v0.6) - 2019-10-12

The v0.6 release added support for Futures and cleaned up the internal implementation of the library.

- **Futures support:**
    - Compatible with the [Rust Futures](https://docs.rs/futures/0.1.25/futures/) library v0.1
    - Now depends on the crates "futures" (v0.1) and "futures-timer" (v0.1).
    - The `Token` objects, which are returned by asynchronous calls, now implements the `Futures` trait, which is _mostly_ compatible with the previous implementation.
    - Incoming messages can be obtained through a `Stream` from the client, implemented with a futures channel.
    - New examples of a publisher and subscriber implemented with futures.

- **Server Responses**
    - There are now several different types of tokens corresponding to different requests for which the server can return a response: _ConnectToken_, _DeliveryToken_, _SubscribeToken_, etc.
    - Tokens now track the type of request and get the server response upon completion. This is the Futures _Item_ type for the token.
    - In particular this is useful for connecting subscribers. The app can now determine if a persistent session is already present, and only needs to subscribe if not.

- **Send and Sync Traits**
    - The clients are now marked as _Send_ and _Sync_
    - The _Token_ types are _Send_
    - Most of the option types are _Send_ and _Sync_
    - _AsyncClient_ and _Token_ objects are now just _Arc_ wrappers around inner structs making it easy to clone and pass references around.

- **Internal Cleanup**
    - Updated to wrap Paho C v1.3.1 which has a number of important bug fixes.
    - Moved `Tokens` into their own source file.
    - Consolidated persistence internals into `UserPersistence` struct.
    - Created a new `ResponseOptions` struct to manage the details of the C `MQTTAsync_responseOptions` objects.
    - Cleanup of the `AsyncClient` implementation.
    - A bad reconnect bug is fixed (Issue #33)


## [v0.5](https://github.com/eclipse/paho.mqtt.rust/compare/v0.4..v0.5) - 2018-12-15

### Added

- WebSocket support (free with Paho C 1.3.0 update).
- Example apps can take server URI's from the command line.

### Changed

- Updated the library to bundle and use Paho C v1.3.0


