# Change Log
#
# Eclipse Paho MQTT Rust library.
#

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.14.0 (Unreleased)

- Added synchronous (blocking) and async event streams.
    - All events from the client flow through the stream: 
      Connect, Connection Lost, Disconnected, Incoming Message 
- Typeof Variable Byte Integer is u32 (not i32)
- `PropertyType` enum now supports `Ord`, `PartialOrd`, and `Hash` traits

## [v0.13.4](https://github.com/eclipse/paho.mqtt.rust/compare/v0.13.3..v0.13.4) - (2025-09-02)

- Fixed some elided lifetime warnings
- Bumped depedencies:
    - thiserror to v2.0
    - toml to v0.9
- [#227](https://github.com/eclipse-paho/paho.mqtt.rust/issues/227) Bump async-channel to v2
- [#248](https://github.com/eclipse-paho/paho.mqtt.rust/pull/248) Imlement binary password support


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
    - Fixes a performance issue, particularily for receiving messages.
    - New URI protocol schemes: "mqtt://" for TCP and "mqtts://" for encrypted SSL/TLS.
- [**Breaking**] Updated `CreateOptions` and `ConnectOptions` behavior:
    - The `CreateOptions` default is for a "universal" client that can connect using v3.x or v5. (This was previously specified as the v5 option).
        - Can use `CreateOptions::new_v3()` for a client that can only connect using v3.x
        - Defaults to no message persistence (i.e. persistence is opt-in).
    - The v3.x vs v5 devision is made when connecting.
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
    - _ws_publish.rs_ - Simeple websocket example with optional proxy.


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


