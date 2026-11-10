// connect_options.rs
//
// The set of options for connecting to an MQTT client.
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2017-2026 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! Connect options for the Paho MQTT Rust client library.
//!
//! This contains the structures to define the options for connecting to the
//! MQTT broker/server.

use crate::{
    Error, Message, Properties, Result, TlsOptions,
    auth::{AuthChallenge, AuthHandler},
};
use paho_mqtt_codec::types::*;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, time::Duration};

/// The default keep alive interval for MQTT (TCP) & MQTTS (TLS) connections.
const DEFAULT_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(60);

/// The default keep alive interval for Websocket connections.
const DEFAULT_KEEP_ALIVE_INTERVAL_WS: Duration = Duration::from_secs(45);

/////////////////////////////////////////////////////////////////////////////
// ConnectOptions

/// The options for connecting to an MQTT broker.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    /// The maximum time that should pass without communications between
    /// the client and server. Resolution in sec.
    pub keep_alive_interval: Duration,
    /// The clean start/session flag.
    pub clean_start: bool,
    /// The maximum number of QoS 1/2 messages that can be in flight at once.
    /// 0, like the default of 65 535, allows as many as the protocol does.
    pub max_inflight: u16,
    /// The Last Will and Testament (LWT) message
    pub will: Option<Message>,
    /// The TLS options
    pub tls: Option<TlsOptions>,
    /// The user name for authentication
    pub user_name: Option<String>,
    /// The password for authentication
    pub password: Option<Vec<u8>>,
    /// The timeout for the connection attempt
    pub timeout: Duration,
    /// The servers to which the client will connect, as URIs.
    pub server_uris: Vec<String>,
    /// The MQTT protocol for the connection
    pub mqtt_version: MqttVersion,
    /// Whether the server should automatically reconnect on a connection lost.
    pub auto_reconnect: bool,
    /// The minimum retry interval for auto reconnects. Resolution in sec.
    pub min_retry_interval: Duration,
    /// The maximum retry interval for auto reconnects. Resolution in sec.
    pub max_retry_interval: Duration,
    /// The connect properties (MQTT v5 only)
    pub props: Properties,
    /// Handler for an MQTT v5 enhanced-authentication exchange. Required
    /// when [`props`](Self::props) names an `AuthenticationMethod`, and
    /// meaningless without one.
    ///
    /// Skipped by `serde`: a handler is code, not configuration. Options
    /// that name an authentication method therefore fail
    /// [`check`](Self::check) after a round trip until the handler is set
    /// again, which is the mistake being reported, not a new one.
    #[cfg_attr(feature = "serde", serde(skip))]
    pub auth_handler: Option<AuthHandler>,
    /// HTTP headers for websocket connections
    pub http_headers: HashMap<String, String>,
    /// The HTTP proxy (insecure), if any
    pub http_proxy: String,
    /// The HTTPS proxy (secure), if any
    pub https_proxy: String,
}

impl ConnectOptions {
    /// Creates a new, default set of connect options, currently for MQTT v5
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new, default set of connect options for MQTT v3.1.1
    pub fn new_v3() -> Self {
        Self {
            mqtt_version: MqttVersion::V3_1_1,
            ..Self::default()
        }
    }

    /// Creates a new, default set of connect options for MQTT v5
    pub fn new_v5() -> Self {
        Self {
            mqtt_version: MqttVersion::V5,
            ..Self::default()
        }
    }

    /// Creates a new, default set of connect options for connecting over
    /// websockets, which carry a longer keep-alive interval and, as
    /// elsewhere, MQTT v5.
    pub fn new_ws() -> Self {
        Self {
            keep_alive_interval: DEFAULT_KEEP_ALIVE_INTERVAL_WS,
            ..Self::default()
        }
    }

    /// Creates a new, default set of connect options for connecting MQTT v3.1.1
    /// over websockets.
    pub fn new_ws_v3() -> Self {
        Self {
            keep_alive_interval: DEFAULT_KEEP_ALIVE_INTERVAL_WS,
            mqtt_version: MqttVersion::V3_1_1,
            ..Self::default()
        }
    }

    /// Creates a new, default set of connect options for connecting MQTT v5
    /// over websockets.
    pub fn new_ws_v5() -> Self {
        Self {
            keep_alive_interval: DEFAULT_KEEP_ALIVE_INTERVAL_WS,
            ..Self::new_v5()
        }
    }

    /// Creates a new set of connect options for the specified MQTT
    /// protocol version (v3.1, v3.1.1, or v5).
    pub fn with_mqtt_version<V: Into<MqttVersion>>(ver: V) -> Self {
        Self {
            mqtt_version: ver.into(),
            ..Self::default()
        }
    }

    /// The keep-alive interval as the whole seconds a CONNECT packet carries.
    ///
    /// The wire field is a `u16` count of seconds (MQTT §3.1.2.10), so a
    /// fractional part cannot be sent. Zero means keep-alive is disabled.
    /// A non-zero interval shorter than a second rounds **up** to one
    /// second: truncating it to zero would disable keep-alive, the opposite
    /// of what was asked for. Anything longer saturates at `u16::MAX`
    /// seconds, a little over 18 hours.
    pub(crate) fn keep_alive_secs(&self) -> u16 {
        let secs = self.keep_alive_interval.as_secs();
        if self.keep_alive_interval.is_zero() {
            0
        } else {
            secs.clamp(1, u16::MAX as u64) as u16
        }
    }

    /// Does a check of the options to determine if they're valid.
    ///
    /// Rules:
    /// - If you specify a secure URI ("tls://", "wss://", etc), you must
    ///   supply TLS options.
    /// - No v5 properties in a v3 connection.
    /// - v5 properties, if present, must be valid
    /// - Advanced auth must be configured for both client and server.
    /// - With auto-reconnect on, `max_retry_interval` must be at least
    ///   `min_retry_interval`.
    /// - TLS options must be usable: a private key needs its key store, and
    ///   ALPN protocol names must be 1 to 255 bytes.
    /// - HTTP headers for a WebSocket connection must be valid HTTP.
    pub fn check(&self) -> Result<()> {
        if !self.server_uris.is_empty() {
            let uris = &self.server_uris;
            let any_secure = uris.iter().any(|s| crate::is_secure_uri(s));

            if any_secure && !self.has_tls_options() {
                return Err(Error::MissingTlsOptions);
            }
        }
        if let Some(tls) = &self.tls {
            tls.check()?;
        }
        // Without a WebSocket transport the headers go nowhere.
        #[cfg(any(feature = "websocket", feature = "smol-websocket"))]
        crate::runtime::ws_util::check_headers(&self.http_headers)?;

        // No properties allowed with MQTT v3
        if !self.props.is_empty() && self.mqtt_version < MqttVersion::V5 {
            return Err(Error::WrongMqttVersion);
        }

        // The Will's v5 properties (delay interval, expiry, content type, …)
        // travel with it; like connect properties they need a v5 connection.
        if let Some(msg) = &self.will
            && !msg.props.is_empty()
            && self.mqtt_version < MqttVersion::V5
        {
            return Err(Error::WrongMqttVersion);
        }

        // An upper retry bound below the lower one has no coherent reading:
        // the backoff starts at the minimum and is then capped at the
        // maximum, so the delay would *shrink* on the first failure. There
        // is nothing to guess at, so say so rather than quietly reordering
        // the two. Only meaningful when reconnecting is enabled.
        if self.auto_reconnect && self.max_retry_interval < self.min_retry_interval {
            return Err(Error::InvalidArgument(
                "max_retry_interval is less than min_retry_interval".into(),
            ));
        }

        // A property the specification does not allow would be refused when
        // the CONNECT is encoded, failing the connect token; `check` is where
        // a configuration mistake is supposed to surface.
        if !self.props.is_valid() {
            return Err(Error::InvalidArgument(
                "the connect properties hold a value the specification does not allow".into(),
            ));
        }

        // Enhanced authentication needs both halves: a method for the broker
        // to run and a handler to answer its challenges. Either alone is a
        // configuration mistake that would otherwise surface as a puzzling
        // refusal from the broker, or as an ignored handler.
        match (
            crate::auth::auth_method(&self.props).is_some(),
            self.auth_handler.is_some(),
        ) {
            (true, false) => Err(Error::InvalidArgument(
                "connect properties name an AuthenticationMethod but no auth handler is set".into(),
            )),
            (false, true) => Err(Error::InvalidArgument(
                "an auth handler is set but the connect properties name no AuthenticationMethod"
                    .into(),
            )),
            _ => Ok(()),
        }
    }

    /// Gets the MQTT protocol version that should be used for the
    /// connection.
    pub fn mqtt_version(&self) -> MqttVersion {
        self.mqtt_version
    }

    /// Gets the MQTT protocol version as a raw integer.
    pub fn mqtt_version_raw(&self) -> u32 {
        self.mqtt_version as u32
    }

    /// Gets the "clean session" setting in the options.
    ///
    /// This is only used in MQTT v3 connections.
    pub fn clean_session(&self) -> bool {
        self.clean_start && self.mqtt_version < MqttVersion::V5
    }

    /// This sets the "clean session" behavior for connecting to the server.
    ///
    /// When set to true, this directs the server to throw away any state
    /// related to the client, as determined by the client identifier.
    /// When set to false, the server keeps the state information and
    /// resumes the previous session.
    ///
    /// This is only used in MQTT v3 connections.
    pub fn set_clean_session(&mut self, clean: bool) {
        self.clean_start = clean;
    }

    /// Gets the "clean start" setting in the options.
    ///
    /// This is only used in MQTT v5 connections.
    ///
    /// # Clean start and session expiry
    ///
    /// MQTT v5 replaced v3's single "clean session" flag with two
    /// independent controls, and both matter for whether a session survives:
    ///
    /// - **Clean Start** (this flag) decides what happens to an *existing*
    ///   session at connect time: `true` discards it and starts fresh,
    ///   `false` resumes it if the server still has one.
    /// - **Session Expiry Interval** (the `SessionExpiryInterval` property in
    ///   [`props`](ConnectOptions::props)) decides how long the server keeps
    ///   the session *after* the connection ends, in seconds. It defaults to
    ///   **0**, meaning the session ends with the connection.
    ///
    /// So `clean_start(false)` alone does not give a durable session: with
    /// the default expiry of 0 the server discards the state as soon as the
    /// client disconnects, and the next connect gets
    /// `session_present == false`. To resume across connections, set both —
    /// `clean_start(false)` and a non-zero `SessionExpiryInterval` (0xFFFFFFFF
    /// means "never expire"):
    ///
    /// ```no_run
    /// use paho_mqtt as mqtt;
    /// use mqtt::Property;
    ///
    /// let mut props = mqtt::Properties::new();
    /// // keep my session for an hour
    /// props.push(Property::SessionExpiryInterval(3600));
    ///
    /// let opts = mqtt::ConnectOptionsBuilder::new_v5()
    ///     .clean_start(false)
    ///     .properties(props)
    ///     .finalize();
    /// ```
    ///
    /// A session expiry may also be sent on DISCONNECT (in
    /// [`DisconnectOptions::props`](crate::DisconnectOptions)) to change the
    /// interval agreed at connect time, though it cannot revive a session
    /// that was established with an expiry of 0.
    ///
    /// The client keeps its half of the session by the same rules. When a
    /// connection closes with the interval at 0, its in-flight QoS 1/2
    /// publishes fail with [`Error::SessionNotResumed`](crate::Error) and
    /// nothing of the session is re-sent on a later connection; otherwise
    /// they wait for the next connection to continue the session.
    ///
    /// One rule is deliberately not enforced: MQTT-3.2.2-4 has a client
    /// with no session state close the connection if the server reports
    /// Session Present = 1. Taken literally, that refuses a process that
    /// starts again, without a persistence store, to collect the messages
    /// the server kept for it; the client carries on instead, as Paho C
    /// does.
    ///
    /// The client's own persistence is independent of all this: it stores
    /// in-flight QoS 1/2 messages so they can be re-sent, but only the
    /// server decides whether the *session* (subscriptions, queued messages)
    /// still exists. Check
    /// [`ConnectResponse::session_present`](crate::ConnectResponse) to find
    /// out which happened.
    pub fn clean_start(&self) -> bool {
        self.clean_start && self.mqtt_version >= MqttVersion::V5
    }

    /// This sets the "clean start" behavior for connecting to the server.
    ///
    /// When set to true, this directs the server to throw away any state
    /// related to the client, as determined by the client identifier.
    /// When set to false, the server keeps the state information and
    /// resumes the previous session.
    ///
    /// This is only used in MQTT v5 connections.
    pub fn set_clean_start(&mut self, clean: bool) {
        self.clean_start = clean;
    }

    /// Whether the options have TLS options for a secure connection
    pub fn has_tls_options(&self) -> bool {
        self.tls.is_some()
    }

    /// Whether the options have TLS options for a secure connection.
    #[deprecated(since = "2.0.0", note = "renamed to `has_tls_options`")]
    pub fn has_ssl_options(&self) -> bool {
        self.has_tls_options()
    }
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            keep_alive_interval: DEFAULT_KEEP_ALIVE_INTERVAL,
            clean_start: true,
            max_inflight: 65535,
            will: None,
            tls: None,
            user_name: None,
            password: None,
            timeout: Duration::from_secs(30),
            server_uris: Vec::new(),
            mqtt_version: MqttVersion::Default,
            auto_reconnect: false,
            min_retry_interval: Duration::from_secs(1),
            max_retry_interval: Duration::from_secs(60),
            props: Properties::default(),
            auth_handler: None,
            http_headers: HashMap::new(),
            http_proxy: String::new(),
            https_proxy: String::new(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Builder
/////////////////////////////////////////////////////////////////////////////

/// Builder to create the options to connect to the MQTT server.
#[derive(Default, Clone)]
pub struct ConnectOptionsBuilder(ConnectOptions);

impl ConnectOptionsBuilder {
    /// Creates a new `ConnectOptionsBuilder` with the default options, which
    /// are currently for MQTT v5.
    ///
    /// Use [`new_v3`](Self::new_v3) for a v3.1.1 connection. Unlike the v1.x
    /// client, nothing later in the builder changes the version: it is chosen
    /// here, or with [`with_mqtt_version`](Self::with_mqtt_version).
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new `ConnectOptionsBuilder` for MQTT v3.1.1.
    ///
    /// The default is v5, so a v3 connection must say so — either here or
    /// with [`with_mqtt_version`](Self::with_mqtt_version).
    pub fn new_v3() -> Self {
        Self(ConnectOptions::new_v3())
    }

    /// Creates a new `ConnectOptionsBuilder` for MQTT v5
    pub fn new_v5() -> Self {
        Self(ConnectOptions::new_v5())
    }

    /// Creates a new `ConnectOptionsBuilder` for connecting over websockets,
    /// with the longer keep-alive interval those need and, by default, MQTT
    /// v5. See [`new_ws_v3`](Self::new_ws_v3) for v3.1.1.
    pub fn new_ws() -> Self {
        Self(ConnectOptions::new_ws())
    }

    /// Creates a new `ConnectOptionsBuilder` for connecting to MQTT v3.1.1
    /// over websockets.
    pub fn new_ws_v3() -> Self {
        Self(ConnectOptions::new_ws_v3())
    }

    /// Creates a new `ConnectOptionsBuilder` for connecting to MQTT v5
    /// over websockets.
    pub fn new_ws_v5() -> Self {
        Self(ConnectOptions::new_ws_v5())
    }

    /// Creates a new set of `ConnectOptionsBuilder` for the specified MQTT
    /// protocol version.
    ///
    /// Note that this will initialize the options with the requested version,
    /// even if it's not currently known. The server will likely reject a
    /// request with a bad protocol version, however.
    pub fn with_mqtt_version<V>(ver: V) -> Self
    where
        V: Into<MqttVersion>,
    {
        Self(ConnectOptions::with_mqtt_version(ver))
    }

    /// Sets the keep alive interval for the client session.
    ///
    /// The protocol carries this as a whole number of seconds, so the
    /// interval is rounded: a fractional part is dropped, and a non-zero
    /// interval shorter than a second becomes one second rather than zero,
    /// which would switch keep-alive off instead of speeding it up. An
    /// interval over `u16::MAX` seconds saturates there.
    ///
    /// `Duration::ZERO` **disables** keep-alive: no PINGREQ is sent and the
    /// server is told not to expect one. Note that v1.x of this library
    /// clamped zero to one second, so it had no way to turn keep-alive off.
    ///
    /// # Arguments
    ///
    /// `keep_alive_interval` The maximum time that should pass without
    ///                       communication between the client and server.
    ///                       This has a resolution in seconds.
    pub fn keep_alive_interval(&mut self, keep_alive_interval: Duration) -> &mut Self {
        self.0.keep_alive_interval = keep_alive_interval;
        self
    }

    /// Sets the 'clean session' flag to send to the broker.
    ///
    /// This is v3's name for the flag that v5 calls 'clean start', and both
    /// setters write the same field, so either spelling works on either
    /// version. Unlike the v1.x client, **this does not change the protocol
    /// version**: choose that with the constructor or
    /// [`with_mqtt_version`](Self::with_mqtt_version).
    ///
    /// The two getters are version-filtered views of the one flag, so on a v5
    /// connection [`ConnectOptions::clean_session`] reads `false` however the
    /// flag was set, and on v3 [`ConnectOptions::clean_start`] does.
    ///
    /// # Arguments
    ///
    /// `clean` Whether the broker should remove any previously-stored
    ///         information for this client.
    pub fn clean_session(&mut self, clean: bool) -> &mut Self {
        self.0.clean_start = clean;
        self
    }

    /// Sets the 'clean start' flag to send to the broker.
    ///
    /// This is v5's name for the flag that v3 calls 'clean session', and both
    /// setters write the same field, so either spelling works on either
    /// version. Unlike the v1.x client, **this does not change the protocol
    /// version**: choose that with the constructor or
    /// [`with_mqtt_version`](Self::with_mqtt_version).
    ///
    /// On v5, a clean start alone does not keep the session from outliving
    /// the connection; see [`ConnectOptions::clean_start`] for how it and the
    /// Session Expiry Interval decide that together.
    ///
    /// # Arguments
    ///
    /// `clean` Whether the broker should remove any previously-stored
    ///         information for this client.
    pub fn clean_start(&mut self, clean: bool) -> &mut Self {
        self.0.clean_start = clean;
        self
    }

    /// Sets the maximum number of in-flight messages that can be
    /// simultaneously handled by this client.
    ///
    /// Publishes beyond the limit wait in the client until earlier ones are
    /// acknowledged. With MQTT v5 the server's Receive Maximum narrows the
    /// limit further.
    ///
    /// The protocol itself caps it: every QoS 1/2 message in flight holds a
    /// 16-bit packet identifier, and 0 is reserved, so no more than 65 535
    /// can be outstanding (fewer while subscribe and unsubscribe requests
    /// hold identifiers too). 0, like the default of 65 535, means that many.
    ///
    /// # Arguments
    ///
    /// `max_inflight` The maximum number of messages that can be in-flight
    ///                at any given time with this client.
    pub fn max_inflight(&mut self, max_inflight: u16) -> &mut Self {
        self.0.max_inflight = max_inflight;
        self
    }

    /// Sets the LWT message for the connection.
    ///
    /// # Arguments
    ///
    /// `will` The LWT options for the connection.
    pub fn will_message<M>(&mut self, will: M) -> &mut Self
    where
        M: Into<Option<Message>>,
    {
        self.0.will = will.into();
        self
    }

    /// Sets the TLS options for the connection.
    ///
    /// # Arguments
    ///
    /// `tls` The TLS options for the connection.
    pub fn tls_options<S>(&mut self, tls: S) -> &mut Self
    where
        S: Into<Option<TlsOptions>>,
    {
        self.0.tls = tls.into();
        self
    }

    /// Sets the user name for authentication with the broker.
    /// This works with the password.
    ///
    /// # Arguments
    ///
    /// `user_name` The user name to send to the broker.
    ///
    pub fn user_name<S>(&mut self, user_name: S) -> &mut Self
    where
        S: Into<String>,
    {
        self.0.user_name = Some(user_name.into());
        self
    }

    /// Clears the user name.
    /// This sets the option to `None`
    pub fn clear_user_name(&mut self) -> &mut Self {
        self.0.user_name = None;
        self
    }

    /// Sets the password for authentication with the broker.
    /// This works with the user name.
    ///
    /// # Arguments
    ///
    /// `password` The password to send to the broker.
    ///
    pub fn password<P>(&mut self, password: P) -> &mut Self
    where
        P: Into<Vec<u8>>,
    {
        self.0.password = Some(password.into());
        self
    }

    /// Clear the password.
    /// This sets the option to `None`
    pub fn clear_password(&mut self) -> &mut Self {
        self.0.password = None;
        self
    }

    /// Sets the user name and password for authentication with the broker.
    ///
    /// # Arguments
    ///
    /// `user_name` The user name to send to the broker.
    /// `password` The password to send to the broker.
    ///
    pub fn user_name_password<S, P>(&mut self, user_name: S, password: P) -> &mut Self
    where
        S: Into<String>,
        P: Into<Vec<u8>>,
    {
        self.0.user_name = Some(user_name.into());
        self.0.password = Some(password.into());
        self
    }

    /// Clears the user name and password.
    /// This sets both options to `None`
    pub fn clear_user_name_password(&mut self) -> &mut Self {
        self.0.user_name = None;
        self.0.password = None;
        self
    }

    /// Sets the time interval to allow the connection to complete.
    ///
    /// This is a timer the client runs itself, so the interval is used as
    /// given, to whatever resolution the runtime provides; sub-second
    /// timeouts work. The default is 30 seconds.
    ///
    /// `Duration::ZERO` **disables** the timeout: the attempt then takes as
    /// long as the transport takes, which for a server that accepts the
    /// connection and then goes quiet during a TLS handshake may be
    /// indefinitely. It is the only way to opt out of a deadline, so it is
    /// passed through rather than overridden — note that v1.x of this
    /// library clamped zero to one second.
    ///
    /// # Arguments
    ///
    /// `timeout` The time interval to allow the connection to complete.
    ///
    pub fn connect_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.0.timeout = timeout;
        self
    }

    /// Specify the servers to which the client will connect.
    ///
    /// # Arguments
    ///
    /// `server_uris` The addresses of the brokers to which this client
    ///               should connect.
    //
    pub fn server_uris<T>(&mut self, server_uris: &[T]) -> &mut Self
    where
        T: AsRef<str>,
    {
        self.0.server_uris = server_uris
            .iter()
            .map(|s| String::from(s.as_ref()))
            .collect();
        self
    }

    /// Sets the client to automatically reconnect if the connection is lost.
    ///
    /// The first retry waits `min_retry_interval`, and each failed retry
    /// doubles the wait until it reaches `max_retry_interval`, where it
    /// stays. A successful connection resets it. These are the client's own
    /// timers, so sub-second intervals work.
    ///
    /// A `min_retry_interval` of `Duration::ZERO` means the first retry
    /// waits for nothing; the backoff then starts at 100 ms rather than
    /// staying at zero, since a doubling sequence cannot climb out of zero
    /// and the client would otherwise retry without pause for as long as
    /// the broker stayed down.
    ///
    /// A `max_retry_interval` below `min_retry_interval` is rejected, by
    /// [`ConnectOptions::check`] and so by `connect()`: the delay would
    /// shrink on the first failure, and there is no sensible way to guess
    /// which of the two was meant.
    ///
    /// # Arguments
    ///
    /// `min_retry_interval` The minimum retry interval. Doubled on each
    ///                      failed retry.
    /// `max_retry_interval` The maximum retry interval. Doubling stops here
    ///                      on failed retries.
    pub fn automatic_reconnect(
        &mut self,
        min_retry_interval: Duration,
        max_retry_interval: Duration,
    ) -> &mut Self {
        self.0.auto_reconnect = true;
        self.0.min_retry_interval = min_retry_interval;
        self.0.max_retry_interval = max_retry_interval;
        self
    }

    /// Sets the handler for an MQTT v5 enhanced-authentication exchange.
    ///
    /// The connect properties must also name an `AuthenticationMethod`; see
    /// [`AuthHandler`] for the whole picture and an example.
    pub fn auth_handler<F>(&mut self, handler: F) -> &mut Self
    where
        F: Fn(&AuthChallenge<'_>) -> Result<Vec<u8>> + Send + Sync + 'static,
    {
        self.0.auth_handler = Some(AuthHandler::new(handler));
        self
    }

    /// Sets the collection of properties for the connections.
    ///
    /// Properties are MQTT v5 only, and — unlike the v1.x client — setting
    /// them does not switch the options to v5. A non-empty collection on a v3
    /// connection fails the connect token with
    /// [`Error::WrongMqttVersion`] rather than
    /// being dropped on the way to the broker.
    ///
    /// # Arguments
    ///
    /// `props` The collection of properties to include with the connect message.
    pub fn properties(&mut self, props: Properties) -> &mut Self {
        self.0.props = props;
        self
    }

    /// Sets the additional HTTP headers that will be sent in the
    /// WebSocket opening handshake.
    ///
    /// `connect()` fails with [`Error::InvalidArgument`] if a name or value
    /// is not one HTTP allows (a space in a name, a line break in a value).
    pub fn http_headers(&mut self, headers: HashMap<String, String>) -> &mut Self {
        self.0.http_headers = headers;
        self
    }

    /// Sets the additional HTTP headers that will be sent in the
    /// WebSocket opening handshake.
    pub fn http_headers_from_coll<N, V>(&mut self, headers: &[(N, V)]) -> &mut Self
    where
        N: AsRef<str>,
        V: AsRef<str>,
    {
        let mut hdrs = HashMap::new();
        for (n, v) in headers {
            hdrs.insert(n.as_ref().into(), v.as_ref().into());
        }
        self.http_headers(hdrs)
    }

    /// Sets an HTTP proxy for websockets.
    ///
    /// # Arguments
    ///
    /// `proxy` The HTTP proxy, as `http://user:password@host:port`, the
    /// credentials and port optional. Characters the user name or password
    /// cannot hold in a URI, such as `@` or `:`, are percent-encoded
    /// (`%40`, `%3A`), and are decoded before they are sent to the proxy.
    ///
    /// `connect()` fails with [`Error::InvalidArgument`]
    /// if this cannot be parsed as a proxy URI.
    ///
    pub fn http_proxy<S>(&mut self, proxy: S) -> &mut Self
    where
        S: Into<String>,
    {
        self.0.http_proxy = proxy.into();
        self
    }

    /// Sets a secure HTTPS proxy for websockets.
    ///
    /// # Arguments
    ///
    /// `proxy` The HTTPS proxy
    ///
    /// `connect()` fails with [`Error::InvalidArgument`]
    /// if this cannot be parsed as a proxy URI.
    ///
    pub fn https_proxy<S>(&mut self, proxy: S) -> &mut Self
    where
        S: Into<String>,
    {
        self.0.https_proxy = proxy.into();
        self
    }

    /// Finalize the builder to create the connect options.
    pub fn finalize(&self) -> ConnectOptions {
        self.0.clone()
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Property, QOS_1};

    const NAME: &str = "some-random-name";
    const PSWD: &str = "some-random-password";

    fn assert_implements_send<T: Send>() {}
    fn assert_implements_sync<T: Sync>() {}

    #[test]
    fn test_new_versions() {
        // new() defaults to v5: clean start, no clean session
        let opts = ConnectOptions::new();
        assert_eq!(MqttVersion::V5, opts.mqtt_version());
        assert!(!opts.clean_session());
        assert!(opts.clean_start());

        // new_v3() explicitly selects v3.1.1: clean session, no clean start
        let opts = ConnectOptions::new_v3();
        assert_eq!(MqttVersion::V3_1_1, opts.mqtt_version());
        assert!(opts.clean_session());
        assert!(!opts.clean_start());

        // new_v5() explicitly selects v5: clean start, no clean session
        let opts = ConnectOptions::new_v5();
        assert_eq!(MqttVersion::V5, opts.mqtt_version());
        assert!(!opts.clean_session());
        assert!(opts.clean_start());
    }

    #[test]
    fn test_with_mqtt_version_stores_requested_version() {
        for ver in [MqttVersion::V3_1, MqttVersion::V3_1_1, MqttVersion::V5] {
            assert_eq!(ConnectOptions::with_mqtt_version(ver).mqtt_version(), ver);
            assert_eq!(
                ConnectOptionsBuilder::with_mqtt_version(ver)
                    .finalize()
                    .mqtt_version(),
                ver
            );
        }
    }

    // Determine that the options can be sent across threads.
    // As long as it compiles, this indicates that ConnectOptions
    // implements the traits.
    #[test]
    fn test_send() {
        assert_implements_send::<ConnectOptions>();
        assert_implements_sync::<ConnectOptions>();
    }

    #[test]
    fn test_set_and_clear() {
        let msg = Message::new("lwt", "disconnected", QOS_1);
        let ssl = TlsOptions::default();

        // user_name: set then clear
        let opts = ConnectOptionsBuilder::new().user_name(NAME).finalize();
        assert_eq!(opts.user_name.as_deref(), Some(NAME));

        let opts = ConnectOptionsBuilder::new()
            .user_name(NAME)
            .clear_user_name()
            .finalize();
        assert!(opts.user_name.is_none());

        // password: set then clear
        let opts = ConnectOptionsBuilder::new().password(PSWD).finalize();
        assert_eq!(opts.password.as_deref(), Some(PSWD.as_bytes()));

        let opts = ConnectOptionsBuilder::new()
            .password(PSWD)
            .clear_password()
            .finalize();
        assert!(opts.password.is_none());

        // will_message: set then clear with None
        let opts = ConnectOptionsBuilder::new()
            .will_message(msg.clone())
            .finalize();
        assert!(opts.will.is_some());

        let opts = ConnectOptionsBuilder::new()
            .will_message(msg)
            .will_message(None)
            .finalize();
        assert!(opts.will.is_none());

        // tls_options: set then clear with None
        let opts = ConnectOptionsBuilder::new().tls_options(ssl).finalize();
        assert!(opts.tls.is_some());

        let opts = ConnectOptionsBuilder::new()
            .tls_options(TlsOptions::default())
            .tls_options(None)
            .finalize();
        assert!(opts.tls.is_none());
    }

    /// Everything that is configuration survives a round trip. The
    /// authentication handler does not, being code.
    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_round_trip() {
        let msg = Message::new("lwt", "gone", QOS_1);
        let opts = ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(45))
            .clean_start(false)
            .max_inflight(7)
            .will_message(msg)
            .tls_options(TlsOptions::default())
            .user_name_password(NAME, PSWD)
            .connect_timeout(Duration::from_secs(9))
            .server_uris(&["mqtt://a:1883", "mqtt://b:1883"])
            .automatic_reconnect(Duration::from_secs(2), Duration::from_secs(30))
            .http_proxy("http://proxy:8080")
            .auth_handler(|_| Ok(Vec::new()))
            .finalize();
        assert!(opts.auth_handler.is_some());

        let back: ConnectOptions =
            serde_json::from_str(&serde_json::to_string(&opts).unwrap()).unwrap();
        assert_eq!(back.keep_alive_interval, Duration::from_secs(45));
        assert!(!back.clean_start);
        assert_eq!(back.max_inflight, 7);
        assert_eq!(back.will.as_ref().map(|w| w.topic()), Some("lwt"));
        assert!(back.tls.is_some());
        assert_eq!(back.user_name.as_deref(), Some(NAME));
        assert_eq!(back.password.as_deref(), Some(PSWD.as_bytes()));
        assert_eq!(back.timeout, Duration::from_secs(9));
        assert_eq!(back.server_uris.len(), 2);
        assert!(back.auto_reconnect);
        assert_eq!(back.min_retry_interval, Duration::from_secs(2));
        assert_eq!(back.http_proxy, "http://proxy:8080");
        assert_eq!(back.mqtt_version, opts.mqtt_version);
        assert!(back.auth_handler.is_none(), "a handler cannot be read back");
    }

    /// Since the handler is dropped, options that name an authentication
    /// method no longer pass `check` after a round trip. That is the same
    /// mistake as configuring a method with no handler, reported the same way.
    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_drops_the_auth_pair() {
        let mut props = Properties::new();
        props.push(Property::AuthenticationMethod("SCRAM-SHA-1".into()));
        let opts = ConnectOptionsBuilder::new()
            .properties(props)
            .auth_handler(|_| Ok(Vec::new()))
            .finalize();
        opts.check().expect("a method with a handler is valid");

        let back: ConnectOptions =
            serde_json::from_str(&serde_json::to_string(&opts).unwrap()).unwrap();
        assert!(back.check().is_err(), "the method now has no handler");
    }

    /// An empty object is the default options, so a configuration file need
    /// only name what it changes.
    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_partial_input_uses_defaults() {
        let opts: ConnectOptions = serde_json::from_str(r#"{"max_inflight":3}"#).unwrap();
        let dflt = ConnectOptions::default();
        assert_eq!(opts.max_inflight, 3);
        assert_eq!(opts.keep_alive_interval, dflt.keep_alive_interval);
        assert_eq!(opts.mqtt_version, dflt.mqtt_version);
        assert_eq!(opts.clean_start, dflt.clean_start);
    }

    /// A property the specification does not allow is a configuration
    /// mistake, so `check` reports it rather than letting the connect token
    /// fail when the CONNECT is encoded.
    #[test]
    fn test_check_rejects_invalid_properties() {
        let mut props = Properties::new();
        // `push` takes it and marks the list; `try_push` would have refused.
        props.push(Property::ReceiveMaximum(0));
        assert!(!props.is_valid());

        let opts = ConnectOptionsBuilder::new_v5().properties(props).finalize();
        assert!(matches!(opts.check(), Err(Error::InvalidArgument(_))));

        // A sound set still passes.
        let mut good = Properties::new();
        good.try_push(Property::ReceiveMaximum(20)).unwrap();
        let opts = ConnectOptionsBuilder::new_v5().properties(good).finalize();
        assert!(opts.check().is_ok());
    }

    // The keep-alive goes on the wire as whole seconds, so the conversion
    // has to round. The case that matters is a non-zero interval under a
    // second: truncating it to zero would *disable* keep-alive, which is
    // the opposite of asking for a fast one.
    #[test]
    fn test_keep_alive_secs_rounding() {
        let secs = |d| {
            ConnectOptionsBuilder::new()
                .keep_alive_interval(d)
                .finalize()
                .keep_alive_secs()
        };

        // Zero stays zero: that is how keep-alive is switched off.
        assert_eq!(0, secs(Duration::ZERO));

        // Anything non-zero below a second rounds up to one.
        assert_eq!(1, secs(Duration::from_nanos(1)));
        assert_eq!(1, secs(Duration::from_millis(1)));
        assert_eq!(1, secs(Duration::from_millis(500)));
        assert_eq!(1, secs(Duration::from_millis(999)));

        // At or above a second, the fractional part is dropped.
        assert_eq!(1, secs(Duration::from_secs(1)));
        assert_eq!(1, secs(Duration::from_millis(1900)));
        assert_eq!(45, secs(Duration::from_millis(45_999)));
        assert_eq!(60, secs(Duration::from_secs(60)));

        // The field is a u16, so a longer interval saturates.
        assert_eq!(u16::MAX, secs(Duration::from_secs(u16::MAX as u64)));
        assert_eq!(u16::MAX, secs(Duration::from_secs(1_000_000)));

        // And the default.
        assert_eq!(60, ConnectOptions::new().keep_alive_secs());
    }

    // A connect timeout is the client's own timer, so it is used as given,
    // and zero means no deadline at all.
    #[test]
    fn test_connect_timeout_passed_through() {
        let opts = ConnectOptionsBuilder::new()
            .connect_timeout(Duration::from_millis(250))
            .finalize();
        assert_eq!(Duration::from_millis(250), opts.timeout);

        let opts = ConnectOptionsBuilder::new()
            .connect_timeout(Duration::ZERO)
            .finalize();
        assert!(opts.timeout.is_zero());

        assert_eq!(Duration::from_secs(30), ConnectOptions::new().timeout);
    }

    // An upper retry bound below the lower one would make the delay shrink
    // on the first failure, so it is refused rather than reordered.
    #[test]
    fn test_retry_interval_order_is_checked() {
        let opts = ConnectOptionsBuilder::new()
            .automatic_reconnect(Duration::from_secs(10), Duration::from_secs(2))
            .finalize();
        assert!(
            matches!(opts.check(), Err(Error::InvalidArgument(msg)) if msg.contains("max_retry_interval"))
        );

        // Equal is fine: a fixed retry interval with no backoff.
        let opts = ConnectOptionsBuilder::new()
            .automatic_reconnect(Duration::from_secs(5), Duration::from_secs(5))
            .finalize();
        assert!(opts.check().is_ok());

        // A zero minimum is allowed; the backoff handles it.
        let opts = ConnectOptionsBuilder::new()
            .automatic_reconnect(Duration::ZERO, Duration::from_secs(60))
            .finalize();
        assert!(opts.check().is_ok());

        // Without auto-reconnect the intervals are unused, so the order of
        // two values nothing reads is not worth failing a connect over.
        let mut opts = ConnectOptions::new();
        opts.min_retry_interval = Duration::from_secs(10);
        opts.max_retry_interval = Duration::from_secs(2);
        assert!(!opts.auto_reconnect);
        assert!(opts.check().is_ok());
    }
}
