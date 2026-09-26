// paho-mqtt/src/client.rs
//
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

//! This contains the synchronous `Client` interface for the Paho MQTT Rust
//! library.
//!
//! This is a simple convenience wrapper around the asynchronous API in which
//! each function calls the underlying async function, and then blocks waiting
//! for it to complete.
//!
//! The synchronous calls use a default timeout

use crate::{
    AsyncClient, AsyncReceiver, ConnectOptions, ConnectResponse, CreateOptions, DeliveryToken,
    DisconnectOptions, Error, Event, Message, MqttVersion, Properties, PublishResponse, QoS,
    ReasonCode, Result, SubscribeOptions, SubscribeResponse, SyncReceiver, UserData,
};
use std::time::Duration;

/////////////////////////////////////////////////////////////////////////////
// Client

/// MQTT Client with a synchronous (blocking) API.
/// This is simply a convenience wrapper around the asynchronous API,
/// providing blocking calls with timeouts.
#[derive(Clone)]
pub struct Client {
    /// The underlying asynchronous client.
    pub(crate) cli: AsyncClient,
    /// The default timeout for synchronous calls.
    pub(crate) timeout: Duration,
}

impl Client {
    /// Creates a new MQTT client which can connect to an MQTT broker.
    ///
    /// # Runtime
    ///
    /// Every call on a `Client` blocks the calling thread until the operation
    /// finishes, so the event loop must be driven by some *other* thread. To
    /// guarantee that, a `Client` left on the default
    /// [`RuntimeMode::Auto`](crate::RuntimeMode::Auto) is switched to
    /// [`Shared`](crate::RuntimeMode::Shared), which runs the event loop on
    /// the library's own runtime.
    ///
    /// This matters when a `Client` is created inside an async runtime.
    /// `Auto` would borrow that runtime, and then a blocking call made from
    /// it — on a `current_thread` runtime, or from every worker of a small
    /// multi-threaded one — would stop the very thread the event loop needs
    /// in order to make progress, and the call would never return.
    ///
    /// Naming a mode explicitly is respected as given, including
    /// [`ApplicationRuntime`](crate::RuntimeMode::ApplicationRuntime); if you
    /// ask for the application's runtime you are taken at your word, and it
    /// is on you to leave a thread free to drive it. Inside async code,
    /// prefer [`AsyncClient`] and `.await` over a `Client`.
    pub fn new<T>(opts: T) -> Result<Client>
    where
        T: Into<CreateOptions>,
    {
        let mut opts: CreateOptions = opts.into();
        if matches!(opts.runtime_mode, crate::RuntimeMode::Auto) {
            opts.runtime_mode = crate::RuntimeMode::Shared(0);
        }
        let async_cli = AsyncClient::new(opts)?;

        let cli = Client {
            cli: async_cli,
            timeout: Duration::from_secs(5 * 60),
        };
        Ok(cli)
    }

    /// Gets the default timeout used for synchronous operations.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Sets the default timeout used for synchronous operations.
    ///
    /// ## Arguments
    ///
    /// * `timeout` The timeout to use for synchronous calls, like
    ///   connect(), disconnect(), publish(), etc.
    ///
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout
    }

    /// Connects to an MQTT broker using the specified connect options.
    pub fn connect<T>(&self, opt_opts: T) -> Result<ConnectResponse>
    where
        T: Into<Option<ConnectOptions>>,
    {
        self.cli.connect(opt_opts).wait_for(self.timeout)
    }

    /// Disconnects from the MQTT broker.
    ///
    /// ## Arguments
    ///
    /// * `opt_opts` Optional disconnect options. Specifying `None` will use
    ///   default of immediate (zero timeout) disconnect.
    ///
    pub fn disconnect<T>(&self, opt_opts: T) -> Result<()>
    where
        T: Into<Option<DisconnectOptions>>,
    {
        self.cli.disconnect(opt_opts).wait_for(self.timeout)?;
        Ok(())
    }

    /// Disconnect from the MQTT broker with a timeout.
    /// This will delay the disconnect for up to the specified timeout to
    /// allow in-flight messages to complete.
    /// This is the same as calling disconnect with options specifying a
    /// timeout.
    ///
    /// # Arguments
    ///
    /// * `timeout` The amount of time to wait for the disconnect. This has
    ///   a resolution in milliseconds.
    ///
    pub fn disconnect_after(&self, timeout: Duration) -> Result<()> {
        self.cli.disconnect_after(timeout).wait_for(self.timeout)?;
        Ok(())
    }

    /// Attempts to reconnect to the broker.
    /// This can only be called after a connection was initially made or
    /// attempted. It will retry with the same connect options.
    pub fn reconnect(&self) -> Result<ConnectResponse> {
        self.cli.reconnect().wait_for(self.timeout)
    }

    /// Determines if this client is currently connected to an MQTT broker.
    pub fn is_connected(&self) -> bool {
        self.cli.is_connected()
    }

    /// Publishes a message to an MQTT broker
    pub fn publish(&self, msg: Message) -> Result<PublishResponse> {
        self.cli.publish(msg).wait_for(self.timeout)
    }

    /// Subscribes to a single topic.
    ///
    /// # Arguments
    ///
    /// * `topic` The topic name
    /// * `qos` The quality of service requested for messages
    ///
    pub fn subscribe<Q>(&self, topic: &str, qos: Q) -> Result<SubscribeResponse>
    where
        Q: TryInto<QoS>,
        Q::Error: Into<Error>,
    {
        self.cli.subscribe(topic, qos).wait_for(self.timeout)
    }

    /// Subscribes to a single topic with v5 options
    ///
    /// # Arguments
    ///
    /// * `topic` The topic name
    /// * `qos` The quality of service requested for messages
    /// * `opts` Options for the subscription
    /// * `props` MQTT v5 properties
    ///
    pub fn subscribe_with_options<S, Q, T, P>(
        &self,
        topic: S,
        qos: Q,
        opts: T,
        props: P,
    ) -> Result<SubscribeResponse>
    where
        S: Into<String>,
        Q: TryInto<QoS>,
        Q::Error: Into<Error>,
        T: Into<SubscribeOptions>,
        P: Into<Option<Properties>>,
    {
        self.cli
            .subscribe_with_options(topic, qos, opts, props)
            .wait_for(self.timeout)
    }

    /// Subscribes to multiple topics simultaneously.
    ///
    /// # Arguments
    ///
    /// * `topic` The topic name
    /// * `qos` The quality of service requested for messages
    ///
    pub fn subscribe_many<T, Q>(&self, topics: &[T], qos: &[Q]) -> Result<SubscribeResponse>
    where
        T: AsRef<str>,
        Q: TryInto<QoS> + Copy,
        Q::Error: Into<Error>,
    {
        self.cli.subscribe_many(topics, qos).wait_for(self.timeout)
    }

    /// Subscribes to multiple topics simultaneously using the same QoS
    /// for all of them.
    ///
    /// # Arguments
    ///
    /// * `topics` The collection of topic names
    /// * `qos` The quality of service requested for all messages
    ///
    pub fn subscribe_many_same_qos<T, Q>(&self, topics: &[T], qos: Q) -> Result<SubscribeResponse>
    where
        T: AsRef<str>,
        Q: TryInto<QoS>,
        Q::Error: Into<Error>,
    {
        self.cli
            .subscribe_many_same_qos(topics, qos)
            .wait_for(self.timeout)
    }

    /// Subscribes to multiple topics simultaneously with options.
    ///
    /// # Arguments
    ///
    /// * `topics` The collection of topic names
    /// * `qos` The quality of service requested for messages
    /// * `opts` Subscribe options (one per topic)
    /// * `props` MQTT v5 properties
    ///
    pub fn subscribe_many_with_options<T, Q, P>(
        &self,
        topics: &[T],
        qos: &[Q],
        opts: &[SubscribeOptions],
        props: P,
    ) -> Result<SubscribeResponse>
    where
        T: AsRef<str>,
        Q: TryInto<QoS> + Copy,
        Q::Error: Into<Error>,
        P: Into<Option<Properties>>,
    {
        self.cli
            .subscribe_many_with_options(topics, qos, opts, props)
            .wait_for(self.timeout)
    }

    /// Unsubscribes from a single topic.
    ///
    /// # Arguments
    ///
    /// * `topic` The topic to unsubscribe. It must match a topic from a
    ///   previous subscribe.
    ///
    pub fn unsubscribe(&self, topic: &str) -> Result<()> {
        self.cli.unsubscribe(topic).wait_for(self.timeout)?;
        Ok(())
    }

    /// Unsubscribes from a single topic.
    ///
    /// # Arguments
    ///
    /// * `topic` The topic to unsubscribe. It must match a topic from a
    ///   previous subscribe.
    /// * `props` MQTT v5 properties for the unsubscribe.
    ///
    pub fn unsubscribe_with_options<S>(&self, topic: S, props: Properties) -> Result<()>
    where
        S: Into<String>,
    {
        self.cli
            .unsubscribe_with_options(topic, props)
            .wait_for(self.timeout)?;
        Ok(())
    }

    /// Unsubscribes from multiple topics simultaneously.
    ///
    /// # Arguments
    ///
    /// * `topic` The topics to unsubscribe. Each must match a topic from a
    ///   previous subscribe.
    ///
    pub fn unsubscribe_many<T>(&self, topics: &[T]) -> Result<()>
    where
        T: AsRef<str>,
    {
        self.cli.unsubscribe_many(topics).wait_for(self.timeout)?;
        Ok(())
    }

    /// Unsubscribes from multiple topics simultaneously.
    ///
    /// # Arguments
    ///
    /// * `topic` The topics to unsubscribe. Each must match a topic from a
    ///   previous subscribe.
    /// * `props` MQTT v5 properties for the unsubscribe.
    ///
    pub fn unsubscribe_many_with_options<T>(&self, topics: &[T], props: Properties) -> Result<()>
    where
        T: AsRef<str>,
    {
        self.cli
            .unsubscribe_many_with_options(topics, props)
            .wait_for(self.timeout)?;
        Ok(())
    }

    /// Starts the client consuming messages.
    ///
    /// This starts the client receiving messages and placing them into a
    /// channel. It returns the receiving-end of the channel for the
    /// application to get the messages.
    ///
    /// This can be called at any time after the client is created, but it
    /// should be called before subscribing to any topics, otherwise messages
    /// can be lost.
    //
    pub fn start_consuming(&self) -> SyncReceiver<Option<Message>> {
        self.cli.start_consuming()
    }

    /// Stops the client consumer.
    pub fn stop_consuming(&self) {
        self.cli.stop_consuming();
    }

    /// Creates a futures stream for consuming events.
    ///
    /// This will install an internal callback to receive the incoming
    /// events from the client, and return the receive side of the channel.
    /// The stream will stay open for the life of the client.
    ///
    /// The stream will rely on a bounded channel with the given buffer
    /// capacity if 'buffer_lim' is 'Some' or will rely on an unbounded channel
    /// if 'buffer_lim' is 'None'.
    ///
    /// It's a best practice to open the stream _before_ connecting to the
    /// server. When using persistent (non-clean) sessions, messages could
    /// arriving as soon as the connection is made - even before the
    /// connect() call returns.
    pub fn start_consuming_events<L>(&mut self, buffer_lim: L) -> SyncReceiver<Event>
    where
        L: Into<Option<usize>>,
    {
        self.cli.start_consuming_events(buffer_lim)
    }

    /// Returns the client ID used for the client instance.
    ///
    /// See [`AsyncClient::client_id`] — a server-assigned identifier
    /// replaces the configured one once a connection has been made with an
    /// empty ID.
    pub fn client_id(&self) -> String {
        self.cli.client_id()
    }

    /// Returns the server URI used for connection.
    pub fn server_uri(&self) -> &str {
        self.cli.server_uri()
    }

    /// Returns the MQTT protocol version negotiated for the last connection.
    pub fn mqtt_version(&self) -> MqttVersion {
        self.cli.mqtt_version()
    }

    /// Returns the raw integer MQTT version negotiated for the last connection.
    pub fn mqtt_version_raw(&self) -> u32 {
        self.cli.mqtt_version_raw()
    }

    /// Gets a copy of the options used in the last connection attempt.
    pub fn connect_options(&self) -> ConnectOptions {
        self.cli.connect_options()
    }

    /// Gets access to the user-defined data stored in the client.
    pub fn user_data(&self) -> Option<&UserData> {
        self.cli.user_data()
    }

    /// Attempts to publish a message without blocking or queuing.
    ///
    /// Returns a `DeliveryToken` immediately if the message was accepted,
    /// or an error if the client's command channel is full or closed.
    pub fn try_publish(&self, msg: Message) -> Result<DeliveryToken> {
        self.cli.try_publish(msg)
    }

    /// Sets the callback for when the connection is established with the broker.
    pub fn set_connected_callback<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient, &ConnectResponse) + Send + 'static,
    {
        self.cli.set_connected_callback(cb);
    }

    /// Removes the callback for when the connection is established.
    pub fn remove_connected_callback(&self) {
        self.cli.remove_connected_callback();
    }

    /// Sets the callback for when the connection to the broker is lost.
    pub fn set_connection_lost_callback<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient) + Send + 'static,
    {
        self.cli.set_connection_lost_callback(cb);
    }

    /// Removes the callback for when the connection is lost.
    pub fn remove_connection_lost_callback(&self) {
        self.cli.remove_connection_lost_callback();
    }

    /// Sets the callback for when a DISCONNECT packet arrives from the broker.
    pub fn set_disconnected_callback<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient, Properties, ReasonCode) + Send + 'static,
    {
        self.cli.set_disconnected_callback(cb);
    }

    /// Removes the callback for when a DISCONNECT packet is received.
    pub fn remove_disconnected_callback(&self) {
        self.cli.remove_disconnected_callback();
    }

    /// Sets the callback for when a message arrives from the broker.
    pub fn set_message_callback<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient, Option<Message>) + Send + 'static,
    {
        self.cli.set_message_callback(cb);
    }

    /// Removes the callback for when a message arrives.
    pub fn remove_message_callback(&self) {
        self.cli.remove_message_callback();
    }

    /// Creates an async message stream.
    ///
    /// Returns the receive end of a channel delivering `Option<Message>` values.
    /// Pass `Some(n)` for a bounded channel of capacity `n`, or `None` for
    /// unbounded.
    pub fn get_stream<L>(&mut self, buffer_lim: L) -> AsyncReceiver<Option<Message>>
    where
        L: Into<Option<usize>>,
    {
        self.cli.get_stream(buffer_lim)
    }

    /// Creates an async event stream.
    ///
    /// Returns the receive end of a channel delivering [`Event`] values.
    /// Pass `Some(n)` for a bounded channel of capacity `n`, or `None` for
    /// unbounded.
    pub fn get_event_stream<L>(&self, buffer_lim: L) -> AsyncReceiver<Event>
    where
        L: Into<Option<usize>>,
    {
        self.cli.get_event_stream(buffer_lim)
    }

    /// Removes all registered callbacks atomically under a single lock.
    pub fn remove_callbacks(&self) {
        self.cli.remove_callbacks();
    }

    /// Stops the event stream, closing the channel.
    pub fn stop_stream(&self) {
        self.cli.stop_stream();
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_options::CreateOptionsBuilder;
    use std::sync::Arc;
    use std::thread;

    // Determine that a client can be sent across threads and signaled.
    // As long as it compiles, this indicates that Client implements the
    // Send trait.
    #[test]
    fn test_send() {
        let cli = Client::new("tcp://localhost:1883").unwrap();
        let thr = thread::spawn(move || {
            assert!(!cli.is_connected());
        });
        thr.join().unwrap();
    }

    // Determine that a client can be shared across threads using an Arc.
    // As long as it compiles, this indicates that Client implements the
    // Send trait.
    // This is a bit redundant with the previous test, but explicitly
    // addresses GitHub Issue #31.
    #[test]
    fn test_send_arc() {
        let cli = Client::new("tcp://localhost:1883").unwrap();

        let cli = Arc::new(cli);
        let cli2 = cli.clone();

        let thr = thread::spawn(move || {
            assert!(!cli.is_connected());
        });
        assert!(!cli2.is_connected());
        thr.join().unwrap();
    }

    #[test]
    fn test_get_client_id() {
        let c_id = "test_client_id_can_be_retrieved";
        let options = CreateOptionsBuilder::new().client_id(c_id).finalize();
        let cli = Client::new(options);
        assert!(cli.is_ok(), "Error in creating sync client with client_id");
        let cli = cli.unwrap();
        let retrieved = cli.client_id();
        assert_eq!(retrieved, c_id);
    }
}
