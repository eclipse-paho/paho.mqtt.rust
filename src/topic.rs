// paho-mqtt/src/topic.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2017-2025 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! Objects for manipulating and checking message topics.
//!

use crate::{
    async_client::AsyncClient,
    client::Client,
    errors::{Error, Result},
    message::{Message, MessageBuilder},
    properties::{Properties, PropertyCode},
    subscribe_options::SubscribeOptions,
    token::{DeliveryToken, Token},
    QoS, ServerResponse,
};
use std::fmt;

/////////////////////////////////////////////////////////////////////////////
//                              Topic
/////////////////////////////////////////////////////////////////////////////

/// A topic destination for messages.
///
/// This keeps message parameters for repeatedly publishing to the same
/// topic on a server.
pub struct Topic<'a> {
    /// Reference to the broker that will receive the messages.
    cli: &'a AsyncClient,
    /// The topic on which to publish the messages.
    topic: String,
    /// The QoS level to publish the messages.
    qos: QoS,
    /// Whether the last message should be retained by the broker.
    retained: bool,
    /// The topic alias.
    /// If this is non-zero is it sent in place of the string topic name.
    alias: u16,
}

impl<'a> Topic<'a> {
    /// Creates a new topic object for publishing messages.
    ///
    /// # Arguments
    ///
    /// `cli` The client used to publish the messages.
    /// `topic` The topic on which to publish the messages
    /// `qos` The quality of service for messages
    ///
    pub fn new<T, Q>(cli: &'a AsyncClient, topic: T, qos: Q) -> Self
    where
        T: Into<String>,
        Q: Into<QoS>,
    {
        Topic {
            cli,
            topic: topic.into(),
            qos: qos.into(),
            retained: false,
            alias: 0,
        }
    }

    /// Creates a new topic object for publishing retain messages.
    ///
    /// # Arguments
    ///
    /// `cli` The client used to publish the messages.
    /// `topic` The topic on which to publish the messages
    /// `qos` The quality of service for messages
    ///
    pub fn new_retained<T, Q>(cli: &'a AsyncClient, topic: T, qos: Q) -> Self
    where
        T: Into<String>,
        Q: Into<QoS>,
    {
        Topic {
            cli,
            topic: topic.into(),
            qos: qos.into(),
            retained: true,
            alias: 0,
        }
    }

    /// Create a message for the topic using the supplied payload.
    ///
    /// If `inc_topic` is true, this will create a message containing the
    /// string topic whether or not the alias is set. This would be done
    /// to set the topic alias on the server.
    ///
    /// If `inc_topic` is false, then the topic will be left blank if the
    /// alias is set, and the topic alias property will be added to the
    /// message.
    pub fn create_message_with_topic<V>(&self, payload: V, inc_topic: bool) -> Message
    where
        V: Into<Vec<u8>>,
    {
        let mut bld = MessageBuilder::new()
            .payload(payload)
            .qos(self.qos)
            .retained(self.retained);

        if self.alias == 0 || inc_topic {
            bld = bld.topic(&self.topic);
        }

        if self.alias != 0 {
            bld = bld.properties(properties! { PropertyCode::TopicAlias => self.alias });
        }

        bld.finalize()
    }

    /// Create a message for the topic using the supplied payload
    ///
    pub fn create_message<V>(&self, payload: V) -> Message
    where
        V: Into<Vec<u8>>,
    {
        self.create_message_with_topic(payload, false)
    }

    /// Subscribe to the topic.
    pub fn subscribe(&self) -> Token {
        self.cli.subscribe(self.topic.clone(), self.qos)
    }

    /// Subscribe to the topic with subscription options.
    pub fn subscribe_with_options<T, P>(&self, opts: T, props: P) -> Token
    where
        T: Into<SubscribeOptions>,
        P: Into<Option<Properties>>,
    {
        self.cli
            .subscribe_with_options(self.topic.clone(), self.qos, opts, props)
    }

    /// Publish a message on the topic.
    ///
    /// If a topic alias was previously sent, this will use the integer alias
    /// property instead of sending the topic string.
    /// Topic aliases are only applicable for MQTT v5 connections.
    ///
    /// # Arguments
    ///
    /// `payload` The payload of the message
    ///
    pub fn publish<V>(&self, payload: V) -> DeliveryToken
    where
        V: Into<Vec<u8>>,
    {
        let msg = self.create_message(payload);
        self.cli.publish(msg)
    }

    /// Attempts to publish a message on the topic, but returns an error
    /// immediately if there's a problem creating or queuing the message for
    /// transmission.
    ///
    /// If a topic alias was previously sent, this will use the integer alias
    /// property instead of sending the topic string.
    /// Topic aliases are only applicable for MQTT v5 connections.
    ///
    /// # Arguments
    ///
    /// `payload` The payload of the message
    ///
    /// Returns a Publish Error containing the complete message on failure.
    pub fn try_publish<V>(&self, payload: V) -> Result<DeliveryToken>
    where
        V: Into<Vec<u8>>,
    {
        let msg = self.create_message(payload);
        self.cli.try_publish(msg)
    }

    /// Publish a message with a topic alias.
    ///
    /// This publishes the message with a topic alias property to set the
    /// alias at the broker. After calling this, the object keeps the
    /// alias and uses it for subsequent publishes instead of sending the
    /// full topic string.
    ///
    /// Note that using an alias is only valid with an MQTT v5 connection,
    /// and the value must be in the range of 1 - TopicAliasMaximum as the
    /// broker reported in the CONNACK packet. The alias is only valid
    /// for a single connection. It should be reset on a reconnect.
    ///
    /// This can be called a second time to change the alias setting.
    /// Using an alias of zero on a subsequent call instructs this object to
    /// stop using the alias and publish with the topic name.
    ///
    /// # Arguments
    ///
    /// `alias` The integer alias to use for subsequent message publishing.
    ///     This must be in the range 1 - `TopicAliasMaximum` as reported by
    ///     the server in the CONNACK package. Using a value of zero
    ///     instructs this object to stop using the alias and go back to
    ///     publishing with the string topic name.
    /// `payload` The payload of the message
    ///
    pub fn publish_with_alias<V>(&mut self, alias: u16, payload: V) -> DeliveryToken
    where
        V: Into<Vec<u8>>,
    {
        self.alias = alias;

        let msg = self.create_message_with_topic(payload, true);
        self.cli.publish(msg)
    }

    /// Attempts to publish a message on the topic using and setting a new topic
    /// alias, but returns an error immediately if there's a problem creating or
    /// queuing the message for transmission.
    ///
    /// See [`publish_with_alias()`](Self::publish_with_alias) for more information.
    ///
    /// Returns a Publish Error containing the complete message on failure.
    pub fn try_publish_with_alias<V>(&mut self, alias: u16, payload: V) -> Result<DeliveryToken>
    where
        V: Into<Vec<u8>>,
    {
        self.alias = alias;

        let msg = self.create_message_with_topic(payload, true);
        self.cli.try_publish(msg)
    }

    /// Gets the alias for the topic, if any.
    pub fn alias(&self) -> Option<u16> {
        match self.alias {
            0 => None,
            val => Some(val),
        }
    }

    /// Removes the alias, if any, from the topic.
    ///
    /// After removing the alias, publshed messages contain the full string
    /// topic. The alias mapping remains on the server though. The alias
    /// number cann  be reused by assigning to a different topic, but the
    /// only way to remove it is to disconnect the client.
    pub fn remove_alias(&mut self) {
        self.alias = 0;
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              SyncTopic
/////////////////////////////////////////////////////////////////////////////

/// A topic destination for messages.
///
/// This keeps message parameters for repeatedly publishing to the same
/// topic on a server.
pub struct SyncTopic<'a> {
    cli: &'a Client,
    topic: Topic<'a>,
}

impl<'a> SyncTopic<'a> {
    /// Creates a new topic object for publishing messages.
    ///
    /// # Arguments
    ///
    /// `cli` The client used to publish the messages.
    /// `topic` The topic on which to publish the messages
    /// `qos` The quality of service for messages
    ///
    pub fn new<T, Q>(cli: &'a Client, topic: T, qos: Q) -> Self
    where
        T: Into<String>,
        Q: Into<QoS>,
    {
        Self {
            cli,
            topic: Topic::new(&cli.cli, topic, qos),
        }
    }

    /// Creates a new topic object for publishing messages.
    ///
    /// # Arguments
    ///
    /// `cli` The client used to publish the messages.
    /// `topic` The topic on which to publish the messages
    /// `qos` The quality of service for messages
    ///
    pub fn new_retained<T, Q>(cli: &'a Client, topic: T, qos: Q) -> Self
    where
        T: Into<String>,
        Q: Into<QoS>,
    {
        Self {
            cli,
            topic: Topic::new_retained(&cli.cli, topic, qos),
        }
    }

    /// Create a message for the topic using the supplied payload
    pub fn create_message<V>(&self, payload: V) -> Message
    where
        V: Into<Vec<u8>>,
    {
        self.topic.create_message(payload)
    }

    /// Subscribe to the topic.
    pub fn subscribe(&self) -> Result<ServerResponse> {
        self.cli.subscribe(&self.topic.topic, self.topic.qos)
    }

    /// Subscribe to the topic with subscription options.
    pub fn subscribe_with_options<T, P>(&self, opts: T, props: P) -> Result<ServerResponse>
    where
        T: Into<SubscribeOptions>,
        P: Into<Option<Properties>>,
    {
        self.cli
            .subscribe_with_options(&self.topic.topic, self.topic.qos, opts, props)
    }

    /// Publish a message on the topic.
    ///
    /// If a topic alias was previously sent, this will use the integer alias
    /// property instead of sending the topic string.
    /// Topic aliases are only applicable for MQTT v5 connections.
    ///
    /// # Arguments
    ///
    /// `payload` The payload of the message
    ///
    pub fn publish<V>(&self, payload: V) -> Result<()>
    where
        V: Into<Vec<u8>>,
    {
        let msg = self.create_message(payload);
        self.cli.publish(msg)
    }

    /// Publish a message with a topic alias.
    ///
    /// This publishes the message with a topic alias property to set the
    /// alias at the broker. After calling this, the object keeps the
    /// alias and uses it for subsequent publishes instead of sending the
    /// full topic string.
    ///
    /// Note that using an alias is only valid with an MQTT v5 connection,
    /// and the value must be in the range of 1 - TopicAliasMaximum as the
    /// broker reported in the CONNACK packet. The alias is only valid
    /// for a single connection. It should be reset on a reconnect.
    ///
    /// This can be called a second time to change the alias setting.
    /// Using an alias of zero on a subsequent call instructs this object to
    /// stop using the alias and publish with the topic name.
    ///
    /// # Arguments
    ///
    /// `alias` The integer alias to use for subsequent message publishing.
    ///     This must be in the range 1 - `TopicAliasMaximum` as reported by
    ///     the server in the CONNACK package. Using a value of zero
    ///     instructs this object to stop using the alias and go back to
    ///     publishing with the string topic name.
    /// `payload` The payload of the message
    ///
    pub fn publish_with_alias<V>(&mut self, alias: u16, payload: V) -> Result<()>
    where
        V: Into<Vec<u8>>,
    {
        self.topic.alias = alias;

        let msg = self.topic.create_message_with_topic(payload, true);
        self.cli.publish(msg)
    }

    /// Gets the alias for the topic, if any.
    pub fn alias(&self) -> Option<u16> {
        self.topic.alias()
    }

    /// Removes the alias, if any, from the topic.
    ///
    /// After removing the alias, publshed messages contain the full string
    /// topic. The alias mapping remains on the server though. The alias
    /// number cann  be reused by assigning to a different topic, but the
    /// only way to remove it is to disconnect the client.
    pub fn remove_alias(&mut self) {
        self.topic.remove_alias();
    }
}

/////////////////////////////////////////////////////////////////////////////
//                          TopicFilter
/////////////////////////////////////////////////////////////////////////////

/// A topic filter.
///
/// An MQTT topic filter is a multi-field string, delimited by forward
/// slashes, '/', in which fields can contain the wildcards:
///     '+' - Matches a single field
///     '#' - Matches all subsequent fields (must be last field in filter)
///
/// It's primary use is to match against incoming topics. The internal
/// implementation is optimized for that use case.
#[derive(Debug)]
pub enum TopicFilter {
    /// If there are no wildcards, the filter is a straight topic string
    Topic(String),
    /// If there are wildcards, the filter is split by fields.
    Fields(Vec<String>),
}

impl TopicFilter {
    /// Creates a new topic filter from the string.
    /// This can fail if the filter is not correct, such as having a '#'
    /// wildcard in anyplace other than the last field, or if
    pub fn new<S: Into<String>>(filter: S) -> Result<Self> {
        let filter = filter.into();
        let n = filter.len();

        if n == 0 {
            return Err(Error::BadTopicFilter);
        }

        // If the topic contains any wildcards.
        let wild = match filter.find('#') {
            Some(i) if i < n - 1 => return Err(Error::BadTopicFilter),
            Some(_) => true,
            None => filter.contains('+'),
        };

        let v = if wild {
            let fields = filter.split('/').map(String::from).collect();
            Self::Fields(fields)
        }
        else {
            Self::Topic(filter)
        };

        Ok(v)
    }

    /// Creates a new topic filter from the string without checking it.
    pub fn new_unchecked<S: Into<String>>(filter: S) -> Self {
        let filter = filter.into();

        if filter.ends_with('#') || filter.contains('+') {
            Self::Fields(filter.split('/').map(String::from).collect())
        }
        else {
            Self::Topic(filter)
        }
    }

    /// Determines if the filter contains wildcards.
    #[inline]
    pub fn has_wildcards(&self) -> bool {
        matches!(self, Self::Fields(_))
    }

    /// Gets the number of fields in the topic filter.
    pub fn num_fields(&self) -> usize {
        match self {
            Self::Topic(filter) => filter.chars().filter(|&c| c == '/').count() + 1,
            Self::Fields(fields) => fields.len(),
        }
    }

    /// Determines if the topic matches the filter.
    ///
    /// This is the same as [`is_match`](Self::is_match), but uses a more
    /// consistent function name with other topic matchers.
    pub fn matches(&self, topic: &str) -> bool {
        use crate::topic_matcher::topic_matches_iter;
        match self {
            Self::Topic(filter) => topic == filter,
            Self::Fields(fields) => {
                topic_matches_iter(fields.iter().map(|s| s.as_str()), topic.split('/'))
            }
        }
    }

    /// Determines if the topic matches the filter.
    pub fn is_match(&self, topic: &str) -> bool {
        self.matches(topic)
    }
}

impl TryFrom<&str> for TopicFilter {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self> {
        Self::new(s)
    }
}

impl TryFrom<String> for TopicFilter {
    type Error = Error;

    fn try_from(s: String) -> Result<Self> {
        Self::new(s)
    }
}

impl fmt::Display for TopicFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Topic(filter) => write!(f, "{}", filter),
            Self::Fields(fields) => write!(f, "{}", fields.join("/")),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nonwild_topic_filter() {
        const FILTER0: &str = "test";

        let filter = TopicFilter::new(FILTER0).unwrap();
        assert!(matches!(filter, TopicFilter::Topic(ref s) if s == FILTER0));
        assert!(filter.is_match(FILTER0));
        assert!(!filter.has_wildcards());
        assert_eq!(filter.num_fields(), 1);
        assert_eq!(format!("{}", filter), FILTER0);

        const FILTER1: &str = "some/random/topic";

        let filter = TopicFilter::new(FILTER1).unwrap();
        assert!(matches!(filter, TopicFilter::Topic(ref s) if s == FILTER1));
        assert!(filter.is_match(FILTER1));
        assert!(!filter.has_wildcards());
        assert_eq!(filter.num_fields(), 3);
        assert_eq!(format!("{}", filter), FILTER1);
    }

    #[test]
    fn test_basic_topic_filter() {
        const FILTER0: &str = "#";

        let filter = TopicFilter::new(FILTER0).unwrap();
        assert!(matches!(filter, TopicFilter::Fields(_)));
        assert!(filter.is_match("this"));
        assert!(filter.is_match("this/that"));
        assert!(filter.is_match("some/topic/thing"));
        assert!(filter.has_wildcards());
        assert_eq!(filter.num_fields(), 1);
        assert_eq!(format!("{}", filter), FILTER0);

        const FILTER1: &str = "some/topic/#";

        let filter = TopicFilter::new(FILTER1).unwrap();
        assert!(matches!(filter, TopicFilter::Fields(_)));
        assert!(filter.is_match("some/topic/thing"));
        assert!(filter.has_wildcards());
        assert_eq!(filter.num_fields(), 3);
        assert_eq!(format!("{}", filter), FILTER1);

        const FILTER2: &str = "some/+/thing";

        let filter = TopicFilter::new(FILTER2).unwrap();
        assert!(matches!(filter, TopicFilter::Fields(_)));
        assert!(filter.is_match("some/topic/thing"));
        assert!(!filter.is_match("some/topic/plus/thing"));
        assert!(filter.has_wildcards());
        assert_eq!(filter.num_fields(), 3);
        assert_eq!(format!("{}", filter), FILTER2);

        const FILTER3: &str = "some/+";

        let filter = TopicFilter::new(FILTER3).unwrap();
        assert!(matches!(filter, TopicFilter::Fields(_)));
        assert!(filter.is_match("some/thing"));
        assert!(!filter.is_match("some/thing/plus"));
        assert!(filter.has_wildcards());
        assert_eq!(filter.num_fields(), 2);
        assert_eq!(format!("{}", filter), FILTER3);
    }

    #[test]
    fn test_topic_filter() {
        // Should match

        assert!(TopicFilter::new_unchecked("foo/bar").matches("foo/bar"));
        assert!(TopicFilter::new_unchecked("foo/+").matches("foo/bar"));
        assert!(TopicFilter::new_unchecked("foo/+/baz").matches("foo/bar/baz"));
        assert!(TopicFilter::new_unchecked("foo/+/#").matches("foo/bar/baz"));
        assert!(TopicFilter::new_unchecked("A/B/+/#").matches("A/B/B/C"));
        assert!(TopicFilter::new_unchecked("#").matches("foo/bar/baz"));
        assert!(TopicFilter::new_unchecked("#").matches("/foo/bar"));
        assert!(TopicFilter::new_unchecked("/#").matches("/foo/bar"));
        assert!(TopicFilter::new_unchecked("$SYS/bar").matches("$SYS/bar"));
        assert!(TopicFilter::new_unchecked("foo/#").matches("foo/$bar"));
        assert!(TopicFilter::new_unchecked("foo/+/baz").matches("foo/$bar/baz"));

        // Should not match

        assert!(!TopicFilter::new_unchecked("test/6/#").matches("test/3"));
        assert!(!TopicFilter::new_unchecked("foo/bar").matches("foo"));
        assert!(!TopicFilter::new_unchecked("foo/+").matches("foo/bar/baz"));
        assert!(!TopicFilter::new_unchecked("foo/+/baz").matches("foo/bar/bar"));
        assert!(!TopicFilter::new_unchecked("foo/+/#").matches("fo2/bar/baz"));
        assert!(!TopicFilter::new_unchecked("/#").matches("foo/bar"));
        assert!(!TopicFilter::new_unchecked("#").matches("$SYS/bar"));
        assert!(!TopicFilter::new_unchecked("$BOB/bar").matches("$SYS/bar"));
        assert!(!TopicFilter::new_unchecked("+/bar").matches("$SYS/bar"));
    }
}
