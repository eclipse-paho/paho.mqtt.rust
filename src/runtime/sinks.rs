// paho-mqtt/src/runtime/sinks.rs
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

//! The application's own channels, fed by the runtime.
//!
//! A *sink* is an async channel the application got from [`get_stream`] or
//! [`get_event_stream`]. The event loop sends to it directly, so a message
//! reaches an application that is waiting on a stream without passing
//! through the event-dispatcher thread first.
//!
//! Callbacks and the synchronous consumers ([`start_consuming`] and
//! [`start_consuming_events`]) are *not* sinks: a callback may block and a
//! `crossbeam` receiver cannot be awaited, so both must stay on the
//! dispatcher thread. Nothing here displaces anything the application
//! registered; the callbacks are the application's alone.
//!
//! This module owns the mapping to the public types, so that `session.rs`
//! deals only in its own [`Message`] and never names [`crate::Event`].
//!
//! [`get_stream`]: crate::AsyncClient::get_stream
//! [`get_event_stream`]: crate::AsyncClient::get_event_stream
//! [`start_consuming`]: crate::AsyncClient::start_consuming
//! [`start_consuming_events`]: crate::AsyncClient::start_consuming_events

use std::{future::Future, pin::Pin};

use async_channel::{Sender, TrySendError};

use crate::{Event, Message, Properties, ReasonCode};

/// One application channel the runtime delivers to.
pub(crate) enum Sink {
    /// A message stream: [`get_stream`](crate::AsyncClient::get_stream).
    /// Receives `None` when the connection is lost, as the message callback
    /// does.
    Messages(Sender<Option<Message>>),
    /// An event stream:
    /// [`get_event_stream`](crate::AsyncClient::get_event_stream). Receives
    /// the messages *and* the connection events, in one sequence.
    Events(Sender<Event>),
}

impl Sink {
    /// Whether the application has dropped the receiving end.
    fn is_closed(&self) -> bool {
        match self {
            Self::Messages(tx) => tx.is_closed(),
            Self::Events(tx) => tx.is_closed(),
        }
    }
}

/// Deliveries that could not be made at once because a sink was full.
///
/// The event loop polls this as a branch of its `select!` and delivers
/// nothing else until it completes, so each destination keeps its order.
/// It owns its senders and items, so dropping the branch mid-poll loses
/// nothing — only the parked future itself holds them.
pub(crate) type Parked = Pin<Box<dyn Future<Output = ()> + Send>>;

/// The application sinks of one client.
///
/// Lives for as long as the client, not as long as a connection: the event
/// loop owns it and lends it to each [`Session`](super::session::Session) in
/// turn, so a stream survives a reconnect.
#[derive(Default)]
pub(crate) struct Sinks {
    list: Vec<Sink>,
}

impl Sinks {
    /// Adds a sink, from `Command::AddSink`.
    pub(crate) fn add(&mut self, sink: Sink) {
        self.list.push(sink);
    }

    /// Closes every sink, from `Command::ClearSinks` (`stop_stream`).
    pub(crate) fn clear(&mut self) {
        self.list.clear();
    }

    /// Whether anything is listening.
    pub(crate) fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    /// Forgets the sinks whose receiver the application has dropped.
    ///
    /// A stream ends by being dropped, which needs no call and no handle, so
    /// this is how a sink is removed. Checking costs an atomic load each and
    /// happens once per delivery, not once per sink per message.
    fn prune(&mut self) {
        self.list.retain(|sink| !sink.is_closed());
    }

    /// Delivers `msg` to every sink.
    ///
    /// Each destination gets the shape it asked for: `Some(msg)` to a
    /// message stream, `Event::Message(msg)` to an event stream. The last
    /// one takes the message by move, so a lone consumer — the usual case —
    /// costs no clone.
    ///
    /// Returns the parked deliveries if any sink was full. The caller must
    /// poll it to completion before delivering anything else.
    pub(crate) fn deliver_message(&mut self, msg: Message) -> Option<Parked> {
        self.prune();
        let n = self.list.len();
        if n == 0 {
            return None;
        }

        let mut blocked = Blocked::default();
        let mut msg = Some(msg);
        for (i, sink) in self.list.iter().enumerate() {
            // Safe to unwrap: only the last pass takes it.
            let m = if i + 1 == n {
                msg.take().unwrap()
            } else {
                msg.as_ref().unwrap().clone()
            };
            match sink {
                Sink::Messages(tx) => blocked.push_msg(tx, Some(m)),
                Sink::Events(tx) => blocked.push_event(tx, Event::Message(m)),
            }
        }
        blocked.into_parked()
    }

    /// Delivers a connection event to the event streams.
    ///
    /// Message streams carry messages alone, so they are not told; the one
    /// thing they do see is the end of the connection, through
    /// [`deliver_connection_lost`](Self::deliver_connection_lost).
    pub(crate) fn deliver_event(&mut self, ev: Event) -> Option<Parked> {
        self.prune();
        let mut blocked = Blocked::default();
        let mut remaining = self
            .list
            .iter()
            .filter(|s| matches!(s, Sink::Events(_)))
            .count();
        let mut ev = Some(ev);
        for sink in &self.list {
            if let Sink::Events(tx) = sink {
                remaining -= 1;
                let e = if remaining == 0 {
                    ev.take().unwrap()
                } else {
                    ev.as_ref().unwrap().clone()
                };
                blocked.push_event(tx, e);
            }
        }
        blocked.into_parked()
    }

    /// Tells every sink the connection has ended: `None` to the message
    /// streams, as the message callback is given, and
    /// `Event::ConnectionLost` to the event streams.
    ///
    /// Sent from the same task that fed them the messages, after the last of
    /// those, so the end cannot overtake what came before it.
    pub(crate) fn deliver_connection_lost(&mut self) -> Option<Parked> {
        self.prune();
        let mut blocked = Blocked::default();
        for sink in &self.list {
            match sink {
                Sink::Messages(tx) => blocked.push_msg(tx, None),
                Sink::Events(tx) => blocked.push_event(tx, Event::ConnectionLost),
            }
        }
        blocked.into_parked()
    }
}

/// Items a [`Sinks`] delivery could not hand over at once.
///
/// Sends that fit are made as they are collected; what is left over becomes
/// one future. `async_channel::Sender` has no `poll_ready`, so there is no
/// way to wait for room without holding the item — hence a future that owns
/// both, rather than a readiness check.
#[derive(Default)]
struct Blocked {
    msgs: Vec<(Sender<Option<Message>>, Option<Message>)>,
    events: Vec<(Sender<Event>, Event)>,
}

impl Blocked {
    /// Sends to a message stream, or records it for the parked future.
    ///
    /// A closed channel is ignored: the sink goes on the next `prune`.
    fn push_msg(&mut self, tx: &Sender<Option<Message>>, item: Option<Message>) {
        if let Err(TrySendError::Full(item)) = tx.try_send(item) {
            self.msgs.push((tx.clone(), item));
        }
    }

    /// Sends to an event stream, or records it for the parked future.
    fn push_event(&mut self, tx: &Sender<Event>, item: Event) {
        if let Err(TrySendError::Full(item)) = tx.try_send(item) {
            self.events.push((tx.clone(), item));
        }
    }

    /// The future that completes the leftover sends, if there are any.
    fn into_parked(self) -> Option<Parked> {
        if self.msgs.is_empty() && self.events.is_empty() {
            return None;
        }
        let Self { msgs, events } = self;
        Some(Box::pin(async move {
            for (tx, item) in msgs {
                let _ = tx.send(item).await;
            }
            for (tx, item) in events {
                let _ = tx.send(item).await;
            }
        }))
    }
}

/// Chains two parked fan-outs, so that the second is only sent once the
/// first has been, keeping each destination's order.
pub(crate) fn chain(first: Option<Parked>, second: Option<Parked>) -> Option<Parked> {
    match (first, second) {
        (None, second) => second,
        (first, None) => first,
        (Some(first), Some(second)) => Some(Box::pin(async move {
            first.await;
            second.await;
        })),
    }
}

/// Builds the public `Disconnected` event. Keeps the mapping to the public
/// types in this module, where the rest of it lives.
pub(crate) fn disconnected_event(reason_code: ReasonCode, props: Properties) -> Event {
    Event::Disconnected { props, reason_code }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QOS_1;

    fn msg(topic: &str) -> Message {
        Message::new(topic, "payload", QOS_1)
    }

    /// Every sink receives every message, each in the shape it asked for.
    #[test]
    fn test_fan_out_to_both_kinds() {
        let (mtx, mrx) = async_channel::unbounded();
        let (etx, erx) = async_channel::unbounded();
        let mut sinks = Sinks::default();
        sinks.add(Sink::Messages(mtx));
        sinks.add(Sink::Events(etx));

        assert!(sinks.deliver_message(msg("a")).is_none());
        assert!(sinks.deliver_message(msg("b")).is_none());

        let got: Vec<String> = (0..2)
            .map(|_| mrx.try_recv().unwrap().unwrap().topic)
            .collect();
        assert_eq!(got, ["a", "b"]);

        let got: Vec<String> = (0..2)
            .map(|_| match erx.try_recv().unwrap() {
                Event::Message(m) => m.topic,
                ev => panic!("unexpected event: {ev:?}"),
            })
            .collect();
        assert_eq!(got, ["a", "b"]);
    }

    /// A sink whose receiver was dropped is forgotten, and the others carry
    /// on. Nothing has to be unregistered by hand.
    #[test]
    fn test_dropped_receiver_unregisters_its_sink() {
        let (tx1, rx1) = async_channel::unbounded();
        let (tx2, rx2) = async_channel::unbounded::<Option<Message>>();
        let mut sinks = Sinks::default();
        sinks.add(Sink::Messages(tx1));
        sinks.add(Sink::Messages(tx2));

        drop(rx2);
        sinks.deliver_message(msg("a"));

        assert_eq!(rx1.try_recv().unwrap().unwrap().topic, "a");
        assert!(
            sinks.is_empty() || sinks.list.len() == 1,
            "the dead sink is gone"
        );
    }

    /// A full sink parks its delivery rather than dropping the message, and
    /// the parked future completes once the consumer makes room.
    #[test]
    fn test_full_sink_parks_rather_than_drops() {
        let (tx, rx) = async_channel::bounded(1);
        let mut sinks = Sinks::default();
        sinks.add(Sink::Messages(tx));

        assert!(sinks.deliver_message(msg("first")).is_none());
        let parked = sinks
            .deliver_message(msg("second"))
            .expect("the second had nowhere to go");

        // Nothing is lost: the first is there, the second is in the future.
        assert_eq!(rx.try_recv().unwrap().unwrap().topic, "first");
        futures::executor::block_on(parked);
        assert_eq!(rx.try_recv().unwrap().unwrap().topic, "second");
    }

    /// The end of a connection reaches both kinds, in their own shapes.
    #[test]
    fn test_connection_lost_reaches_both_kinds() {
        let (mtx, mrx) = async_channel::unbounded();
        let (etx, erx) = async_channel::unbounded();
        let mut sinks = Sinks::default();
        sinks.add(Sink::Messages(mtx));
        sinks.add(Sink::Events(etx));

        assert!(sinks.deliver_connection_lost().is_none());

        assert!(
            mrx.try_recv().unwrap().is_none(),
            "end of the message stream"
        );
        assert!(matches!(erx.try_recv().unwrap(), Event::ConnectionLost));
    }

    /// A connection event goes to the event streams only.
    #[test]
    fn test_connection_event_skips_message_streams() {
        let (mtx, mrx) = async_channel::unbounded::<Option<Message>>();
        let (etx, erx) = async_channel::unbounded();
        let mut sinks = Sinks::default();
        sinks.add(Sink::Messages(mtx));
        sinks.add(Sink::Events(etx));

        sinks.deliver_event(disconnected_event(
            ReasonCode::NormalDisconnection,
            Properties::new(),
        ));

        assert!(
            mrx.try_recv().is_err(),
            "a message stream has no event to see"
        );
        assert!(matches!(
            erx.try_recv().unwrap(),
            Event::Disconnected { .. }
        ));
    }
}
