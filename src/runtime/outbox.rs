// paho-mqtt/src/runtime/outbox.rs
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

//! The session's outbound byte queue.
//!
//! The session encodes packets into an [`Outbox`] and never writes to the
//! transport itself; the run loop's writer branch drains the outbox with
//! vectored writes whenever the transport will take more. Packets that pile
//! up while a write is in progress therefore go out together in the next
//! one, while a packet queued on an idle connection goes out on the very next
//! poll, with the same syscalls it would have cost on its own.
//!
//! Small packets are encoded back to back into one buffer. A large PUBLISH
//! payload is not copied: it is queued by reference as its own segment.

use std::{collections::VecDeque, io::IoSlice};

use bytes::{Bytes, BytesMut};
use paho_mqtt_codec::{MqttVersion, Packet};

use crate::{Result, responses::PublishResponse, token::Completer};

/// PUBLISH payloads up to this size are copied in behind their header; larger
/// ones are queued by reference to avoid the copy.
pub(crate) const INLINE_PAYLOAD_MAX: usize = 16 * 1024;

/// Initial capacity of the encode buffer. It is reused: once the segments
/// split off it have been written and dropped, its space is reclaimed.
const TAIL_CAPACITY: usize = 16 * 1024;

/// Where the most recently pushed frame lies, for [`Outbox::last_frame`].
#[derive(Clone, Copy)]
enum LastFrame {
    /// Nothing to report: no push yet, or the frame has been handed to the
    /// writer since.
    None,
    /// Header and body are `tail[start..]`.
    Inline(usize),
    /// The header ends the next-to-last segment, from `start`; the payload
    /// is the last segment.
    Split(usize),
}

/// Outbound bytes that the transport has not yet accepted, in wire order.
pub(crate) struct Outbox {
    /// Frozen runs of small packets, and large payloads held by reference.
    segs: VecDeque<Bytes>,
    /// Small packets are encoded here in place; frozen into `segs` when a
    /// large payload must follow, or when the writer takes it.
    tail: BytesMut,
    /// Bytes of `segs.front()` already accepted by the transport.
    front_off: usize,
    /// Monotonic byte counters: `queued - written` is [`len`](Self::len),
    /// and `flushed <= written`. TLS and WebSocket transports buffer
    /// internally, so bytes count as delivered only once a flush completes.
    queued: u64,
    written: u64,
    flushed: u64,
    /// QoS 0 completers, each released once `flushed` reaches its offset.
    on_flushed: VecDeque<(u64, Completer<PublishResponse>)>,
    /// Whether the transport needs a flush before written bytes count as
    /// delivered. When it does not, writing is flushing: `flushed` follows
    /// `written`, and the writer never makes a separate flush.
    flush_needed: bool,
    /// The frame most recently pushed.
    last: LastFrame,
}

impl Outbox {
    /// Creates an empty outbox for a transport that does, or does not, hold
    /// written bytes until a flush.
    pub fn new(flush_needed: bool) -> Self {
        Self {
            segs: VecDeque::new(),
            tail: BytesMut::with_capacity(TAIL_CAPACITY),
            front_off: 0,
            queued: 0,
            written: 0,
            flushed: 0,
            on_flushed: VecDeque::new(),
            flush_needed,
            last: LastFrame::None,
        }
    }

    /// Number of bytes queued but not yet accepted by the transport.
    pub fn len(&self) -> usize {
        (self.queued - self.written) as usize
    }

    /// `true` when everything queued has been accepted by the transport.
    pub fn is_empty(&self) -> bool {
        self.queued == self.written
    }

    /// `true` when everything is written but not yet flushed.
    pub fn needs_flush(&self) -> bool {
        self.is_empty() && self.flushed < self.written
    }

    /// Encodes `pkt` onto the end of the queue and returns the offset at
    /// which its frame ends.
    ///
    /// A PUBLISH payload larger than [`INLINE_PAYLOAD_MAX`] becomes its own
    /// segment, without a copy. A frame larger than `max`, the server's
    /// maximum packet size, is refused. On error nothing is queued.
    pub fn push(&mut self, pkt: &Packet, ver: MqttVersion, max: Option<u32>) -> Result<u64> {
        let start = self.tail.len();
        let payload = pkt.encode_split(&mut self.tail, ver)?;
        let hdr_len = self.tail.len() - start;
        let frame_len = hdr_len + payload.as_ref().map_or(0, Bytes::len);

        if let Some(max) = max
            && frame_len > max as usize
        {
            self.tail.truncate(start);
            return Err(paho_mqtt_codec::Error::PacketTooLarge.into());
        }

        match payload {
            Some(p) if p.len() > INLINE_PAYLOAD_MAX => {
                self.segs.push_back(self.tail.split().freeze());
                self.segs.push_back(p);
                self.last = LastFrame::Split(start);
            }
            Some(p) => {
                self.tail.extend_from_slice(&p);
                self.last = LastFrame::Inline(start);
            }
            None => self.last = LastFrame::Inline(start),
        }
        self.queued += frame_len as u64;
        Ok(self.queued)
    }

    /// The most recently pushed frame, as `[header_and_inline_body,
    /// large_payload]`, the second empty when the payload is inline. Used to
    /// persist a PUBLISH exactly as it goes on the wire.
    ///
    /// Valid until the next push, or until the writer takes the queue.
    pub fn last_frame(&self) -> [&[u8]; 2] {
        match self.last {
            LastFrame::Inline(start) => [&self.tail[start..], &[]],
            LastFrame::Split(start) => {
                let n = self.segs.len();
                [&self.segs[n - 2][start..], &self.segs[n - 1]]
            }
            LastFrame::None => {
                debug_assert!(false, "last_frame() with no frame pushed");
                [&[], &[]]
            }
        }
    }

    /// Releases `tx` with success once everything up to `end` has been
    /// flushed.
    pub fn complete_when_flushed(&mut self, end: u64, tx: Completer<PublishResponse>) {
        if end <= self.flushed {
            tx.complete(Ok(PublishResponse::default()));
        } else {
            self.on_flushed.push_back((end, tx));
        }
    }

    /// Freezes the encode buffer into the segment list, so that everything
    /// unwritten is in `segs` and [`io_slices`](Self::io_slices) can be taken
    /// from a shared borrow.
    pub fn prepare_write(&mut self) {
        if !self.tail.is_empty() {
            self.segs.push_back(self.tail.split().freeze());
        }
        self.last = LastFrame::None;
    }

    /// Fills `dst` with slices covering the unwritten bytes, in order, and
    /// returns how many it filled. Call after
    /// [`prepare_write`](Self::prepare_write).
    pub fn io_slices<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        debug_assert!(self.tail.is_empty(), "io_slices() before prepare_write()");
        let mut n = 0;
        for (i, seg) in self.segs.iter().enumerate() {
            if n == dst.len() {
                break;
            }
            let off = if i == 0 { self.front_off } else { 0 };
            dst[n] = IoSlice::new(&seg[off..]);
            n += 1;
        }
        n
    }

    /// Records that the transport accepted the next `n` bytes. On a
    /// transport that needs no flush, that delivers them too.
    pub fn advance(&mut self, mut n: usize) {
        debug_assert!(n <= self.len(), "advance() past the end of the queue");
        self.written += n as u64;
        if !self.flush_needed {
            self.flushed();
        }
        while n > 0 {
            let Some(front) = self.segs.front() else {
                break;
            };
            let rem = front.len() - self.front_off;
            if n < rem {
                self.front_off += n;
                break;
            }
            n -= rem;
            self.segs.pop_front();
            self.front_off = 0;
        }
        self.last = LastFrame::None;
    }

    /// Records a completed flush of everything written so far, releasing
    /// every completer whose frame it covers.
    pub fn flushed(&mut self) {
        self.flushed = self.written;
        while let Some((end, _)) = self.on_flushed.front() {
            if *end > self.flushed {
                break;
            }
            if let Some((_, tx)) = self.on_flushed.pop_front() {
                tx.complete(Ok(PublishResponse::default()));
            }
        }
    }

    /// Drops all unsent bytes. Pending completers are dropped, so their
    /// tokens resolve with [`Error::Disconnected`](crate::Error::Disconnected).
    #[cfg(test)]
    pub fn clear(&mut self) {
        self.segs.clear();
        self.tail.clear();
        self.front_off = 0;
        self.written = self.queued;
        self.flushed = self.queued;
        self.on_flushed.clear();
        self.last = LastFrame::None;
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Error, Token};
    use paho_mqtt_codec::{Properties, PubAck, Publish, QoS};

    const VER: MqttVersion = MqttVersion::V5;

    fn publish(topic: &str, payload: Bytes) -> Packet {
        Packet::Publish(Publish {
            dup: false,
            qos: QoS::QoS0,
            retain: false,
            topic: topic.into(),
            packet_id: None,
            payload,
            properties: Properties::new(),
        })
    }

    fn encoded(pkt: &Packet) -> Vec<u8> {
        let mut buf = BytesMut::new();
        pkt.encode(&mut buf, VER).unwrap();
        buf.to_vec()
    }

    /// Drains the outbox `chunk` bytes at a time, as a transport that
    /// accepts at most that much per write would, and returns the bytes.
    fn drain(out: &mut Outbox, chunk: usize, slots: usize) -> Vec<u8> {
        let mut wire = Vec::new();
        out.prepare_write();
        while !out.is_empty() {
            let mut iov = vec![IoSlice::new(&[]); slots];
            let n = out.io_slices(&mut iov);
            let mut take = chunk;
            let mut accepted = 0;
            for s in &iov[..n] {
                let k = s.len().min(take);
                wire.extend_from_slice(&s[..k]);
                accepted += k;
                take -= k;
                if take == 0 {
                    break;
                }
            }
            out.advance(accepted);
        }
        wire
    }

    #[test]
    fn test_order_preserved_across_small_and_large() {
        let big = Bytes::from(vec![7u8; INLINE_PAYLOAD_MAX + 1]);
        let pkts = [
            publish("a", Bytes::from_static(b"small")),
            Packet::PubAck(PubAck::new(3)),
            publish("b", big.clone()),
            publish("c", Bytes::from_static(b"after")),
            publish("d", big),
        ];
        let mut out = Outbox::new(true);
        let mut expect = Vec::new();
        for p in &pkts {
            let end = out.push(p, VER, None).unwrap();
            expect.extend(encoded(p));
            assert_eq!(end, expect.len() as u64);
        }
        assert_eq!(out.len(), expect.len());
        assert_eq!(drain(&mut out, usize::MAX, 64), expect);
        assert!(out.is_empty());
    }

    #[test]
    fn test_advance_within_and_across_segments() {
        let big = Bytes::from(vec![1u8; INLINE_PAYLOAD_MAX + 100]);
        let pkts = [
            publish("x", Bytes::from_static(b"hello")),
            publish("y", big),
            Packet::PingReq,
        ];
        let mut out = Outbox::new(true);
        let mut expect = Vec::new();
        for p in &pkts {
            out.push(p, VER, None).unwrap();
            expect.extend(encoded(p));
        }
        // 7 bytes at a time cuts inside and across every segment boundary.
        assert_eq!(drain(&mut out, 7, 64), expect);
    }

    #[test]
    fn test_fewer_slots_than_segments() {
        let big = Bytes::from(vec![2u8; INLINE_PAYLOAD_MAX + 1]);
        let mut out = Outbox::new(true);
        let mut expect = Vec::new();
        for i in 0..5 {
            let p = publish(&format!("t{i}"), big.clone());
            out.push(&p, VER, None).unwrap();
            expect.extend(encoded(&p));
        }
        // Ten segments, two slots per write.
        assert_eq!(drain(&mut out, usize::MAX, 2), expect);
    }

    #[test]
    fn test_large_payload_is_not_copied() {
        let big = Bytes::from(vec![3u8; INLINE_PAYLOAD_MAX + 1]);
        let mut out = Outbox::new(true);
        out.push(&publish("big", big.clone()), VER, None).unwrap();
        out.prepare_write();
        let mut iov = [IoSlice::new(&[]); 4];
        let n = out.io_slices(&mut iov);
        assert_eq!(n, 2);
        assert_eq!(iov[1].as_ptr(), big.as_ptr());
    }

    #[test]
    fn test_failed_push_queues_nothing() {
        let mut out = Outbox::new(true);
        let first = publish("ok", Bytes::from_static(b"1"));
        out.push(&first, VER, None).unwrap();
        let len = out.len();
        let last: Vec<u8> = out.last_frame().concat();

        // Too large for the server's limit.
        let big = publish("big", Bytes::from(vec![0u8; 100]));
        assert!(out.push(&big, VER, Some(50)).is_err());
        // Refused by the encoder: a wildcard in a topic name.
        let bad = publish("bad/#", Bytes::new());
        assert!(out.push(&bad, VER, None).is_err());

        assert_eq!(out.len(), len);
        assert_eq!(out.last_frame().concat(), last);
        assert_eq!(drain(&mut out, usize::MAX, 8), encoded(&first));
    }

    #[test]
    fn test_last_frame() {
        let mut out = Outbox::new(true);
        out.push(&Packet::PingReq, VER, None).unwrap();

        let small = publish("s", Bytes::from_static(b"body"));
        out.push(&small, VER, None).unwrap();
        assert_eq!(out.last_frame().concat(), encoded(&small));
        assert!(out.last_frame()[1].is_empty());

        let big = publish("b", Bytes::from(vec![9u8; INLINE_PAYLOAD_MAX + 1]));
        out.push(&big, VER, None).unwrap();
        assert_eq!(out.last_frame().concat(), encoded(&big));
        assert_eq!(out.last_frame()[1].len(), INLINE_PAYLOAD_MAX + 1);
    }

    #[test]
    fn test_completers_fire_at_flush_not_write() {
        let mut out = Outbox::new(true);
        let (mut tok1, tx1) = Token::<PublishResponse>::new();
        let end1 = out
            .push(&publish("a", Bytes::from_static(b"1")), VER, None)
            .unwrap();
        out.complete_when_flushed(end1, tx1);
        let (mut tok2, tx2) = Token::<PublishResponse>::new();
        let end2 = out
            .push(&publish("b", Bytes::from_static(b"2")), VER, None)
            .unwrap();
        out.complete_when_flushed(end2, tx2);

        // All of the first frame and part of the second written.
        out.prepare_write();
        out.advance(end1 as usize + 1);
        assert!(!out.needs_flush());
        out.flushed();
        assert!(tok1.try_wait().is_some());
        assert!(tok2.try_wait().is_none());

        // The rest written: still pending until flushed.
        out.advance(out.len());
        assert!(out.needs_flush());
        assert!(tok2.try_wait().is_none());
        out.flushed();
        assert!(!out.needs_flush());
        assert!(matches!(tok2.try_wait(), Some(Ok(_))));
    }

    /// Where the transport needs no flush, a write completes its frames.
    #[test]
    fn test_no_flush_transport_completes_on_write() {
        let mut out = Outbox::new(false);
        let (mut tok, tx) = Token::<PublishResponse>::new();
        let end = out
            .push(&publish("a", Bytes::from_static(b"1")), VER, None)
            .unwrap();
        out.complete_when_flushed(end, tx);

        out.prepare_write();
        out.advance(end as usize - 1);
        assert!(tok.try_wait().is_none(), "not all of it written");
        out.advance(1);
        assert!(matches!(tok.try_wait(), Some(Ok(_))));
        assert!(!out.needs_flush(), "the writer makes no separate flush");
    }

    #[test]
    fn test_clear_resolves_pending_disconnected() {
        let mut out = Outbox::new(true);
        let (mut tok, tx) = Token::<PublishResponse>::new();
        let end = out
            .push(&publish("a", Bytes::from_static(b"1")), VER, None)
            .unwrap();
        out.complete_when_flushed(end, tx);
        out.clear();
        assert!(out.is_empty());
        assert!(!out.needs_flush());
        assert!(matches!(tok.try_wait(), Some(Err(Error::Disconnected))));
    }
}
