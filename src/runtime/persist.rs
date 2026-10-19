// paho-mqtt/src/runtime/persist.rs
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

//! The persistence store, off the event loop.
//!
//! A store that touches a disk can take milliseconds, and the event loop
//! cannot afford to wait inside one: with the tokio backend borrowing the
//! application's runtime, the thread it would block is the application's.
//! So a store that says it may block ([`ClientPersistence::is_blocking`])
//! gets a thread of its own and an ordered queue, and the session talks to
//! it rather than to the store.
//!
//! **Persist before send still holds.** Each queued put carries a sequence
//! number, and the session records where in the outbound byte stream the
//! frame it belongs to begins. The writer is held at the earliest of those
//! until the worker reports that sequence stored, so a PUBLISH cannot reach
//! the wire before the store has it. Everything queued ahead of that point
//! — acknowledgements, QoS 0, publishes already stored — goes out as usual.
//!
//! **Order is kept.** Everything the session does to the store goes through
//! this one queue, so a remove can never overtake the put it undoes, and
//! the worker is the only thing holding the store's lock while a session
//! runs.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    thread,
    time::Duration,
};

use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender};
use futures::task::AtomicWaker;
use log::warn;

use crate::persistence::ClientPersistence;

/// The store, shared with whatever opened it.
pub(crate) type Store = Arc<Mutex<Box<dyn ClientPersistence>>>;

/// One piece of work for the store, in the order the session asked for it.
enum Op {
    /// Store `parts` under `key`. Consecutive puts are passed to the store
    /// as one [`put_many`](ClientPersistence::put_many).
    Put {
        seq: u64,
        key: String,
        parts: Vec<Bytes>,
    },
    /// Remove `key`, after everything queued before it.
    Remove { key: String },
    /// Close the store, at the end of a connection.
    Close,
    /// Report progress without doing anything, so that the loop can wait
    /// for the queue to empty before using the store directly.
    Barrier { seq: u64 },
}

/// How far the worker has got, and whether anyone is waiting to hear.
#[derive(Default)]
struct Progress {
    /// The highest sequence number now stored.
    done: AtomicU64,
    waker: AtomicWaker,
    /// Whether the session is waiting on `done`. Read by the worker after
    /// every batch, so the usual case costs a load rather than the
    /// compare-and-swap inside [`AtomicWaker::wake`].
    waiting: AtomicBool,
}

impl Progress {
    /// Records that everything up to `seq` is stored.
    fn publish(&self, seq: u64) {
        self.done.store(seq, Ordering::SeqCst);
        if self.waiting.load(Ordering::SeqCst) {
            self.waker.wake();
        }
    }
}

/// The session's end of the persistence worker.
///
/// Owned by the event loop and lent to each session, like the store itself:
/// one thread per client, not per connection. Dropping it closes the queue,
/// which is how the thread learns to exit.
pub(crate) struct PersistWorker {
    tx: Sender<Op>,
    progress: Arc<Progress>,
    /// The sequence number given to the next put.
    next_seq: u64,
    /// How long the session waits for this store before giving up on the
    /// connection; zero means forever.
    timeout: Duration,
}

impl PersistWorker {
    /// Starts the worker thread for `store`.
    pub(crate) fn start(store: Store, timeout: Duration) -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();
        let progress = Arc::new(Progress::default());
        let worker = Arc::clone(&progress);
        // Detached: the thread ends when the queue closes, which happens
        // when the event loop drops this handle.
        let spawned = thread::Builder::new()
            .name("mqtt-persist".into())
            .spawn(move || run(&rx, &store, &worker));
        if let Err(e) = &spawned {
            warn!("Could not start the persistence thread ({e}); the store will not be used");
        }
        Self {
            tx,
            progress,
            next_seq: 0,
            timeout,
        }
    }

    /// How long to wait for this store before giving up on the connection.
    pub(crate) fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Queues a put and returns its sequence number, which the caller holds
    /// against the frame that must not go out before it.
    pub(crate) fn put(&mut self, key: String, parts: Vec<Bytes>) -> u64 {
        self.next_seq += 1;
        let seq = self.next_seq;
        let _ = self.tx.send(Op::Put { seq, key, parts });
        seq
    }

    /// Queues a remove. Nothing waits for it: a message that is gone from
    /// the session is gone whether or not the store has caught up, and the
    /// queue keeps it behind the put it undoes.
    pub(crate) fn remove(&mut self, key: String) {
        let _ = self.tx.send(Op::Remove { key });
    }

    /// Queues a close, for the end of a connection.
    pub(crate) fn close(&mut self) {
        let _ = self.tx.send(Op::Close);
    }

    /// The highest sequence number stored so far.
    pub(crate) fn done_seq(&self) -> u64 {
        self.progress.done.load(Ordering::SeqCst)
    }

    /// Ready once everything queued up to `seq` has been stored.
    pub(crate) fn poll_done(&self, cx: &mut Context<'_>, seq: u64) -> Poll<()> {
        if self.done_seq() >= seq {
            self.progress.waiting.store(false, Ordering::SeqCst);
            return Poll::Ready(());
        }
        // Set before the second check, so that a batch finishing in between
        // either sees the flag and wakes us, or is seen by the check. Both
        // sides are `SeqCst`, or each could read the other's stale value
        // and the wakeup would be lost.
        self.progress.waiting.store(true, Ordering::SeqCst);
        self.progress.waker.register(cx.waker());
        if self.done_seq() >= seq {
            self.progress.waiting.store(false, Ordering::SeqCst);
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    /// Waits until the queue has drained, so that the caller may use the
    /// store directly — at connect time, to open it, read it back, or
    /// clear it — without contending with the worker or reading a state it
    /// is half way through writing.
    pub(crate) async fn idle(&mut self) {
        self.next_seq += 1;
        let seq = self.next_seq;
        if self.tx.send(Op::Barrier { seq }).is_err() {
            return;
        }
        std::future::poll_fn(|cx| self.poll_done(cx, seq)).await
    }
}

/// The worker thread: take everything queued, do it in order, say how far
/// it got.
fn run(rx: &Receiver<Op>, store: &Store, progress: &Progress) {
    while let Ok(first) = rx.recv() {
        // Whatever else is already waiting goes in the same pass, so a
        // burst of publishes costs one round trip to the store rather than
        // one each.
        let mut batch = vec![first];
        while let Ok(op) = rx.try_recv() {
            batch.push(op);
        }

        let mut done = 0;
        let mut puts: Vec<(String, Vec<Bytes>)> = Vec::new();
        let mut store = match store.lock() {
            Ok(store) => store,
            Err(poisoned) => poisoned.into_inner(),
        };
        for op in batch {
            match op {
                Op::Put { seq, key, parts } => {
                    puts.push((key, parts));
                    done = seq;
                }
                // Ordered against the puts around it.
                Op::Remove { key } => {
                    flush(&mut store, &mut puts);
                    if let Err(e) = store.remove(&key) {
                        warn!("Could not remove {key} from the persistence store: {e}");
                    }
                }
                Op::Close => {
                    flush(&mut store, &mut puts);
                    let _ = store.close();
                }
                Op::Barrier { seq } => {
                    flush(&mut store, &mut puts);
                    done = seq;
                }
            }
        }
        flush(&mut store, &mut puts);
        drop(store);
        if done != 0 {
            progress.publish(done);
        }
    }
}

/// Hands the puts collected so far to the store, in one call.
///
/// A failure is reported and the sequence still counted as done: a message
/// that could not be stored is no reason to stop the session, and holding
/// its bytes back for ever would do exactly that. It is the same outcome a
/// failed `put` has always had, minus the silence.
fn flush(store: &mut Box<dyn ClientPersistence>, puts: &mut Vec<(String, Vec<Bytes>)>) {
    if puts.is_empty() {
        return;
    }
    if let Err(e) = store.put_many(std::mem::take(puts)) {
        warn!("Could not write to the persistence store: {e}");
    }
}

/// A store for tests elsewhere: it holds every write until it is let go,
/// so a test can see what the client does while the store is busy.
#[cfg(test)]
pub(crate) mod testing {
    use super::*;

    /// A store whose writes block until [`release`](HeldStore::release).
    pub(crate) struct HeldStore {
        released: Arc<AtomicBool>,
        writes: Arc<AtomicU64>,
    }

    impl HeldStore {
        /// The store, the switch that lets its writes through, and the
        /// count of writes that have completed.
        pub(crate) fn held() -> (Store, Arc<AtomicBool>, Arc<AtomicU64>) {
            let released = Arc::new(AtomicBool::new(false));
            let writes = Arc::new(AtomicU64::new(0));
            let store = HeldStore {
                released: Arc::clone(&released),
                writes: Arc::clone(&writes),
            };
            (
                Arc::new(Mutex::new(Box::new(store) as Box<_>)),
                released,
                writes,
            )
        }

        fn wait(&self) {
            while !self.released.load(Ordering::SeqCst) {
                thread::sleep(std::time::Duration::from_millis(1));
            }
        }
    }

    impl ClientPersistence for HeldStore {
        fn open(&mut self, _id: &str, _uri: &str) -> crate::Result<()> {
            Ok(())
        }
        fn close(&mut self) -> crate::Result<()> {
            Ok(())
        }
        fn put(&mut self, _key: &str, _bufs: Vec<&[u8]>) -> crate::Result<()> {
            self.wait();
            self.writes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn get(&mut self, _key: &str) -> crate::Result<Vec<u8>> {
            Ok(Vec::new())
        }
        fn remove(&mut self, _key: &str) -> crate::Result<()> {
            Ok(())
        }
        fn keys(&mut self) -> crate::Result<Vec<String>> {
            Ok(Vec::new())
        }
        fn clear(&mut self) -> crate::Result<()> {
            Ok(())
        }
        fn contains_key(&mut self, _key: &str) -> bool {
            false
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::MemPersistence;
    use std::time::Instant;

    /// A store that records what it was asked to do, and how.
    #[derive(Default)]
    struct SpyStore {
        log: Arc<Mutex<Vec<String>>>,
        /// Blocks every call for this long, standing in for a slow disk.
        delay: Duration,
    }

    impl ClientPersistence for SpyStore {
        fn open(&mut self, _id: &str, _uri: &str) -> crate::Result<()> {
            Ok(())
        }
        fn close(&mut self) -> crate::Result<()> {
            self.log.lock().unwrap().push("close".into());
            Ok(())
        }
        fn put(&mut self, key: &str, _bufs: Vec<&[u8]>) -> crate::Result<()> {
            thread::sleep(self.delay);
            self.log.lock().unwrap().push(format!("put {key}"));
            Ok(())
        }
        fn put_many(&mut self, items: Vec<(String, Vec<Bytes>)>) -> crate::Result<()> {
            thread::sleep(self.delay);
            let keys: Vec<&str> = items.iter().map(|(k, _)| k.as_str()).collect();
            self.log
                .lock()
                .unwrap()
                .push(format!("put_many {}", keys.join(",")));
            Ok(())
        }
        fn get(&mut self, _key: &str) -> crate::Result<Vec<u8>> {
            Ok(Vec::new())
        }
        fn remove(&mut self, key: &str) -> crate::Result<()> {
            self.log.lock().unwrap().push(format!("remove {key}"));
            Ok(())
        }
        fn keys(&mut self) -> crate::Result<Vec<String>> {
            Ok(Vec::new())
        }
        fn clear(&mut self) -> crate::Result<()> {
            Ok(())
        }
        fn contains_key(&mut self, _key: &str) -> bool {
            false
        }
    }

    fn spy(delay: Duration) -> (Store, Arc<Mutex<Vec<String>>>) {
        let log = Arc::new(Mutex::new(Vec::new()));
        let store = SpyStore {
            log: Arc::clone(&log),
            delay,
        };
        (Arc::new(Mutex::new(Box::new(store) as Box<_>)), log)
    }

    /// Waits for the worker to reach `seq`, or gives up.
    fn wait_done(w: &PersistWorker, seq: u64) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while w.done_seq() < seq && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        w.done_seq() >= seq
    }

    /// A burst of puts reaches the store as one `put_many`, and the worker
    /// reports the last of them done.
    #[test]
    fn test_consecutive_puts_are_batched() {
        let (store, log) = spy(Duration::from_millis(20));
        let mut w = PersistWorker::start(store, Duration::from_secs(30));
        // The first put occupies the worker; the rest pile up behind it and
        // go together.
        let mut last = 0;
        for i in 0..5u8 {
            last = w.put(format!("s{i}"), vec![Bytes::from_static(b"x")]);
        }
        assert!(wait_done(&w, last), "the worker did not finish");

        let log = log.lock().unwrap().clone();
        assert!(
            log.iter().all(|e| e.starts_with("put_many")),
            "every write went through put_many: {log:?}"
        );
        let keys: Vec<&str> = log
            .iter()
            .flat_map(|e| e.trim_start_matches("put_many ").split(','))
            .collect();
        assert_eq!(keys, ["s0", "s1", "s2", "s3", "s4"], "in order: {log:?}");
        assert!(log.len() < 5, "and not one call each: {log:?}");
    }

    /// A remove cannot overtake the put it undoes, however they are
    /// batched.
    #[test]
    fn test_remove_stays_behind_its_put() {
        let (store, log) = spy(Duration::from_millis(10));
        let mut w = PersistWorker::start(store, Duration::from_secs(30));
        w.put("s1".into(), vec![Bytes::from_static(b"x")]);
        w.remove("s1".into());
        let seq = w.put("s2".into(), vec![Bytes::from_static(b"y")]);
        assert!(wait_done(&w, seq));

        let log = log.lock().unwrap().clone();
        let joined = log.join(" | ");
        let put1 = joined.find("s1").expect("the put happened");
        let rm = joined.find("remove s1").expect("the remove happened");
        assert!(put1 < rm, "remove overtook its put: {joined}");
    }

    /// `idle` returns only once the queue has drained, so the loop can use
    /// the store directly without racing the worker.
    #[test]
    fn test_idle_waits_for_the_queue() {
        let (store, log) = spy(Duration::from_millis(30));
        let mut w = PersistWorker::start(store, Duration::from_secs(30));
        for i in 0..3u8 {
            w.put(format!("s{i}"), vec![Bytes::from_static(b"x")]);
        }
        futures::executor::block_on(w.idle());
        assert_eq!(
            log.lock().unwrap().len(),
            1,
            "idle returned before the queue drained"
        );
    }

    /// The store in-process is not worth a thread: the client runs it
    /// inline, and says so.
    #[test]
    fn test_memory_store_is_not_blocking() {
        assert!(!MemPersistence::new().is_blocking());
    }
}
