// paho-mqtt/src/token.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2018-2026 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! The Token module for the Paho MQTT Rust client library.
//!
//! Asynchronous operations return a `Token<T>` that is a type of future. It
//! can be used to determine if an operation has completed, block and wait
//! for the operation to complete, and obtain the final result.
//!
//! Tokens are _eagerly started_: the underlying MQTT operation begins at the
//! point the `AsyncClient` method is called, not when the token is `.await`-ed.
//! Both `.await` (async) and `.wait()` / `.wait_for(dur)` (blocking) work on
//! the same token.
//!
//! ```no_run
//! use paho_mqtt as mqtt;
//!
//! # async fn run() -> mqtt::Result<()> {
//! let cli = mqtt::AsyncClient::new("mqtt://localhost:1883").unwrap();
//!
//! // Start the async operation and get a typed token.
//! let tok = cli.connect(mqtt::ConnectOptions::new());
//!
//! // ...do other work...
//!
//! // Wait for the connection to complete.
//! let rsp: mqtt::ConnectResponse = tok.await?;
//! # Ok(())
//! # }
//! ```

use crate::{
    Error, Result,
    responses::{ConnectResponse, PublishResponse, SubscribeResponse, UnsubscribeResponse},
};
use futures::{executor::block_on, future::FutureExt, pin_mut, select};
use futures_timer::Delay;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Duration,
};

/////////////////////////////////////////////////////////////////////////////
// FutureData — shared state for the Token

/// Typed shared state for a [`Token`].
struct FutureData<T> {
    result: Option<Result<T>>,
    waker: Option<Waker>,
}

impl<T> Default for FutureData<T> {
    fn default() -> Self {
        Self {
            result: None,
            waker: None,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
// Completer<T>

/// Completes a [`Token`] with the operation result.
///
/// Created by [`Token::new`]. The runtime holds the `Completer` and the
/// caller holds the matching [`Token`]. Dropping a `Completer` without
/// calling [`Completer::complete`] resolves the paired token with
/// [`Error::Disconnected`].
pub(crate) struct Completer<T: 'static + Send> {
    inner: Arc<Mutex<FutureData<T>>>,
}

impl<T: 'static + Send> Completer<T> {
    /// Creates a standalone completer with no paired [`Token`].
    ///
    /// Use when an operation needs to drive ACK-based cleanup (e.g. removing a
    /// persistence entry) but no caller is waiting on the result.  When
    /// [`complete`](Self::complete) is called the shared state is freed
    /// immediately; if dropped without completing it resolves to
    /// [`Error::Disconnected`] and the shared state is freed then.
    pub(crate) fn orphan() -> Self {
        Self {
            inner: Arc::new(Mutex::new(FutureData::default())),
        }
    }

    /// Resolves the paired [`Token`] with `result`.
    pub(crate) fn complete(self, result: Result<T>) {
        let inner = self.inner.clone();
        std::mem::forget(self);
        let mut data = inner.lock().unwrap();
        data.result = Some(result);
        if let Some(w) = data.waker.take() {
            w.wake();
        }
    }
}

impl<T: 'static + Send> Drop for Completer<T> {
    fn drop(&mut self) {
        let mut data = self.inner.lock().unwrap();
        if data.result.is_none() {
            data.result = Some(Err(Error::Disconnected));
            if let Some(w) = data.waker.take() {
                w.wake();
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
// Token<T>

/// A `Token<T>` is a future that tracks an in-progress asynchronous MQTT
/// operation and resolves to `Result<T>` on completion.
///
/// Tokens are _eagerly started_: the operation begins when the `AsyncClient`
/// method is called, not when the token is polled or `.await`-ed.
pub struct Token<T: 'static + Send> {
    inner: Arc<Mutex<FutureData<T>>>,
}

impl<T: 'static + Send> Token<T> {
    /// Creates a pending token and its paired [`Completer`].
    ///
    /// The runtime holds the `Completer`; the caller receives the `Token`.
    pub(crate) fn new() -> (Self, Completer<T>) {
        let inner = Arc::new(Mutex::new(FutureData::default()));
        let tok = Self {
            inner: inner.clone(),
        };
        let comp = Completer { inner };
        (tok, comp)
    }

    /// Creates a token already resolved with an error.
    pub(crate) fn from_err(err: Error) -> Self {
        let inner = Arc::new(Mutex::new(FutureData {
            result: Some(Err(err)),
            waker: None,
        }));
        Self { inner }
    }

    /// Blocks the caller until the asynchronous operation completes.
    pub fn wait(self) -> Result<T> {
        block_on(self)
    }

    /// Non-blocking check — returns `Some(result)` if complete, `None` if
    /// still pending.
    pub fn try_wait(&mut self) -> Option<Result<T>> {
        self.now_or_never()
    }

    /// Blocks the caller for at most `dur`, returning `Err(Error::Timeout)`
    /// if the operation does not complete in time.
    pub fn wait_for(self, dur: Duration) -> Result<T> {
        block_on(async move {
            let f = self.fuse();
            let to = Delay::new(dur).fuse();

            pin_mut!(f, to);

            select! {
                val = f => val,
                _ = to => Err(Error::Timeout),
            }
        })
    }
}

impl<T: 'static + Send> Clone for Token<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

unsafe impl<T: 'static + Send> Send for Token<T> {}

impl<T: 'static + Send> Future for Token<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut data = self.inner.lock().unwrap();
        match data.result.take() {
            Some(r) => Poll::Ready(r),
            None => {
                data.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
// Type aliases

/// A token for a connect operation — resolves to [`ConnectResponse`].
pub type ConnectToken = Token<ConnectResponse>;

/// A token for a publish (delivery) operation — resolves to [`PublishResponse`].
pub type DeliveryToken = Token<PublishResponse>;

/// A token for a single-topic subscribe — resolves to [`SubscribeResponse`].
pub type SubscribeToken = Token<SubscribeResponse>;

/// A token for a multi-topic subscribe — resolves to [`SubscribeResponse`].
pub type SubscribeManyToken = Token<SubscribeResponse>;

/// A token for a single-topic unsubscribe — resolves to [`UnsubscribeResponse`].
pub type UnsubscribeToken = Token<UnsubscribeResponse>;

/// A token for a multi-topic unsubscribe — resolves to [`UnsubscribeResponse`].
pub type UnsubscribeManyToken = Token<UnsubscribeResponse>;

/////////////////////////////////////////////////////////////////////////////
// From<Error> conversions

impl From<Error> for ConnectToken {
    fn from(err: Error) -> Self {
        Token::from_err(err)
    }
}

impl From<Error> for DeliveryToken {
    fn from(err: Error) -> Self {
        Token::from_err(err)
    }
}

impl From<Error> for SubscribeToken {
    fn from(err: Error) -> Self {
        Token::from_err(err)
    }
}

impl From<Error> for UnsubscribeToken {
    fn from(err: Error) -> Self {
        Token::from_err(err)
    }
}

impl From<Error> for Token<()> {
    fn from(err: Error) -> Self {
        Token::from_err(err)
    }
}

/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_pending() {
        let (tok, _comp) = ConnectToken::new();
        assert!(tok.inner.lock().unwrap().result.is_none());
    }

    #[test]
    fn test_completer_complete() {
        let (tok, comp) = ConnectToken::new();
        comp.complete(Ok(ConnectResponse::default()));
        let res = tok.wait();
        assert!(res.is_ok());
    }

    #[test]
    fn test_completer_complete_err() {
        let (tok, comp) = ConnectToken::new();
        comp.complete(Err(Error::Timeout));
        let res = tok.wait();
        assert!(matches!(res, Err(Error::Timeout)));
    }

    #[test]
    fn test_completer_drop_resolves_disconnected() {
        let (tok, comp) = ConnectToken::new();
        drop(comp);
        let res = tok.wait();
        assert!(matches!(res, Err(Error::Disconnected)));
    }

    #[test]
    fn test_from_err() {
        let tok = ConnectToken::from_err(Error::Timeout);
        let res = tok.wait();
        assert!(matches!(res, Err(Error::Timeout)));
    }

    #[test]
    fn test_token_send() {
        use std::thread;

        let (tok, comp) = ConnectToken::new();

        let thr = thread::spawn(move || tok.wait());

        comp.complete(Ok(ConnectResponse::default()));
        let res = thr.join().unwrap();
        assert!(res.is_ok());
    }

    #[test]
    fn test_try_wait() {
        let (mut tok, comp) = ConnectToken::new();
        comp.complete(Err(Error::Timeout));
        match tok.try_wait() {
            Some(Err(Error::Timeout)) => {}
            other => panic!("unexpected: {:?}", other),
        }

        let (mut tok, _comp) = ConnectToken::new();
        if tok.try_wait().is_some() {
            panic!("should be pending");
        }
    }
}
