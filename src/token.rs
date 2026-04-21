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
    AsyncClient, Error, Properties, Result, ServerRequest, ServerResponse, ffi,
    responses::{
        ConnectResponse, PublishResponse, SubscribeResponse, UnsubscribeResponse,
        connect_convert, publish_convert, subscribe_convert, unit_convert,
        unsubscribe_convert,
    },
};
use futures::{
    executor::block_on,
    future::FutureExt, // for `.fuse()` / `now_or_never`
    pin_mut,
    select,
};
use futures_timer::Delay;
use std::{
    ffi::CStr,
    future::Future,
    os::raw::c_void,
    pin::Pin,
    ptr,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Duration,
};

/////////////////////////////////////////////////////////////////////////////
// FutureData — shared state for the new, channel-based Token path

/// Typed shared state for a [`Token`] created via [`Token::new`].
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
    /// Resolves the paired [`Token`] with `result`.
    pub(crate) fn complete(self, result: Result<T>) {
        // Clone the Arc so we can lock without holding a borrow on `self`.
        let inner = self.inner.clone();
        // Forget self before locking to prevent Drop from also writing a result.
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
// TokenData — shared state for the legacy C-library Token path

/// The result data for the token, protected by a mutex and updated by the
/// C library callback when the asynchronous operation completes.
#[derive(Debug, Default)]
pub(crate) struct TokenData {
    /// The MQTT Message ID
    pub(crate) msg_id: i16,
    /// When `Some`, the raw result of the operation (success or error).
    /// `None` means the operation is still in progress.
    res: Option<Result<ServerResponse>>,
    /// Waker to notify the executor when the operation completes.
    waker: Option<Waker>,
}

impl TokenData {
    pub fn from_error(rc: i32) -> Self {
        Self {
            res: Some(Err(Error::from(rc))),
            ..Self::default()
        }
    }
}

impl From<Error> for TokenData {
    fn from(err: Error) -> Self {
        Self {
            res: Some(Err(err)),
            ..Self::default()
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
// TokenInner — legacy C-library bridge

/// The shared, heap-allocated core of a legacy token.
///
/// Passed as the `context` pointer to the C library so the callback can
/// locate and resolve the right token. Kept alive by `Arc` reference
/// counting; the C library holds one count via `Arc::into_raw` /
/// `Arc::from_raw`.
pub(crate) struct TokenInner {
    /// Mutex-protected result and waker
    pub(crate) lock: Mutex<TokenData>,
    /// The client that created the token (used for post-connect bookkeeping)
    pub(crate) cli: Option<AsyncClient>,
    /// The type of request the token is tracking
    pub(crate) req: ServerRequest,
}

impl TokenInner {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn with_client(cli: &AsyncClient, req: ServerRequest) -> Arc<Self> {
        Arc::new(Self {
            cli: Some(cli.clone()),
            req,
            ..Self::default()
        })
    }

    pub fn from_request<'a, C>(cli: C, req: ServerRequest) -> Arc<Self>
    where
        C: Into<Option<&'a AsyncClient>>,
    {
        let cli = cli.into().cloned();
        Arc::new(Self {
            cli,
            req,
            ..Self::default()
        })
    }

    pub fn from_error(rc: i32) -> Arc<Self> {
        Arc::new(Self {
            lock: Mutex::new(TokenData::from_error(rc)),
            ..Self::default()
        })
    }

    /// Reconstructs an `Arc<TokenInner>` from a raw context pointer.
    ///
    /// # Safety
    ///
    /// `ptr` must be a pointer previously produced by `Arc::into_raw` on an
    /// `Arc<TokenInner>`.
    pub(crate) unsafe fn from_raw(ptr: *mut c_void) -> Arc<Self> {
        unsafe { Arc::from_raw(ptr as *const Self) }
    }

    // ---- C callbacks ----

    pub(crate) unsafe extern "C" fn on_success(
        context: *mut c_void,
        rsp: *mut ffi::MQTTAsync_successData,
    ) {
        if context.is_null() {
            warn!("Received a token v3 success callback with no context");
            return;
        }

        debug!("Token success! Token: {:?}, Response: {:?}", context, rsp);
        let inner = unsafe { TokenInner::from_raw(context) };

        let msgid = match rsp.is_null() {
            true => 0,
            false => unsafe { (*rsp).token as u16 },
        };
        inner.on_complete(msgid, 0, None, rsp);
    }

    pub(crate) unsafe extern "C" fn on_failure(
        context: *mut c_void,
        rsp: *mut ffi::MQTTAsync_failureData,
    ) {
        if context.is_null() {
            warn!("Received a token v3 failure callback with no context");
            return;
        }

        debug!("Token failure! Token: {:?}, Response: {:?}", context, rsp);
        let inner = unsafe { TokenInner::from_raw(context) };

        let mut msgid = 0;
        let mut rc = -1;
        let mut err_msg = None;

        if let Some(rsp) = unsafe { rsp.as_ref() } {
            msgid = rsp.token as u16;
            rc = if rsp.code == 0 { -1 } else { rsp.code as i32 };

            if !rsp.message.is_null()
                && let Ok(cmsg) = unsafe { CStr::from_ptr(rsp.message).to_str() }
            {
                debug!("Token failure message: {:?}", cmsg);
                err_msg = Some(cmsg.to_string());
            }
        }

        inner.on_complete(msgid, rc, err_msg, ptr::null_mut());
    }

    pub(crate) unsafe extern "C" fn on_success5(
        context: *mut c_void,
        rsp: *mut ffi::MQTTAsync_successData5,
    ) {
        if context.is_null() {
            warn!("Received a token v5 success callback with no context");
            return;
        }

        debug!(
            "Token v5 success! Token: {:?}, Response: {:?}",
            context, rsp
        );
        let inner = unsafe { TokenInner::from_raw(context) };

        let msgid = unsafe { rsp.as_ref() }
            .map(|rsp| rsp.token as u16)
            .unwrap_or(0);

        debug!("Expecting server response for: {:?}", inner.req);

        let rsp = unsafe { rsp.as_ref() }
            .map(|rsp| unsafe { ServerResponse::from_success5(inner.req, rsp) })
            .unwrap_or_default();
        debug!("Got response: {rsp:?}");

        if let Some(data) = rsp.connect_data()
            && let Some(cli) = inner.cli.as_ref()
        {
            cli.set_mqtt_version(data.mqtt_version);
        }

        inner.on_complete5(msgid, 0, Ok(rsp));
    }

    pub(crate) unsafe extern "C" fn on_failure5(
        context: *mut c_void,
        rsp: *mut ffi::MQTTAsync_failureData5,
    ) {
        if context.is_null() {
            warn!("Received a token v5 failure callback with no context");
            return;
        }

        debug!(
            "Token v5 failure! Token: {:?}, Response: {:?}",
            context, rsp
        );
        let inner = unsafe { TokenInner::from_raw(context) };

        let mut msgid = 0;
        let mut rc = -1;
        let mut err_msg = None;
        let mut props = Properties::default();

        if let Some(rsp) = unsafe { rsp.as_ref() } {
            msgid = rsp.token as u16;
            rc = if rsp.reasonCode >= 0x80 {
                debug!(
                    "Token w ID {} failed with reason code: {}",
                    msgid, rsp.reasonCode
                );
                props = Properties::from(&rsp.properties);
                rsp.reasonCode as i32
            }
            else if rsp.code == 0 {
                warn!("Token w ID {} failed with no reported error", msgid);
                ffi::MQTTASYNC_FAILURE
            }
            else {
                debug!("Token w ID {} failed with return code: {}", msgid, rsp.code);
                rsp.code as i32
            };

            if !rsp.message.is_null()
                && let Ok(cmsg) = unsafe { CStr::from_ptr(rsp.message).to_str() }
            {
                debug!("Token failure message: {:?}", cmsg);
                err_msg = Some(cmsg.to_string());
            }
        }

        let err = if rc > 0 {
            debug_assert!(rc >= 0x80);
            Err(Error::from_reason_code(rc as u8, props))
        }
        else {
            Err(Error::from((rc, err_msg)))
        };

        inner.on_complete5(msgid, rc, err);
    }

    // ---- completion helpers ----

    pub(crate) fn on_complete(
        &self,
        msgid: u16,
        rc: i32,
        err_msg: Option<String>,
        rsp: *mut ffi::MQTTAsync_successData,
    ) {
        debug!("Completing Token w ID {} and code: {}", msgid, rc);

        let mut data = self.lock.lock().unwrap();

        let res = unsafe {
            use std::cmp::Ordering;

            match rc.cmp(&0) {
                Ordering::Equal => {
                    debug!("Expecting server response for: {:?}", self.req);
                    let rsp = if let Some(rsp) = rsp.as_ref() {
                        ServerResponse::from_success(self.req, rsp)
                    }
                    else {
                        ServerResponse::default()
                    };
                    debug!("Got response: {:?}", rsp);

                    if let Some(data) = rsp.connect_data()
                        && let Some(cli) = &self.cli
                    {
                        cli.set_mqtt_version(data.mqtt_version);
                    }
                    Ok(rsp)
                }
                Ordering::Greater => {
                    if self.req == ServerRequest::Connect {
                        Err(Error::from_connect_return_code(rc as u8))
                    }
                    else {
                        warn!("Unknown positive return code for {:?}: {}", self.req, rc);
                        Err(Error::from(-1))
                    }
                }
                Ordering::Less => Err(Error::from((rc, err_msg))),
            }
        };

        data.res = Some(res);

        if let Some(waker) = data.waker.take() {
            waker.wake()
        }
    }

    pub(crate) fn on_complete5(&self, msgid: u16, rc: i32, res: Result<ServerResponse>) {
        debug!("Completing v5 Token w ID {} and code: {}", msgid, rc);

        let mut data = self.lock.lock().unwrap();
        data.res = Some(res);

        if let Some(waker) = data.waker.take() {
            waker.wake()
        }
    }
}

impl Default for TokenInner {
    fn default() -> Self {
        Self {
            lock: Mutex::new(TokenData::default()),
            cli: None,
            req: ServerRequest::None,
        }
    }
}

impl From<Error> for TokenInner {
    fn from(err: Error) -> Self {
        Self {
            lock: Mutex::new(TokenData::from(err)),
            ..Self::default()
        }
    }
}

unsafe impl Send for TokenInner {}
unsafe impl Sync for TokenInner {}

/////////////////////////////////////////////////////////////////////////////
// TokenKind — internal two-path discriminant

/// Selects between the new channel-based and legacy C-library completion paths.
enum TokenKind<T: 'static + Send> {
    /// New runtime path: completed via a [`Completer<T>`].
    New(Arc<Mutex<FutureData<T>>>),
    /// Legacy C-library path: completed via a C callback writing to [`TokenInner`].
    Legacy(Arc<TokenInner>, fn(ServerResponse) -> Result<T>),
}

/////////////////////////////////////////////////////////////////////////////
// Token<T>

/// A `Token<T>` is a future that tracks an in-progress asynchronous MQTT
/// operation and resolves to `Result<T>` on completion.
///
/// Tokens are _eagerly started_: the operation begins when the `AsyncClient`
/// method is called, not when the token is polled or `.await`-ed.
pub struct Token<T: 'static + Send> {
    kind: TokenKind<T>,
}

impl<T: 'static + Send> Token<T> {
    // ---- New runtime path ----

    /// Creates a pending token and its paired [`Completer`].
    ///
    /// The runtime holds the `Completer`; the caller receives the `Token`.
    pub(crate) fn new() -> (Self, Completer<T>) {
        let inner = Arc::new(Mutex::new(FutureData::default()));
        let tok = Self {
            kind: TokenKind::New(inner.clone()),
        };
        let comp = Completer { inner };
        (tok, comp)
    }

    /// Creates a token already resolved with an error (new path).
    pub(crate) fn from_err(err: Error) -> Self {
        let inner = Arc::new(Mutex::new(FutureData {
            result: Some(Err(err)),
            waker: None,
        }));
        Self {
            kind: TokenKind::New(inner),
        }
    }

    // ---- Legacy C-library path ----

    /// Creates a new, pending legacy token.
    pub(crate) fn new_pending(convert: fn(ServerResponse) -> Result<T>) -> Self {
        Self {
            kind: TokenKind::Legacy(TokenInner::new(), convert),
        }
    }

    /// Creates a legacy token that is already resolved with an error.
    pub(crate) fn new_err(err: Error, convert: fn(ServerResponse) -> Result<T>) -> Self {
        Self {
            kind: TokenKind::Legacy(Arc::new(TokenInner::from(err)), convert),
        }
    }

    /// Creates a legacy token pre-resolved with a C return-code error.
    pub(crate) fn from_error(rc: i32, convert: fn(ServerResponse) -> Result<T>) -> Self {
        Self {
            kind: TokenKind::Legacy(TokenInner::from_error(rc), convert),
        }
    }

    /// Creates an unsignalled legacy token associated with a client and
    /// request type.
    pub(crate) fn with_client(
        cli: &AsyncClient,
        req: ServerRequest,
        convert: fn(ServerResponse) -> Result<T>,
    ) -> Self {
        Self {
            kind: TokenKind::Legacy(TokenInner::with_client(cli, req), convert),
        }
    }

    /// Creates an unsignalled legacy token for the given request.
    pub(crate) fn from_request<'a, C>(
        cli: C,
        req: ServerRequest,
        convert: fn(ServerResponse) -> Result<T>,
    ) -> Self
    where
        C: Into<Option<&'a AsyncClient>>,
    {
        Self {
            kind: TokenKind::Legacy(TokenInner::from_request(cli, req), convert),
        }
    }

    // ---- C-library interop (legacy path only) ----

    /// Returns a clone of the `Arc<TokenInner>` for legacy C-library integration.
    ///
    /// # Panics
    ///
    /// Panics if called on a new-style token.
    pub(crate) fn inner(&self) -> Arc<TokenInner> {
        match &self.kind {
            TokenKind::Legacy(inner, _) => inner.clone(),
            TokenKind::New(_) => panic!("Token::inner called on new-style token"),
        }
    }

    /// Leaks the inner `Arc<TokenInner>` into a raw `*mut c_void` context
    /// pointer for the C library.
    pub(crate) fn into_raw(self) -> *mut c_void {
        match self.kind {
            TokenKind::Legacy(inner, _) => Arc::into_raw(inner) as *mut c_void,
            TokenKind::New(_) => panic!("Token::into_raw called on new-style token"),
        }
    }

    /// Sets the MQTT message ID on a legacy token.
    pub(crate) fn set_msgid(&self, msg_id: i16) {
        if let TokenKind::Legacy(inner, _) = &self.kind {
            inner.lock.lock().unwrap().msg_id = msg_id;
        }
    }

    // ---- Public API ----

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
        let kind = match &self.kind {
            TokenKind::New(inner) => TokenKind::New(inner.clone()),
            TokenKind::Legacy(inner, convert) => TokenKind::Legacy(inner.clone(), *convert),
        };
        Self { kind }
    }
}

unsafe impl<T: 'static + Send> Send for Token<T> {}

impl<T: 'static + Send> Future for Token<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &self.kind {
            TokenKind::New(inner) => {
                let mut data = inner.lock().unwrap();
                match data.result.take() {
                    Some(r) => Poll::Ready(r),
                    None => {
                        data.waker = Some(cx.waker().clone());
                        Poll::Pending
                    }
                }
            }
            TokenKind::Legacy(inner, convert) => {
                let mut data = inner.lock.lock().unwrap();
                match data.res.take() {
                    Some(Ok(rsp)) => Poll::Ready(convert(rsp)),
                    Some(Err(e)) => Poll::Ready(Err(e)),
                    None => {
                        data.waker = Some(cx.waker().clone());
                        Poll::Pending
                    }
                }
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
    use crate::{ConnectReturnCode, ReasonCode, ServerRequest};

    // ---- New runtime path ----

    #[test]
    fn test_new_pending() {
        let (tok, _comp) = ConnectToken::new();
        match &tok.kind {
            TokenKind::New(inner) => assert!(inner.lock().unwrap().result.is_none()),
            _ => panic!("expected New token"),
        }
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

    // ---- Legacy C-library path ----

    #[test]
    fn test_legacy_new_pending() {
        let tok = ConnectToken::new_pending(connect_convert);
        match &tok.kind {
            TokenKind::Legacy(inner, _) => {
                assert!(inner.lock.lock().unwrap().res.is_none())
            }
            _ => panic!("expected Legacy token"),
        }
    }

    #[test]
    fn test_legacy_from_error() {
        const ERR_CODE: i32 = ffi::MQTTASYNC_BAD_QOS;

        let tok = ConnectToken::from_error(ERR_CODE, connect_convert);
        match &tok.kind {
            TokenKind::Legacy(inner, _) => {
                assert!(matches!(
                    inner.lock.lock().unwrap().res,
                    Some(Err(Error::BadQos))
                ))
            }
            _ => panic!("expected Legacy token"),
        }
    }

    #[test]
    fn test_legacy_v3_connect_error() {
        use ConnectReturnCode::*;

        let tok = ConnectToken::from_request(None, ServerRequest::Connect, connect_convert);
        let _ = tok.clone().into_raw();
        if let TokenKind::Legacy(inner, _) = &tok.kind {
            inner.on_complete(0, 1, None, ptr::null_mut());
        }
        let res = tok.wait();
        assert!(matches!(
            res,
            Err(Error::ConnectReturn(UnacceptableProtocolVersion))
        ));
    }

    #[test]
    fn test_try_wait() {
        // Pre-resolved token via Completer
        let (mut tok, comp) = ConnectToken::new();
        comp.complete(Err(Error::Timeout));
        match tok.try_wait() {
            Some(Err(Error::Timeout)) => {}
            other => panic!("unexpected: {:?}", other),
        }

        // Pending token
        let (mut tok, _comp) = ConnectToken::new();
        if tok.try_wait().is_some() {
            panic!("should be pending");
        }
    }

    #[test]
    fn test_on_success5() {
        let tok =
            SubscribeToken::from_request(None, ServerRequest::Subscribe, subscribe_convert);
        let tok_wait = tok.clone();
        let ctx = tok.into_raw();

        let mut rsp = unsafe { std::mem::zeroed::<ffi::MQTTAsync_successData5>() };
        rsp.token = 1;

        unsafe { TokenInner::on_success5(ctx, &mut rsp) };

        let res = tok_wait.wait();
        assert!(res.is_ok());
    }

    #[test]
    fn test_on_failure5_c_error() {
        let tok =
            SubscribeToken::from_request(None, ServerRequest::Subscribe, subscribe_convert);
        let tok_wait = tok.clone();
        let ctx = tok.into_raw();

        let mut rsp = unsafe { std::mem::zeroed::<ffi::MQTTAsync_failureData5>() };
        rsp.token = 2;
        rsp.code = ffi::MQTTASYNC_DISCONNECTED;

        unsafe { TokenInner::on_failure5(ctx, &mut rsp) };

        let res = tok_wait.wait();
        assert!(matches!(res, Err(Error::Disconnected)));
    }

    #[test]
    fn test_on_failure5_reason_code() {
        let tok =
            SubscribeToken::from_request(None, ServerRequest::Subscribe, subscribe_convert);
        let tok_wait = tok.clone();
        let ctx = tok.into_raw();

        let mut rsp = unsafe { std::mem::zeroed::<ffi::MQTTAsync_failureData5>() };
        rsp.token = 3;
        rsp.reasonCode = ffi::MQTTReasonCodes_MQTTREASONCODE_UNSPECIFIED_ERROR;

        unsafe { TokenInner::on_failure5(ctx, &mut rsp) };

        let res = tok_wait.wait();
        assert!(matches!(
            res,
            Err(Error::ReasonCode(ReasonCode::UnspecifiedError, _))
        ));
    }
}
