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
    future::FutureExt, // for `.fuse()`
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
// TokenData

/// Callback for the token on successful completion
pub type SuccessCallback = dyn Fn(&AsyncClient, u16) + 'static;

/// Callback for the token on failed completion
pub type FailureCallback = dyn Fn(&AsyncClient, u16, i32) + 'static;

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

    #[allow(dead_code)]
    pub fn from_error_descr(rc: i32, descr: &str) -> Self {
        Self {
            res: Some(Err(Error::from((rc, descr)))),
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
// TokenInner

/// The shared, heap-allocated core of a token.
///
/// Passed as the `context` pointer to the C library so the callback can
/// locate and resolve the right token. Kept alive by `Arc` reference
/// counting; the C library holds one count via `Arc::into_raw` /
/// `Arc::from_raw`.
pub(crate) struct TokenInner {
    /// Mutex-protected result and waker
    pub(crate) lock: Mutex<TokenData>,
    /// The client that created the token (used to invoke user callbacks)
    pub(crate) cli: Option<AsyncClient>,
    /// The type of request the token is tracking
    pub(crate) req: ServerRequest,
    /// User callback for successful completion
    on_success: Option<Box<SuccessCallback>>,
    /// User callback for failed completion
    on_failure: Option<Box<FailureCallback>>,
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

    pub fn with_client_callbacks<FS, FF>(
        cli: &AsyncClient,
        req: ServerRequest,
        success_cb: FS,
        failure_cb: FF,
    ) -> Arc<Self>
    where
        FS: Fn(&AsyncClient, u16) + 'static,
        FF: Fn(&AsyncClient, u16, i32) + 'static,
    {
        Arc::new(Self {
            cli: Some(cli.clone()),
            req,
            on_success: Some(Box::new(success_cb)),
            on_failure: Some(Box::new(failure_cb)),
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

    pub fn from_error(rc: i32) -> Arc<TokenInner> {
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

        if let Some(ref cli) = self.cli {
            if rc == 0 {
                if let Some(ref cb) = self.on_success {
                    trace!(
                        "Invoking Token success callback for client handle {:?}",
                        cli.handle()
                    );
                    cb(cli, msgid);
                }
            }
            else if let Some(ref cb) = self.on_failure {
                trace!(
                    "Invoking Token failure callback for client handle {:?}",
                    cli.handle()
                );
                cb(cli, msgid, rc);
            }
        }

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

        if let Some(ref cli) = self.cli {
            if rc == 0 {
                if let Some(ref cb) = self.on_success {
                    trace!(
                        "Invoking Token success callback for client handle {:?}",
                        cli.handle()
                    );
                    cb(cli, msgid);
                }
            }
            else if let Some(ref cb) = self.on_failure {
                trace!(
                    "Invoking Token failure callback for client handle {:?}",
                    cli.handle()
                );
                cb(cli, msgid, rc);
            }
        }

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
            on_success: None,
            on_failure: None,
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
// Token<T>

/// A `Token<T>` is a future that tracks an in-progress asynchronous MQTT
/// operation and resolves to `Result<T>` on completion.
///
/// Tokens are _eagerly started_: the operation begins when the `AsyncClient`
/// method is called, not when the token is polled or `.await`-ed.
pub struct Token<T: 'static + Send> {
    pub(crate) inner: Arc<TokenInner>,
    /// Converts the raw [`ServerResponse`] into the typed result `T`.
    convert: fn(ServerResponse) -> Result<T>,
}

impl<T: 'static + Send> Token<T> {
    /// Creates a new, pending token with the given convert function.
    pub(crate) fn new_pending(convert: fn(ServerResponse) -> Result<T>) -> Self {
        Self {
            inner: TokenInner::new(),
            convert,
        }
    }

    /// Creates a token that is already resolved with an error.
    pub(crate) fn new_err(err: Error, convert: fn(ServerResponse) -> Result<T>) -> Self {
        Self {
            inner: Arc::new(TokenInner::from(err)),
            convert,
        }
    }

    /// Creates a token pre-resolved with a C return-code error.
    pub(crate) fn from_error(rc: i32, convert: fn(ServerResponse) -> Result<T>) -> Self {
        Self {
            inner: TokenInner::from_error(rc),
            convert,
        }
    }

    /// Creates an unsignaled token associated with a client and request type.
    pub(crate) fn with_client(
        cli: &AsyncClient,
        req: ServerRequest,
        convert: fn(ServerResponse) -> Result<T>,
    ) -> Self {
        Self {
            inner: TokenInner::with_client(cli, req),
            convert,
        }
    }

    /// Creates an unsignaled token for the given request, optionally associated
    /// with a client.
    pub(crate) fn from_request<'a, C>(
        cli: C,
        req: ServerRequest,
        convert: fn(ServerResponse) -> Result<T>,
    ) -> Self
    where
        C: Into<Option<&'a AsyncClient>>,
    {
        Self {
            inner: TokenInner::from_request(cli, req),
            convert,
        }
    }

    /// Creates an unsignaled token with success and failure user callbacks.
    pub(crate) fn with_client_callbacks<FS, FF>(
        cli: &AsyncClient,
        req: ServerRequest,
        convert: fn(ServerResponse) -> Result<T>,
        success_cb: FS,
        failure_cb: FF,
    ) -> Self
    where
        FS: Fn(&AsyncClient, u16) + 'static,
        FF: Fn(&AsyncClient, u16, i32) + 'static,
    {
        Self {
            inner: TokenInner::with_client_callbacks(cli, req, success_cb, failure_cb),
            convert,
        }
    }

    /// Leaks the inner `Arc<TokenInner>` into a raw `*mut c_void` context
    /// pointer for the C library.
    pub(crate) fn into_raw(self) -> *mut c_void {
        Arc::into_raw(self.inner) as *mut c_void
    }

    /// Sets the MQTT message ID on the token.
    pub(crate) fn set_msgid(&self, msg_id: i16) {
        self.inner.lock.lock().unwrap().msg_id = msg_id;
    }

    /// Blocks the caller until the asynchronous operation completes.
    pub fn wait(self) -> Result<T> {
        block_on(self)
    }

    /// Non-blocking check — returns `Some(result)` if complete, `None` if still
    /// pending.
    pub fn try_wait(&mut self) -> Option<Result<T>> {
        self.now_or_never()
    }

    /// Blocks the caller for at most `dur`, returning `Err(Error::Timeout)` if
    /// the operation does not complete in time.
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
        Token {
            inner: self.inner.clone(),
            convert: self.convert,
        }
    }
}

unsafe impl<T: 'static + Send> Send for Token<T> {}

impl<T: 'static + Send> Future for Token<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut data = self.inner.lock.lock().unwrap();

        match data.res.take() {
            Some(Ok(rsp)) => Poll::Ready((self.convert)(rsp)),
            Some(Err(e)) => Poll::Ready(Err(e)),
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
// From<Error> for concrete token types

impl From<Error> for ConnectToken {
    fn from(err: Error) -> Self {
        Token::new_err(err, connect_convert)
    }
}

impl From<Error> for DeliveryToken {
    fn from(err: Error) -> Self {
        Token::new_err(err, publish_convert)
    }
}

impl From<Error> for SubscribeToken {
    fn from(err: Error) -> Self {
        Token::new_err(err, subscribe_convert)
    }
}

impl From<Error> for UnsubscribeToken {
    fn from(err: Error) -> Self {
        Token::new_err(err, unsubscribe_convert)
    }
}

impl From<Error> for Token<()> {
    fn from(err: Error) -> Self {
        Token::new_err(err, unit_convert)
    }
}

/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConnectReturnCode, ReasonCode, ServerRequest};

    #[test]
    fn test_new() {
        let tok = ConnectToken::new_pending(connect_convert);
        let data = tok.inner.lock.lock().unwrap();
        assert!(data.res.is_none());
    }

    #[test]
    fn test_from_message() {
        const MSG_ID: i16 = 42;
        let tok = DeliveryToken::new_pending(publish_convert);
        tok.set_msgid(MSG_ID);
        let data = tok.inner.lock.lock().unwrap();
        assert!(data.res.is_none());
        assert_eq!(MSG_ID, data.msg_id);
    }

    #[test]
    fn test_from_error() {
        const ERR_CODE: i32 = ffi::MQTTASYNC_BAD_QOS;

        let tok = ConnectToken::from_error(ERR_CODE, connect_convert);
        let data = tok.inner.lock.lock().unwrap();

        assert!(data.res.is_some());
        assert!(matches!(data.res, Some(Err(Error::BadQos))));
    }

    #[test]
    fn test_token_clones() {
        let tok1 = ConnectToken::new_pending(connect_convert);
        tok1.inner.lock.lock().unwrap().msg_id = 42;

        let tok2 = tok1.clone();

        let p1 = tok1.into_raw();
        let p2 = tok2.into_raw();

        assert_eq!(p1, p2);

        let inner1 = unsafe { TokenInner::from_raw(p1) };
        let inner2 = unsafe { TokenInner::from_raw(p2) };

        assert_eq!(42, inner1.lock.lock().unwrap().msg_id);
        assert_eq!(42, inner2.lock.lock().unwrap().msg_id);
    }

    #[test]
    fn test_token_send() {
        use std::thread;

        let tok = ConnectToken::new_pending(connect_convert);
        let tok2 = tok.clone();

        let thr = thread::spawn(move || tok.wait());

        tok2.inner.on_complete(0, 0, None, ptr::null_mut());
        let _ = thr.join().unwrap();
    }

    #[test]
    fn test_v3_connect_error() {
        use ConnectReturnCode::*;

        let tok = ConnectToken::from_request(None, ServerRequest::Connect, connect_convert);
        tok.inner.on_complete(0, 1, None, ptr::null_mut());
        let res = tok.wait();
        assert!(matches!(
            res,
            Err(Error::ConnectReturn(UnacceptableProtocolVersion))
        ));

        let tok = ConnectToken::from_request(None, ServerRequest::Connect, connect_convert);
        tok.inner.on_complete(0, 3, None, ptr::null_mut());
        let res = tok.wait();
        assert!(matches!(res, Err(Error::ConnectReturn(ServerUnavailable))));
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

        let tok =
            SubscribeToken::from_request(None, ServerRequest::Subscribe, subscribe_convert);
        let tok_wait = tok.clone();
        let ctx = tok.into_raw();

        let mut rsp = unsafe { std::mem::zeroed::<ffi::MQTTAsync_failureData5>() };
        rsp.token = 4;
        rsp.reasonCode = ffi::MQTTReasonCodes_MQTTREASONCODE_NOT_AUTHORIZED;

        unsafe { TokenInner::on_failure5(ctx, &mut rsp) };

        let res = tok_wait.wait();
        assert!(matches!(
            res,
            Err(Error::ReasonCode(ReasonCode::NotAuthorized, _))
        ));
    }

    #[test]
    fn test_try_wait() {
        const ERR_CODE: i32 = ffi::MQTTASYNC_BAD_QOS;
        let mut tok = ConnectToken::from_error(ERR_CODE, connect_convert);

        match tok.try_wait() {
            Some(Err(Error::BadQos)) => {}
            Some(Err(err)) => panic!("Wrong error: {}", err),
            Some(Ok(_)) => panic!("Should be an error"),
            None => panic!("Should be complete"),
        }

        let mut tok = ConnectToken::new_pending(connect_convert);

        if tok.try_wait().is_some() {
            panic!("Should not be complete");
        }

        {
            let mut data = tok.inner.lock.lock().unwrap();
            data.res = Some(Err(Error::BadQos));
        }

        match tok.try_wait() {
            Some(Err(Error::BadQos)) => (),
            Some(Err(err)) => panic!("Wrong error: {}", err),
            Some(Ok(_)) => panic!("Should be an error"),
            None => panic!("Should be complete"),
        }
    }
}
