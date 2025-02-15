// paho-mqtt/src/token.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2018-2023 Frank Pagliughi <fpagliughi@mindspring.com>
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
//! Asynchronous operations return a `Token` that is a type of future. It
//! can be used to determine if an operation has completed, block and wait
//! for the operation to complete, and obtain the final result.
//! For example, you can start a connection, do something else, and then
//! wait for the connection to complete.
//!
//! The Token object implements the Future trait, and thus can be used and
//! combined with any other Rust futures.
//!

use {
    crate::{
        async_client::AsyncClient,
        ffi,
        message::Message,
        server_response::{ServerRequest, ServerResponse},
        Error, Result,
    },
    futures::{
        executor::block_on,
        future::FutureExt, // for `.fuse()`
        pin_mut,
        select,
    },
    futures_timer::Delay,
    std::{
        ffi::CStr,
        future::Future,
        os::raw::c_void,
        pin::Pin,
        ptr,
        sync::{Arc, Mutex},
        task::{Context, Poll, Waker},
        time::Duration,
    },
};

/////////////////////////////////////////////////////////////////////////////
// TokenData

/// Callback for the token on successful completion
pub type SuccessCallback = dyn Fn(&AsyncClient, u16) + 'static;

/// Callback for the token on failed completion
pub type FailureCallback = dyn Fn(&AsyncClient, u16, i32) + 'static;

/// The result data for the token.
/// This contains the guarded elements in the token which are updated by
/// the C library callback when the asynchronous operation completes.
#[derive(Debug, Default)]
pub(crate) struct TokenData {
    /// The MQTT Message ID
    msg_id: i16,
    /// When Some, the result of the operation, response or error.
    /// None means not complete yet.
    res: Option<Result<ServerResponse>>,
    /// To wake the future on completion
    waker: Option<Waker>,
}

impl TokenData {
    /// Creates token data for a specific message
    pub fn from_message_id(msg_id: i16) -> Self {
        Self {
            msg_id,
            ..Self::default()
        }
    }

    /// Creates a new token that is already signaled with an error.
    pub fn from_error(rc: i32) -> Self {
        Self {
            res: Some(Err(Error::from(rc))),
            ..Self::default()
        }
    }

    /// Creates a new token that is already signaled with an error.
    // TODO: Use this!
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

/// The actual data structure for an asynchronous token.
/// Instances of this are passed as the context pointer to the C library
/// to track asynchronous operations. They are kept on the heap, via Arc
/// pointers so that their addresses don't change in memory. The internal
/// `TokenData` is thread-protected by a mutex as it is updated by the
/// async callback in a thread originating in the C lib. The other fields
/// are set at creation and remain static.
//#[derive(Debug)]
pub(crate) struct TokenInner {
    /// Mutex guards: (done, ret, msgid)
    lock: Mutex<TokenData>,
    /// The client that created the token.
    cli: Option<AsyncClient>,
    /// The type of request the token is tracking
    req: ServerRequest,
    /// User callback for successful completion of the async action
    on_success: Option<Box<SuccessCallback>>,
    /// User callback for failed completion of the async action
    on_failure: Option<Box<FailureCallback>>,
}

impl TokenInner {
    /// Creates a new, unsignaled Token.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Creates a token for a specific request type
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

    /// Creates a new, un-signaled delivery Token.
    /// This is a token which tracks delivery of a message.
    pub fn from_message(msg: &Message) -> Arc<Self> {
        Arc::new(Self {
            lock: Mutex::new(TokenData::from_message_id(msg.cmsg.msgid as i16)),
            ..Self::default()
        })
    }

    /// Creates a new, un-signaled Token with callbacks.
    pub fn from_client<FS, FF>(
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

    /// Creates a new Token signaled with a return code.
    pub fn from_error(rc: i32) -> Arc<TokenInner> {
        Arc::new(Self {
            lock: Mutex::new(TokenData::from_error(rc)),
            ..Self::default()
        })
    }

    // Callback from the C library for when an MQTT v3.x operation succeeds.
    pub(crate) unsafe extern "C" fn on_success(
        context: *mut c_void,
        rsp: *mut ffi::MQTTAsync_successData,
    ) {
        debug!("Token success! Token: {:?}, Response: {:?}", context, rsp);
        if context.is_null() {
            return;
        }

        let tok = Token::from_raw(context);

        // TODO: Maybe compare this msgid to the one in the token?
        let msgid = match rsp.is_null() {
            true => 0,
            false => (*rsp).token as u16,
        };
        tok.inner.on_complete(msgid, 0, None, rsp);
    }

    // Callback from the C library when an MQTT v3.x operation fails.
    pub(crate) unsafe extern "C" fn on_failure(
        context: *mut c_void,
        rsp: *mut ffi::MQTTAsync_failureData,
    ) {
        debug!("Token failure! Token: {:?}, Response: {:?}", context, rsp);
        if context.is_null() {
            return;
        }

        let tok = Token::from_raw(context);

        let mut msgid = 0;
        let mut rc = -1;
        let mut err_msg = None;

        if let Some(rsp) = rsp.as_ref() {
            msgid = rsp.token as u16;
            rc = if rsp.code == 0 { -1 } else { rsp.code as i32 };

            if !rsp.message.is_null() {
                if let Ok(cmsg) = CStr::from_ptr(rsp.message).to_str() {
                    debug!("Token failure message: {:?}", cmsg);
                    err_msg = Some(cmsg.to_string());
                }
            }
        }

        tok.inner.on_complete(msgid, rc, err_msg, ptr::null_mut());
    }

    // Callback from the C library for when an MQTT v5 async operation succeeds.
    pub(crate) unsafe extern "C" fn on_success5(
        context: *mut c_void,
        rsp: *mut ffi::MQTTAsync_successData5,
    ) {
        debug!(
            "Token v5 success! Token: {:?}, Response: {:?}",
            context, rsp
        );
        if context.is_null() {
            return;
        }

        let tok = Token::from_raw(context);

        // TODO: Maybe compare this msgid to the one in the token?
        let msgid = match rsp.is_null() {
            false => 0,
            true => (*rsp).token as u16,
        };
        tok.inner.on_complete5(msgid, 0, None, rsp);
    }

    // Callback from the C library when an MQTT v5 async operation fails.
    pub(crate) unsafe extern "C" fn on_failure5(
        context: *mut c_void,
        rsp: *mut ffi::MQTTAsync_failureData5,
    ) {
        debug!(
            "Token v5 failure! Token: {:?}, Response: {:?}",
            context, rsp
        );
        if context.is_null() {
            return;
        }

        let tok = Token::from_raw(context);

        let mut msgid = 0;
        let mut rc = -1;
        let mut err_msg = None;

        if let Some(rsp) = rsp.as_ref() {
            msgid = rsp.token as u16;
            rc = if rsp.code == 0 { -1 } else { rsp.code as i32 };

            if !rsp.message.is_null() {
                if let Ok(cmsg) = CStr::from_ptr(rsp.message).to_str() {
                    debug!("Token failure message: {:?}", cmsg);
                    err_msg = Some(cmsg.to_string());
                }
            }
        }

        debug!("Token w ID {} failed with code: {}", msgid, rc);

        // Fire off any user callbacks

        if let Some(ref cli) = tok.inner.cli {
            if let Some(ref cb) = tok.inner.on_failure {
                trace!(
                    "Invoking Token failure callback for client handle {:?}",
                    cli.handle()
                );
                cb(cli, msgid, rc);
            }
        }

        // Signal completion of the token

        let mut data = tok.inner.lock.lock().unwrap();
        data.res = Some(if rc == 0 {
            if let Some(rsp) = rsp.as_ref() {
                Ok(ServerResponse::from_failure5(rsp))
            }
            else {
                Ok(ServerResponse::default())
            }
        }
        else {
            Err(Error::from((rc, err_msg)))
        });

        // If this is none, it means that no one is waiting on
        // the future yet, so we don't need to wake it.
        if let Some(waker) = data.waker.take() {
            waker.wake();
        }
    }

    // Callback function to update the token when the action completes.
    pub(crate) fn on_complete(
        &self,
        msgid: u16,
        rc: i32,
        err_msg: Option<String>,
        rsp: *mut ffi::MQTTAsync_successData,
    ) {
        debug!("Completing Token w ID {} and code: {}", msgid, rc);

        // Fire off any user callbacks

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

        // Signal completion of the token

        let mut data = self.lock.lock().unwrap();
        unsafe {
            data.res = Some(if rc == 0 {
                // Get the response from the server, if any.
                debug!("Expecting server response for: {:?}", self.req);
                let rsp = if let Some(rsp) = rsp.as_ref() {
                    ServerResponse::from_success(self.req, rsp)
                }
                else {
                    ServerResponse::default()
                };
                debug!("Got response: {:?}", rsp);

                if let Some(rsp) = rsp.connect_response() {
                    if let Some(cli) = &self.cli {
                        cli.set_mqtt_version(rsp.mqtt_version);
                    }
                }
                Ok(rsp)
            }
            else {
                Err(Error::from((rc, err_msg)))
            });
        }

        // If this is none, it means that no one is waiting on
        // the future yet, so we don't need to wake it.
        if let Some(waker) = data.waker.take() {
            waker.wake()
        }
    }

    // Callback function to update the token when the action completes.
    pub(crate) fn on_complete5(
        &self,
        msgid: u16,
        rc: i32,
        err_msg: Option<String>,
        rsp: *mut ffi::MQTTAsync_successData5,
    ) {
        debug!("Token completed with code: {}", rc);

        // Fire off any user callbacks

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

        // Signal completion of the token

        let mut data = self.lock.lock().unwrap();
        unsafe {
            data.res = Some(if rc == 0 {
                // Get the response from the server, if any.
                debug!("Expecting server response for: {:?}", self.req);
                let rsp = if let Some(rsp) = rsp.as_ref() {
                    ServerResponse::from_success5(self.req, rsp)
                }
                else {
                    ServerResponse::default()
                };
                debug!("Got response: {:?}", rsp);

                if let Some(rsp) = rsp.connect_response() {
                    if let Some(cli) = &self.cli {
                        cli.set_mqtt_version(rsp.mqtt_version);
                    }
                }
                Ok(rsp)
            }
            else {
                Err(Error::from((rc, err_msg)))
            });
        }

        // If this is none, it means that no one is waiting on
        // the future yet, so we don't need to wake it.
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
// Token

/// A `Token` is a mechanism for tracking the progress of an asynchronous
/// operation.
#[derive(Clone)]
pub struct Token {
    pub(crate) inner: Arc<TokenInner>,
}

impl Token {
    /// Creates a new, unsignaled Token.
    pub fn new() -> Self {
        Self {
            inner: TokenInner::new(),
        }
    }

    /// Creates a token for a specific request type
    pub fn from_request<'a, C>(cli: C, req: ServerRequest) -> Self
    where
        C: Into<Option<&'a AsyncClient>>,
    {
        Self {
            inner: TokenInner::from_request(cli, req),
        }
    }

    /// Creates a new, un-signaled Token with callbacks.
    pub fn from_client<FS, FF>(
        cli: &AsyncClient,
        req: ServerRequest,
        success_cb: FS,
        failure_cb: FF,
    ) -> Self
    where
        FS: Fn(&AsyncClient, u16) + 'static,
        FF: Fn(&AsyncClient, u16, i32) + 'static,
    {
        Self {
            inner: TokenInner::from_client(cli, req, success_cb, failure_cb),
        }
    }

    /// Creates a new Token signaled with an error code.
    pub fn from_error(rc: i32) -> Self {
        Self {
            inner: TokenInner::from_error(rc),
        }
    }

    /// Creates a new Token signaled with a "success" return code.
    pub fn from_success() -> Self {
        Self {
            inner: TokenInner::from_error(ffi::MQTTASYNC_SUCCESS as i32),
        }
    }

    /// Constructs a Token from a raw pointer to the inner structure.
    /// This is how a token is normally reconstructed from a context
    /// pointer coming back from the C lib.
    pub(crate) unsafe fn from_raw(ptr: *mut c_void) -> Self {
        Self {
            inner: Arc::from_raw(ptr as *mut TokenInner),
        }
    }

    /// Consumes the `Token`, returning the inner wrapped value.
    pub(crate) fn into_raw(self) -> *mut c_void {
        Arc::into_raw(self.inner) as *mut c_void
    }

    /// Blocks the caller until the asynchronous operation completes.
    pub fn wait(self) -> Result<ServerResponse> {
        block_on(self)
    }

    /// Non-blocking check to see if the token is complete.
    ///
    /// Returns `None` if the operation is still in progress, otherwise
    /// returns the result of the operation which can be an error or,
    /// on success, the response from the server.
    pub fn try_wait(&mut self) -> Option<Result<ServerResponse>> {
        self.now_or_never()
    }

    /// Blocks the caller a limited amount of time waiting for the
    /// asynchronous operation to complete.
    // TODO: We probably shouldn't consume the token if it's not complete.
    //      Maybe take '&mut self'?
    pub fn wait_for(self, dur: Duration) -> Result<ServerResponse> {
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

impl Default for Token {
    fn default() -> Self {
        Self::new()
    }
}

impl From<Error> for Token {
    /// Creates a new token that is already signaled with an error.
    fn from(err: Error) -> Self {
        Self {
            inner: Arc::new(TokenInner::from(err)),
        }
    }
}

unsafe impl Send for Token {}

impl Future for Token {
    type Output = Result<ServerResponse>;

    /// Poll the token to see if the request has completed yet.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut data = self.inner.lock.lock().unwrap();

        match data.res.take() {
            Some(res) => Poll::Ready(res),
            None => {
                // Set waker so that the C callback can wake up the current task
                // when the operation has completed.
                data.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

/// A token for a Connect request.
pub type ConnectToken = Token;

/// A token for a Subscribe request.
pub type SubscribeToken = Token;

/// A token for a Subscribe Many request.
pub type SubscribeManyToken = Token;

/// A token for an Unsubscribe request.
pub type UnsubscribeToken = Token;

/// A token for an Unsubscribe Many request.
pub type UnsubscribeManyToken = Token;

/////////////////////////////////////////////////////////////////////////////
// DeliveryToken

/// A `DeliveryToken` is a mechanism for tracking the progress of an
/// asynchronous message publish operation.
#[derive(Clone)]
pub struct DeliveryToken {
    pub(crate) inner: Arc<TokenInner>,
    msg: Message,
}

impl DeliveryToken {
    /// Creates a new, un-signaled delivery Token.
    /// This is a token which tracks delivery of a message.
    pub fn new(msg: Message) -> DeliveryToken {
        DeliveryToken {
            inner: TokenInner::from_message(&msg),
            msg,
        }
    }

    /// Creates a new Token signaled with a return code.
    pub fn from_error(msg: Message, rc: i32) -> DeliveryToken {
        DeliveryToken {
            inner: TokenInner::from_error(rc),
            msg,
        }
    }

    /// Sets the message ID for the token
    pub(crate) fn set_msgid(&self, msg_id: i16) {
        let mut data = self.inner.lock.lock().unwrap();
        data.msg_id = msg_id;
    }

    /// Gets the message associated with the publish token.
    pub fn message(&self) -> &Message {
        &self.msg
    }

    /// Blocks the caller until the asynchronous operation completes.
    pub fn wait(self) -> Result<()> {
        block_on(self)
    }

    /// Blocks the caller a limited amount of time waiting for the
    /// asynchronous operation to complete.
    pub fn wait_for(self, dur: Duration) -> Result<()> {
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

unsafe impl Send for DeliveryToken {}

impl From<DeliveryToken> for Message {
    fn from(v: DeliveryToken) -> Message {
        v.msg
    }
}

impl From<DeliveryToken> for Token {
    /// Converts the delivery token into a Token
    fn from(v: DeliveryToken) -> Token {
        Token { inner: v.inner }
    }
}

impl Future for DeliveryToken {
    type Output = Result<()>;

    /// Poll the token to see if the request has completed yet.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut data = self.inner.lock.lock().unwrap();

        match data.res.take() {
            Some(res) => Poll::Ready(res.map(|_| ())),
            None => {
                // Set waker so that the C callback can wake up the current task
                // when the operation has completed.
                data.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_new() {
        let tok = Token::new();
        let data = tok.inner.lock.lock().unwrap();
        assert!(data.res.is_none());
    }

    #[test]
    fn test_from_message() {
        const MSG_ID: i16 = 42;
        let mut msg = Message::new("hello", "Hi there", 1);
        msg.cmsg.msgid = MSG_ID as i32;

        let tok = DeliveryToken::new(msg);
        let data = tok.inner.lock.lock().unwrap();
        assert!(data.res.is_none());
        assert_eq!(MSG_ID, data.msg_id);
    }

    // Created from an error code, should be complete with the right return code.
    #[test]
    fn test_from_error() {
        const ERR_CODE: i32 = ffi::MQTTASYNC_BAD_QOS;

        let tok = Token::from_error(ERR_CODE);
        let data = tok.inner.lock.lock().unwrap();

        assert!(data.res.is_some());
        assert!(matches!(data.res, Some(Err(Error::BadQos))));
    }

    // Cloned tokens should have the same (inner) raw address.
    #[test]
    fn test_token_clones() {
        let tok1 = Token::new();
        tok1.inner.lock.lock().unwrap().msg_id = 42;

        let tok2 = tok1.clone();

        let p1 = Token::into_raw(tok1);
        let p2 = Token::into_raw(tok2);

        assert_eq!(p1, p2);

        let (tok1, tok2) = unsafe { (Token::from_raw(p1), Token::from_raw(p2)) };

        assert_eq!(42, tok1.inner.lock.lock().unwrap().msg_id);
        assert_eq!(42, tok2.inner.lock.lock().unwrap().msg_id);
    }

    // Determine that a token can be sent across threads and signaled.
    // As long as it compiles, this indicates that Token implements the Send
    // trait.
    // TODO: This would likely deadlock on an error. Consider something that
    // would timeout on error, instead of hanging forever.
    #[test]
    fn test_token_send() {
        let tok = Token::new();
        let tok2 = tok.clone();

        let thr = thread::spawn(move || tok.wait());

        tok2.inner.on_complete(0, 0, None, ptr::null_mut());
        let _ = thr.join().unwrap();
    }

    #[test]
    fn test_try_wait() {
        const ERR_CODE: i32 = ffi::MQTTASYNC_BAD_QOS;
        let mut tok = Token::from_error(ERR_CODE);

        match tok.try_wait() {
            Some(Err(Error::BadQos)) => {}
            Some(Err(err)) => panic!("Wrong error: {}", err),
            Some(Ok(_)) => panic!("Should be an error"),
            None => panic!("Should be complete"),
        }

        // An unsignaled token
        let mut tok = Token::new();

        // If it's not done, we should get None
        if tok.try_wait().is_some() {
            panic!("Should not be complete");
        }

        // Complete the token
        {
            let mut data = tok.inner.lock.lock().unwrap();
            data.res = Some(Err(Error::BadQos));
        }

        // Now it should resolve to Some(Err(...))
        match tok.try_wait() {
            Some(Err(Error::BadQos)) => (),
            Some(Err(err)) => panic!("Wrong error: {}", err),
            Some(Ok(_)) => panic!("Should be an error"),
            None => panic!("Should be complete"),
        }
    }
}
