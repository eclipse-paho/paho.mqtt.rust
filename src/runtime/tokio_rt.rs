// paho-mqtt/src/runtime/tokio_rt.rs
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
//! Tokio backend for the MQTT event loop.
//!
//! Supplies the runtime-specific pieces the shared [`super::session`] module
//! is generic over — the [`Backend`] implementation (transport open, read,
//! write, timer) — plus Tokio runtime selection and ownership.  Transport
//! types live in [`super::tokio_conn`].

use std::{
    future::Future,
    io::{self, IoSlice},
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll},
    time::Duration,
};

use bytes::BytesMut;
use futures::future::FusedFuture;
#[cfg(any(feature = "tls", feature = "openssl", feature = "websocket"))]
use tokio::net::TcpStream;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    time,
};
#[cfg(feature = "tls")]
use tokio_rustls::TlsConnector;

#[cfg(feature = "tls")]
use super::tls_config::make_tls_config;
#[cfg(all(feature = "openssl", not(feature = "tls")))]
use super::tls_config::{make_openssl_config, openssl_ssl_for};
#[cfg(all(feature = "openssl", not(feature = "tls")))]
use super::tokio_conn::OpenSslConnection;
#[cfg(feature = "tls")]
use super::tokio_conn::TlsConnection;
#[cfg(unix)]
use super::tokio_conn::UnixConnection;
#[cfg(feature = "websocket")]
use super::tokio_conn::WsConnection;
#[cfg(feature = "websocket")]
use super::ws_util::build_ws_request;
use super::{
    backend::{Backend, READ_CHUNK, Sleep},
    commands::{ConnectParams, OfflineConfig, ServerAddr},
    connection::Connection,
    session::RuntimeHandle,
    tokio_conn::TcpConnection,
};
#[cfg(any(feature = "tls", feature = "openssl", feature = "websocket"))]
use crate::Error;
use crate::Result;

/////////////////////////////////////////////////////////////////////////////

/// Tracks whether this client owns the Tokio runtime or merely borrows it.
///
/// Stored in [`InnerAsyncClient`] to keep the owned runtime alive until the
/// last client using it is dropped.
///
/// [`InnerAsyncClient`]: crate::async_client::InnerAsyncClient
pub(crate) enum RuntimeOwnership {
    /// The library created this runtime; the `Arc` keeps it alive until the
    /// last client that shares it is dropped.
    #[allow(dead_code)] // held purely for its Drop effect
    Owned(Arc<LibRuntime>),
    /// The application owns the runtime; the library only holds a `Handle`.
    Borrowed,
}

/// A runtime the library created, which shuts down safely wherever its last
/// owner is dropped.
///
/// Dropping a tokio [`Runtime`](tokio::runtime::Runtime) blocks while its
/// worker threads stop, and tokio panics if that happens inside an async
/// context. The last client to hold this runtime may well be dropped from
/// async code — and a [`Client`](crate::Client) made there always uses it —
/// so in that case the runtime is shut down in the background instead.
/// Anywhere else it is dropped as usual, which waits for its threads.
pub(crate) struct LibRuntime(Option<tokio::runtime::Runtime>);

impl LibRuntime {
    /// A handle to the runtime.
    fn handle(&self) -> &tokio::runtime::Handle {
        self.0
            .as_ref()
            .expect("runtime present until drop")
            .handle()
    }

    /// The number of worker threads.
    fn num_workers(&self) -> usize {
        self.0
            .as_ref()
            .expect("runtime present until drop")
            .metrics()
            .num_workers()
    }
}

impl Drop for LibRuntime {
    fn drop(&mut self) {
        if let Some(rt) = self.0.take()
            && tokio::runtime::Handle::try_current().is_ok()
        {
            rt.shutdown_background();
        }
    }
}

/// Process-wide shared runtime registry.
///
/// Holds a `Weak` reference so the runtime is dropped automatically when
/// the last owned client is dropped.
static SHARED_RT: Mutex<Option<Weak<LibRuntime>>> = Mutex::new(None);

/// Returns a `Handle` to the appropriate runtime and the corresponding
/// ownership record, based on `mode`.
pub(crate) fn resolve_runtime(
    mode: &crate::create_options::RuntimeMode,
) -> (tokio::runtime::Handle, RuntimeOwnership) {
    use crate::create_options::RuntimeMode;
    match mode {
        RuntimeMode::Auto => {
            if let Ok(h) = tokio::runtime::Handle::try_current() {
                return (h, RuntimeOwnership::Borrowed);
            }
            let rt = get_or_create_shared_runtime(0);
            let h = rt.handle().clone();
            (h, RuntimeOwnership::Owned(rt))
        }
        RuntimeMode::Shared(n) => {
            let rt = get_or_create_shared_runtime(*n);
            let h = rt.handle().clone();
            (h, RuntimeOwnership::Owned(rt))
        }
        RuntimeMode::ApplicationRuntime => {
            let h = tokio::runtime::Handle::try_current()
                .expect("RuntimeMode::ApplicationRuntime requires a current Tokio runtime");
            (h, RuntimeOwnership::Borrowed)
        }
    }
}

/// Returns the process-wide shared runtime, creating it if necessary.
///
/// `num_threads` is only honoured on first creation. If the runtime already
/// exists and the requested thread count differs, a warning is logged and
/// the existing runtime is returned unchanged.
fn get_or_create_shared_runtime(num_threads: usize) -> Arc<LibRuntime> {
    let mut guard = SHARED_RT.lock().unwrap();
    if let Some(weak) = guard.as_ref()
        && let Some(rt) = weak.upgrade()
    {
        let existing = rt.num_workers();
        let requested = num_threads.max(1);
        if requested != existing {
            warn!(
                "Ignoring num_threads={} — shared runtime already has {} worker(s)",
                num_threads, existing
            );
        }
        return rt;
    }
    let rt = Arc::new(LibRuntime(Some(build_runtime(num_threads))));
    *guard = Some(Arc::downgrade(&rt));
    rt
}

/// How often, in task polls, a busy worker of the shared runtime checks the
/// I/O driver for socket readiness. Tokio's default is 61.
///
/// A worker otherwise checks only when it runs out of tasks. With one worker
/// (the default) and a client under load, the publishing threads keep
/// waking the event loop, so the worker is never idle, and readiness for
/// inbound packets such as PUBACKs goes unseen for milliseconds at a time.
/// At 100K msg/s that put the 99th-percentile publish latency at 20–30 ms;
/// with 2 it is 2–4 ms. Each check is one non-blocking `epoll_wait`, made
/// only while the worker is busy, so an idle client pays nothing.
/// (A second worker does as well or slightly better, by sitting in the
/// driver, but costs a thread and about 10% more CPU per message.)
const SHARED_EVENT_INTERVAL: u32 = 2;

fn build_runtime(num_threads: usize) -> tokio::runtime::Runtime {
    // Always use multi_thread: spawned tasks need background worker threads
    // to drive them. current_thread only runs tasks when block_on is called,
    // which does not apply to the library-owned shared runtime.
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_threads.max(1))
        .event_interval(SHARED_EVENT_INTERVAL)
        .enable_all()
        .build()
        .expect("build tokio runtime")
}

impl RuntimeHandle {
    /// Spawns the MQTT event loop as a task on `handle` and returns the
    /// channel endpoints for communicating with it.
    pub(crate) fn spawn(handle: &tokio::runtime::Handle, offline_cfg: OfflineConfig) -> Self {
        let (this, task) = Self::new::<TokioBackend>(offline_cfg);
        handle.spawn(task);
        this
    }
}

/////////////////////////////////////////////////////////////////////////////
// Backend

/// Resettable timer over [`tokio::time::Sleep`].
///
/// `Sleep` is `!Unpin`, so it is boxed once here; resets reuse the same
/// allocation.
pub(crate) struct TokioSleep {
    sleep: Pin<Box<time::Sleep>>,
    /// `false` for a timer created with [`Sleep::never`].
    enabled: bool,
}

impl Future for TokioSleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.enabled {
            self.sleep.as_mut().poll(cx)
        } else {
            Poll::Pending
        }
    }
}

impl FusedFuture for TokioSleep {
    fn is_terminated(&self) -> bool {
        !self.enabled
    }
}

impl Sleep for TokioSleep {
    fn new(dur: Duration) -> Self {
        Self {
            sleep: Box::pin(time::sleep(dur)),
            enabled: true,
        }
    }

    fn never() -> Self {
        Self {
            sleep: Box::pin(time::sleep(Duration::ZERO)),
            enabled: false,
        }
    }

    fn reset(&mut self, dur: Duration) {
        self.sleep.as_mut().reset(time::Instant::now() + dur);
    }
}

/// Opens a TCP connection to `host:port`, tunnelling through `proxy_uri` if
/// one is configured.
///
/// See [`super::proxy`]; the tunnel is transparent once established, so a
/// TLS handshake or WebSocket upgrade runs over it unchanged.
#[cfg(feature = "websocket")]
async fn tcp_maybe_proxied(proxy_uri: &str, host: &str, port: u16) -> Result<TcpStream> {
    use super::proxy;

    if proxy_uri.is_empty() {
        let tcp = TcpStream::connect((host, port)).await?;
        tcp.set_nodelay(true)?;
        return Ok(tcp);
    }

    let proxy = proxy::parse(proxy_uri)?;
    let mut tcp = TcpStream::connect((proxy.host.as_str(), proxy.port)).await?;
    tcp.set_nodelay(true)?;
    tcp.write_all(&proxy::connect_request(&proxy, host, port))
        .await?;
    tcp.flush().await?;

    let mut buf = Vec::with_capacity(256);
    let mut tmp = [0u8; 512];
    loop {
        let n = tcp.read(&mut tmp).await?;
        if n == 0 {
            return Err(Error::Proxy("the proxy closed the connection".into()));
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(end) = proxy::check_response(&buf)? {
            if end != buf.len() {
                // Nothing should arrive before we have spoken, and the extra
                // bytes cannot be pushed back into the socket.
                return Err(Error::Proxy(
                    "data arrived before the tunnel was open".into(),
                ));
            }
            return Ok(tcp);
        }
    }
}

/// The Tokio [`Backend`].
pub(crate) struct TokioBackend;

/// Below this much spare room, the buffer is grown before the next read.
///
/// Only this backend needs it: tokio reads into the buffer's spare
/// capacity, so the room left in it *is* the read size. The smol backend
/// reads into a buffer of its own and always offers a full
/// [`READ_CHUNK`](super::backend::READ_CHUNK).
const READ_LOW_WATER: usize = 4 * 1024;

/// Makes sure the next read has room to be worth making.
///
/// Left alone, the buffer settles at whatever is spare after the packets
/// read so far were taken out of it, and a burst then costs a syscall every
/// few packets. Growing it when it runs low keeps reads at
/// [`READ_CHUNK`](super::backend::READ_CHUNK) without reallocating on every
/// one.
fn make_room(buf: &mut BytesMut) {
    if buf.capacity() - buf.len() < READ_LOW_WATER {
        buf.reserve(READ_CHUNK);
    }
}

impl Backend for TokioBackend {
    type Reader = ReadHalf<Box<dyn Connection>>;
    type Writer = WriteHalf<Box<dyn Connection>>;
    type Sleep = TokioSleep;

    async fn open(
        addr: &ServerAddr,
        #[allow(unused_variables)] p: &ConnectParams, // only TLS/WS arms use it
    ) -> Result<(Self::Reader, Self::Writer)> {
        let conn: Box<dyn Connection> = match addr {
            ServerAddr::Tcp { host, port } => Box::new(TcpConnection::connect(host, *port).await?),
            #[cfg(feature = "tls")]
            ServerAddr::Tls { host, port } => {
                let tls = p.tls.as_ref().ok_or(Error::MissingTlsOptions)?;
                let config = make_tls_config(tls)?;
                let connector = TlsConnector::from(config);
                let server_name = rustls::pki_types::ServerName::try_from(host.as_str())
                    .map_err(|_| Error::Tls(format!("invalid TLS server name: {}", host)))?
                    .to_owned();
                let tcp = TcpStream::connect((host.as_str(), *port)).await?;
                tcp.set_nodelay(true)?;
                let tls = connector
                    .connect(server_name, tcp)
                    .await
                    .map_err(|e| Error::Tls(e.to_string()))?;
                Box::new(TlsConnection::new(tls))
            }
            #[cfg(all(not(feature = "tls"), feature = "openssl"))]
            ServerAddr::Tls { host, port } => {
                let tls_opts = p.tls.as_ref().ok_or(Error::MissingTlsOptions)?;
                let connector = make_openssl_config(tls_opts)?;
                let ssl = openssl_ssl_for(&connector, tls_opts, host.as_str())?;
                let tcp = TcpStream::connect((host.as_str(), *port)).await?;
                tcp.set_nodelay(true)?;
                let mut stream = tokio_openssl::SslStream::new(ssl, tcp)
                    .map_err(|e| Error::Tls(e.to_string()))?;
                Pin::new(&mut stream)
                    .connect()
                    .await
                    .map_err(|e| Error::Tls(e.to_string()))?;
                Box::new(OpenSslConnection::new(stream))
            }
            #[cfg(unix)]
            ServerAddr::Unix { path } => Box::new(UnixConnection::connect(path).await?),
            #[cfg(feature = "websocket")]
            ServerAddr::Ws { host, port, path } => {
                let url = format!("ws://{}:{}{}", host, port, path);
                let request = build_ws_request(&url, &p.http_headers)?;
                let tcp = tcp_maybe_proxied(&p.http_proxy, host.as_str(), *port).await?;
                let (ws, _) = tokio_tungstenite::client_async(request, tcp)
                    .await
                    .map_err(|e| Error::WebSocket(e.to_string()))?;
                Box::new(WsConnection::new(ws))
            }
            #[cfg(all(feature = "websocket", feature = "tls"))]
            ServerAddr::Wss { host, port, path } => {
                let tls = p.tls.as_ref().ok_or(Error::MissingTlsOptions)?;
                let config = make_tls_config(tls)?;
                let connector = TlsConnector::from(config);
                let server_name = rustls::pki_types::ServerName::try_from(host.as_str())
                    .map_err(|_| Error::Tls(format!("invalid TLS server name: {}", host)))?
                    .to_owned();
                let tcp = tcp_maybe_proxied(&p.https_proxy, host.as_str(), *port).await?;
                let tls = connector
                    .connect(server_name, tcp)
                    .await
                    .map_err(|e| Error::Tls(e.to_string()))?;
                let url = format!("wss://{}:{}{}", host, port, path);
                let request = build_ws_request(&url, &p.http_headers)?;
                let (ws, _) = tokio_tungstenite::client_async(request, tls)
                    .await
                    .map_err(|e| Error::WebSocket(e.to_string()))?;
                Box::new(WsConnection::new(ws))
            }
            #[cfg(all(feature = "websocket", not(feature = "tls"), feature = "openssl"))]
            ServerAddr::Wss { host, port, path } => {
                let tls_opts = p.tls.as_ref().ok_or(Error::MissingTlsOptions)?;
                let connector = make_openssl_config(tls_opts)?;
                let ssl = openssl_ssl_for(&connector, tls_opts, host.as_str())?;
                let tcp = tcp_maybe_proxied(&p.https_proxy, host.as_str(), *port).await?;
                let mut stream = tokio_openssl::SslStream::new(ssl, tcp)
                    .map_err(|e| Error::Tls(e.to_string()))?;
                Pin::new(&mut stream)
                    .connect()
                    .await
                    .map_err(|e| Error::Tls(e.to_string()))?;
                let url = format!("wss://{}:{}{}", host, port, path);
                let request = build_ws_request(&url, &p.http_headers)?;
                let (ws, _) = tokio_tungstenite::client_async(request, stream)
                    .await
                    .map_err(|e| Error::WebSocket(e.to_string()))?;
                Box::new(WsConnection::new(ws))
            }
        };
        Ok(tokio::io::split(conn))
    }

    /// `tokio::io::read_buf` appends into the spare capacity of `buf`,
    /// growing it when full, and is cancel-safe.
    /// Tokio reads into the buffer's spare capacity, so the room left in it
    /// *is* the read size; see [`make_room`].
    async fn read_buf(reader: &mut Self::Reader, buf: &mut BytesMut) -> io::Result<usize> {
        make_room(buf);
        reader.read_buf(buf).await
    }

    async fn write_all(writer: &mut Self::Writer, data: &[u8]) -> io::Result<()> {
        writer.write_all(data).await
    }

    /// One `poll_write_vectored`, which tokio documents as cancel-safe.
    async fn write_vectored(writer: &mut Self::Writer, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        writer.write_vectored(bufs).await
    }

    async fn flush(writer: &mut Self::Writer) -> io::Result<()> {
        writer.flush().await
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{
        Command, Event,
        commands::StateCell,
        session::{Dispatch, run_event_loop},
    };

    fn assert_send<F: Send + 'static>(_: F) {}

    /// A buffer with little room left is grown to a full chunk; one with
    /// room to spare is left alone, so a read does not reallocate.
    #[test]
    fn test_make_room_grows_only_when_low() {
        let mut buf = BytesMut::with_capacity(READ_CHUNK);
        let (cap, ptr) = (buf.capacity(), buf.as_ptr());
        make_room(&mut buf);
        assert_eq!(buf.capacity(), cap, "untouched while there is room");
        assert_eq!(buf.as_ptr(), ptr, "and not reallocated");

        // Nearly full: what is left would make a small read.
        buf.resize(READ_CHUNK - 1024, 0);
        make_room(&mut buf);
        assert!(
            buf.capacity() - buf.len() >= READ_CHUNK,
            "a full chunk of room: {} spare",
            buf.capacity() - buf.len()
        );
    }

    /// The last owner of a library runtime may be dropped inside async code,
    /// where dropping a plain `Runtime` panics. Fails against an
    /// `Arc<Runtime>` with "Cannot drop a runtime in a context where
    /// blocking is not allowed".
    #[test]
    fn test_lib_runtime_drops_inside_async_context() {
        let app = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let lib = Arc::new(LibRuntime(Some(build_runtime(1))));
        let task = lib.handle().spawn(std::future::pending::<()>());
        app.block_on(async move {
            drop(lib);
        });
        // The background shutdown cancels what was still running.
        let res = app.block_on(task);
        assert!(res.unwrap_err().is_cancelled());
    }

    /// Outside async code the runtime is dropped as usual.
    #[test]
    fn test_lib_runtime_drops_outside_async_context() {
        let lib = Arc::new(LibRuntime(Some(build_runtime(1))));
        let n = lib.handle().block_on(async { 1 + 1 });
        assert_eq!(n, 2);
        drop(lib);
    }

    /// The event loop must be `Send` to run on a multi-threaded runtime.
    #[test]
    fn test_run_event_loop_is_send() {
        let (_, cmd_rx) = async_channel::unbounded::<Command>();
        let (event_tx, _) = async_channel::unbounded::<Event>();
        assert_send(run_event_loop::<TokioBackend>(
            cmd_rx,
            event_tx,
            Dispatch::new(),
            OfflineConfig {
                send_while_disconnected: false,
                max_buffered_msgs: 100,
                allow_send_any_time: false,
                delete_oldest: false,
                restore_msgs: false,
                persist_qos0: true,
                persist_timeout: std::time::Duration::from_secs(30),
            },
            StateCell::new(),
        ));
    }
}
