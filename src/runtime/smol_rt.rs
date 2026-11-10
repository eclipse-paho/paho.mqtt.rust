// paho-mqtt/src/runtime/smol_rt.rs
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
//! Smol backend for the MQTT event loop.
//!
//! Supplies the runtime-specific pieces the shared [`super::session`] module
//! is generic over — the [`Backend`] implementation (transport open, read,
//! write, timer) — plus the process-wide smol executor.  Transport types
//! live in [`super::smol_conn`].
//!
//! TLS is supported via the `smol-tls` feature (uses `futures-rustls`).
//! WebSocket support requires the `smol-websocket` feature (`async-tungstenite`).

use std::{
    future::Future,
    io::{self, IoSlice},
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
    time::Duration,
};

use bytes::BytesMut;
use futures::{
    future::FusedFuture,
    io::{ReadHalf, WriteHalf},
};
use smol::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(any(feature = "smol-tls", feature = "smol-websocket"))]
use smol::net::TcpStream;

#[cfg(feature = "smol-tls")]
use super::smol_conn::TlsConnection;
#[cfg(unix)]
use super::smol_conn::UnixConnection;
#[cfg(feature = "smol-websocket")]
use super::smol_conn::WsConnection;
#[cfg(feature = "smol-tls")]
use super::tls_config::{invalid_server_name, make_tls_config, rustls_handshake_err};
#[cfg(feature = "smol-websocket")]
use super::ws_util::build_ws_request;
use super::{
    backend::{Backend, READ_CHUNK, Sleep},
    commands::{ConnectParams, OfflineConfig, ServerAddr},
    connection::Connection,
    session::RuntimeHandle,
    smol_conn::TcpConnection,
};
#[cfg(any(
    feature = "smol-tls",
    feature = "smol-websocket",
    feature = "tls",
    feature = "openssl",
    feature = "websocket"
))]
use crate::Error;
use crate::Result;

/////////////////////////////////////////////////////////////////////////////
// Runtime management

/// Tracks whether this client owns the smol executor or merely uses it.
///
/// Stored in [`InnerAsyncClient`] to keep the executor alive.
///
/// [`InnerAsyncClient`]: crate::async_client::InnerAsyncClient
pub(crate) enum SmolOwnership {
    /// The library manages the shared executor; the `Arc` keeps it alive.
    #[allow(dead_code)] // held purely for its drop effect
    Shared(Arc<smol::Executor<'static>>),
}

/// Process-wide shared smol executor.
///
/// Created on first use and kept alive for the process lifetime.  A
/// background OS thread drives the executor by running
/// `executor.run(future::pending())` indefinitely.
static SHARED_EXECUTOR: OnceLock<Arc<smol::Executor<'static>>> = OnceLock::new();

/// How many threads were started to run [`SHARED_EXECUTOR`].
static SHARED_THREADS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Returns the executor to run this client's event loop on, honouring `mode`.
///
/// smol has no ambient runtime to discover or borrow — an executor is just
/// an object, and someone has to run it — so
/// [`Auto`](crate::RuntimeMode::Auto) and
/// [`Shared(n)`](crate::RuntimeMode::Shared) both mean the process-wide
/// executor, with `n` deciding how many threads drive it.
///
/// # Errors
///
/// [`Error::InvalidArgument`](crate::Error) for
/// [`ApplicationRuntime`](crate::RuntimeMode::ApplicationRuntime), which
/// cannot be honoured here: there is no current executor to attach to, as
/// the tokio backend reports when that mode is chosen with no runtime
/// running. [`Error::Io`](crate::Error) if not one thread can be started to
/// run the executor.
pub(crate) fn resolve_executor(
    mode: &crate::create_options::RuntimeMode,
) -> crate::Result<Arc<smol::Executor<'static>>> {
    use crate::create_options::RuntimeMode;

    let threads = match mode {
        RuntimeMode::Auto => 1,
        RuntimeMode::Shared(n) => (*n).max(1),
        RuntimeMode::ApplicationRuntime => {
            return Err(crate::Error::InvalidArgument(
                "RuntimeMode::ApplicationRuntime is not supported by the smol backend: \
                 smol has no ambient executor to borrow. Use RuntimeMode::Auto or Shared(n)."
                    .into(),
            ));
        }
    };

    if let Some(ex) = SHARED_EXECUTOR.get() {
        // As with the tokio backend, the thread count is honoured only by
        // whichever client creates the executor.
        let existing = SHARED_THREADS.load(std::sync::atomic::Ordering::Relaxed);
        if matches!(mode, RuntimeMode::Shared(_)) && threads != existing {
            warn!(
                "Ignoring num_threads={threads}: the shared smol executor already has {existing}"
            );
        }
        return Ok(ex.clone());
    }

    // Creation can fail, which `OnceLock::get_or_init` cannot report, so it
    // is serialised here instead: two clients created at once must not
    // each start threads for an executor of their own.
    static CREATING: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _creating = CREATING.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(ex) = SHARED_EXECUTOR.get() {
        return Ok(ex.clone());
    }

    let ex = Arc::new(smol::Executor::new());
    let mut started = 0;
    for i in 0..threads {
        let runner = ex.clone();
        let res = std::thread::Builder::new()
            .name(format!("mqtt-smol-{i}"))
            .spawn(move || smol::block_on(runner.run(std::future::pending::<()>())));
        match res {
            Ok(_) => started += 1,
            // With no thread at all nothing would run the event loop.
            Err(e) if started == 0 => return Err(e.into()),
            // With some, it runs, if more slowly than asked for.
            Err(e) => {
                warn!("Started {started} of {threads} smol executor threads: {e}");
                break;
            }
        }
    }
    SHARED_THREADS.store(started, std::sync::atomic::Ordering::Relaxed);
    let _ = SHARED_EXECUTOR.set(ex.clone());
    Ok(ex)
}

impl RuntimeHandle {
    /// Spawns the MQTT event loop as a detached task on `executor` and
    /// returns the channel endpoints for communicating with it.
    pub(crate) fn spawn(executor: &smol::Executor<'static>, offline_cfg: OfflineConfig) -> Self {
        let (this, task) = Self::new::<SmolBackend>(offline_cfg);
        executor.spawn(task).detach();
        this
    }
}

/////////////////////////////////////////////////////////////////////////////
// Backend

/// Resettable timer over [`smol::Timer`].
pub(crate) struct SmolSleep {
    timer: smol::Timer,
    /// `false` for a timer created with [`Sleep::never`].
    enabled: bool,
}

impl Future for SmolSleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.enabled {
            Pin::new(&mut self.timer).poll(cx).map(|_| ())
        } else {
            Poll::Pending
        }
    }
}

impl FusedFuture for SmolSleep {
    fn is_terminated(&self) -> bool {
        !self.enabled
    }
}

impl Sleep for SmolSleep {
    fn new(dur: Duration) -> Self {
        Self {
            timer: smol::Timer::after(dur),
            enabled: true,
        }
    }

    fn never() -> Self {
        Self {
            timer: smol::Timer::never(),
            enabled: false,
        }
    }

    fn reset(&mut self, dur: Duration) {
        self.timer.set_after(dur);
    }
}

/// Bytes read from the transport per `read_buf` call.
/// The read half of a connection, with the buffer its reads land in.
///
/// `futures::io::AsyncRead` reads into an initialized `&mut [u8]`, so this
/// backend cannot read straight into `BytesMut`'s spare capacity the way
/// tokio's `ReadBuf` can: there has to be a buffer in between. Keeping one
/// per connection, allocated when it is opened, means a read costs one copy
/// and nothing else — a fresh array would have to be zeroed every time,
/// which at [`READ_CHUNK`] bytes would be felt on the single-message path.
pub(crate) struct SmolReader {
    half: ReadHalf<Box<dyn Connection>>,
    scratch: Vec<u8>,
}

impl SmolReader {
    fn new(half: ReadHalf<Box<dyn Connection>>) -> Self {
        Self {
            half,
            scratch: vec![0u8; READ_CHUNK],
        }
    }
}

/// Opens a TCP connection to `host:port`, tunnelling through `proxy_uri` if
/// one is configured.
///
/// See [`super::proxy`]; the tunnel is transparent once established, so a
/// TLS handshake or WebSocket upgrade runs over it unchanged.
#[cfg(feature = "smol-websocket")]
async fn tcp_maybe_proxied(proxy_uri: &str, host: &str, port: u16) -> Result<TcpStream> {
    use super::proxy;

    if proxy_uri.is_empty() {
        let tcp = TcpStream::connect((host, port)).await?;
        tcp.set_nodelay(true)?;
        return Ok(tcp);
    }

    let proxy = proxy::Proxy::parse(proxy_uri)?;
    let mut tcp = TcpStream::connect((proxy.host.as_str(), proxy.port)).await?;
    tcp.set_nodelay(true)?;
    tcp.write_all(&proxy.connect_request(host, port)).await?;
    tcp.flush().await?;

    let mut buf = Vec::with_capacity(256);
    let mut tmp = [0u8; 512];
    loop {
        let n = tcp.read(&mut tmp).await?;
        if n == 0 {
            return Err(crate::ProxyError::Closed.into());
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(end) = proxy::check_response(&buf)? {
            if end != buf.len() {
                // Nothing should arrive before we have spoken, and the extra
                // bytes cannot be pushed back into the socket.
                return Err(crate::ProxyError::EarlyData.into());
            }
            return Ok(tcp);
        }
    }
}

/// The smol [`Backend`].
pub(crate) struct SmolBackend;

impl Backend for SmolBackend {
    type Reader = SmolReader;
    type Writer = WriteHalf<Box<dyn Connection>>;
    type Sleep = SmolSleep;

    async fn open(
        addr: &ServerAddr,
        #[allow(unused_variables)] p: &ConnectParams, // only TLS/WS arms use it
    ) -> Result<(Self::Reader, Self::Writer)> {
        let conn: Box<dyn Connection> = match addr {
            ServerAddr::Tcp { host, port } => Box::new(TcpConnection::connect(host, *port).await?),
            #[cfg(feature = "smol-tls")]
            ServerAddr::Tls { host, port } => {
                let tls = p.tls.as_ref().ok_or(Error::MissingTlsOptions)?;
                let config = make_tls_config(tls)?;
                let connector = futures_rustls::TlsConnector::from(config);
                let server_name = rustls::pki_types::ServerName::try_from(host.as_str())
                    .map_err(|_| invalid_server_name(host))?
                    .to_owned();
                let tcp = TcpStream::connect((host.as_str(), *port)).await?;
                tcp.set_nodelay(true)?;
                let tls_stream = connector
                    .connect(server_name, tcp)
                    .await
                    .map_err(rustls_handshake_err)?;
                Box::new(TlsConnection::new(tls_stream))
            }
            #[cfg(unix)]
            ServerAddr::Unix { path } => Box::new(UnixConnection::connect(path).await?),
            #[cfg(feature = "smol-websocket")]
            ServerAddr::Ws { host, port, path } => {
                let url = format!("ws://{}{}", super::commands::host_port(host, *port), path);
                let request = build_ws_request(&url, &p.http_headers)?;
                let tcp = tcp_maybe_proxied(&p.http_proxy, host.as_str(), *port).await?;
                let (ws, _) = async_tungstenite::client_async(request, tcp)
                    .await
                    .map_err(|e| Error::WebSocket(e.to_string()))?;
                Box::new(WsConnection::new(ws))
            }
            #[cfg(all(feature = "smol-websocket", feature = "smol-tls"))]
            ServerAddr::Wss { host, port, path } => {
                let tls = p.tls.as_ref().ok_or(Error::MissingTlsOptions)?;
                let config = make_tls_config(tls)?;
                let connector = futures_rustls::TlsConnector::from(config);
                let server_name = rustls::pki_types::ServerName::try_from(host.as_str())
                    .map_err(|_| invalid_server_name(host))?
                    .to_owned();
                let tcp = tcp_maybe_proxied(&p.https_proxy, host.as_str(), *port).await?;
                let tls_stream = connector
                    .connect(server_name, tcp)
                    .await
                    .map_err(rustls_handshake_err)?;
                let url = format!("wss://{}{}", super::commands::host_port(host, *port), path);
                let request = build_ws_request(&url, &p.http_headers)?;
                let (ws, _) = async_tungstenite::client_async(request, tls_stream)
                    .await
                    .map_err(|e| Error::WebSocket(e.to_string()))?;
                Box::new(WsConnection::new(ws))
            }
        };
        // Split with `futures::io` rather than `smol::io`: the latter's write
        // half does not forward `poll_write_vectored`, so every vectored
        // write would send only its first slice.
        let (half, writer) = futures::io::AsyncReadExt::split(conn);
        Ok((SmolReader::new(half), writer))
    }

    /// `futures::io::AsyncRead` only reads into an initialised slice, so
    /// read into a stack buffer and append; `buf` is untouched until the read
    /// completes, which keeps this cancel-safe.
    async fn read_buf(reader: &mut Self::Reader, buf: &mut BytesMut) -> io::Result<usize> {
        let SmolReader { half, scratch } = reader;
        let n = half.read(scratch).await?;
        buf.extend_from_slice(&scratch[..n]);
        Ok(n)
    }

    async fn write_all(writer: &mut Self::Writer, data: &[u8]) -> io::Result<()> {
        writer.write_all(data).await
    }

    /// One `poll_write_vectored`, so cancel-safe.
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

    /// The event loop must be `Send` to run on the shared executor thread.
    #[test]
    fn test_run_event_loop_is_send() {
        let (_, cmd_rx) = async_channel::unbounded::<Command>();
        let (event_tx, _) = async_channel::unbounded::<Event>();
        assert_send(run_event_loop::<SmolBackend>(
            cmd_rx,
            event_tx,
            Dispatch::new(),
            OfflineConfig {
                send_while_disconnected: false,
                max_buffered_msgs: 100,
                max_held_qos0_msgs: 1024,
                max_held_qos0_bytes: 16 * 1024 * 1024,
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
