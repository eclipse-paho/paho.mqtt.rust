// paho-mqtt/src/runtime/smol_rt.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.

/*******************************************************************************
 * Copyright (c) 2024-2026 Frank Pagliughi <fpagliughi@mindspring.com>
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
    io,
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use futures::future::FusedFuture;
use smol::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf, split};
#[cfg(any(feature = "smol-tls", feature = "smol-websocket"))]
use smol::net::TcpStream;

#[cfg(unix)]
use super::smol_conn::UnixConnection;
#[cfg(feature = "smol-tls")]
use super::smol_conn::{TlsConnection, make_tls_config};
#[cfg(feature = "smol-websocket")]
use super::smol_conn::{WsConnection, build_ws_request};
use super::{
    backend::{Backend, Sleep},
    commands::{BufConfig, ConnectParams, ServerAddr},
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

/// Returns the process-wide shared executor, creating it on first call.
pub(crate) fn get_or_create_shared_executor() -> Arc<smol::Executor<'static>> {
    SHARED_EXECUTOR
        .get_or_init(|| {
            let ex = Arc::new(smol::Executor::new());
            let ex2 = ex.clone();
            std::thread::Builder::new()
                .name("mqtt-smol".to_string())
                .spawn(move || smol::block_on(ex2.run(std::future::pending::<()>())))
                .expect("spawn mqtt-smol executor thread");
            ex
        })
        .clone()
}

impl RuntimeHandle {
    /// Spawns the MQTT event loop as a detached task on `executor` and
    /// returns the channel endpoints for communicating with it.
    pub(crate) fn spawn(executor: &smol::Executor<'static>, buf_cfg: BufConfig) -> Self {
        let (this, task) = Self::new::<SmolBackend>(buf_cfg);
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
        }
        else {
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
const READ_CHUNK: usize = 4096;

/// The smol [`Backend`].
pub(crate) struct SmolBackend;

impl Backend for SmolBackend {
    type Reader = ReadHalf<Box<dyn Connection>>;
    type Writer = WriteHalf<Box<dyn Connection>>;
    type Sleep = SmolSleep;

    async fn open(
        addr: &ServerAddr,
        #[allow(unused_variables)] p: &ConnectParams, // only TLS/WS arms use it
    ) -> Result<(Self::Reader, Self::Writer)> {
        let conn: Box<dyn Connection> = match addr {
            ServerAddr::Tcp { host, port } => {
                Box::new(TcpConnection::connect(host, *port).await?)
            }
            #[cfg(feature = "smol-tls")]
            ServerAddr::Tls { host, port } => {
                let tls = p.tls.as_ref().ok_or(Error::MissingTlsOptions)?;
                let config = make_tls_config(tls)?;
                let connector = futures_rustls::TlsConnector::from(config);
                let server_name = rustls::pki_types::ServerName::try_from(host.as_str())
                    .map_err(|_| {
                        Error::GeneralString(format!("invalid TLS server name: {}", host))
                    })?
                    .to_owned();
                let tcp = TcpStream::connect((host.as_str(), *port)).await?;
                tcp.set_nodelay(true)?;
                let tls_stream = connector
                    .connect(server_name, tcp)
                    .await
                    .map_err(|e| Error::Tls(e.to_string()))?;
                Box::new(TlsConnection::new(tls_stream))
            }
            #[cfg(not(feature = "smol-tls"))]
            #[cfg(any(feature = "tls", feature = "openssl"))]
            ServerAddr::Tls { .. } => {
                return Err(Error::General(
                    "TLS is not yet supported with the smol runtime",
                ));
            }
            #[cfg(unix)]
            ServerAddr::Unix { path } => Box::new(UnixConnection::connect(path).await?),
            #[cfg(feature = "smol-websocket")]
            ServerAddr::Ws { host, port, path } => {
                let url = format!("ws://{}:{}{}", host, port, path);
                let request = build_ws_request(&url, &p.http_headers)?;
                let tcp = TcpStream::connect((host.as_str(), *port)).await?;
                tcp.set_nodelay(true)?;
                let (ws, _) = async_tungstenite::client_async(request, tcp)
                    .await
                    .map_err(|e| Error::GeneralString(e.to_string()))?;
                Box::new(WsConnection::new(ws))
            }
            #[cfg(not(feature = "smol-websocket"))]
            #[cfg(feature = "websocket")]
            ServerAddr::Ws { .. } => {
                return Err(Error::General(
                    "WebSocket is not yet supported with the smol runtime",
                ));
            }
            #[cfg(all(feature = "smol-websocket", feature = "smol-tls"))]
            ServerAddr::Wss { host, port, path } => {
                let tls = p.tls.as_ref().ok_or(Error::MissingTlsOptions)?;
                let config = make_tls_config(tls)?;
                let connector = futures_rustls::TlsConnector::from(config);
                let server_name = rustls::pki_types::ServerName::try_from(host.as_str())
                    .map_err(|_| {
                        Error::GeneralString(format!("invalid TLS server name: {}", host))
                    })?
                    .to_owned();
                let tcp = TcpStream::connect((host.as_str(), *port)).await?;
                tcp.set_nodelay(true)?;
                let tls_stream = connector
                    .connect(server_name, tcp)
                    .await
                    .map_err(|e| Error::Tls(e.to_string()))?;
                let url = format!("wss://{}:{}{}", host, port, path);
                let request = build_ws_request(&url, &p.http_headers)?;
                let (ws, _) = async_tungstenite::client_async(request, tls_stream)
                    .await
                    .map_err(|e| Error::GeneralString(e.to_string()))?;
                Box::new(WsConnection::new(ws))
            }
            #[cfg(all(
                feature = "smol-websocket",
                not(feature = "smol-tls"),
                any(feature = "tls", feature = "openssl")
            ))]
            ServerAddr::Wss { .. } => {
                return Err(Error::General(
                    "WebSocket+TLS is not yet supported with the smol runtime",
                ));
            }
        };
        Ok(split(conn))
    }

    /// `futures::io::AsyncRead` only reads into an initialised slice, so
    /// read into a stack buffer and append; `buf` is untouched until the read
    /// completes, which keeps this cancel-safe.
    async fn read_buf(reader: &mut Self::Reader, buf: &mut BytesMut) -> io::Result<usize> {
        let mut tmp = [0u8; READ_CHUNK];
        let n = reader.read(&mut tmp).await?;
        buf.extend_from_slice(&tmp[..n]);
        Ok(n)
    }

    async fn write_all(writer: &mut Self::Writer, data: &[u8]) -> io::Result<()> {
        writer.write_all(data).await
    }

    /// No vectored `write_all` in `futures::io`; two writes, one flush.
    async fn write_frame(
        writer: &mut Self::Writer,
        header: &[u8],
        payload: Bytes,
    ) -> io::Result<()> {
        writer.write_all(header).await?;
        writer.write_all(&payload).await
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
    use crate::runtime::{Command, Event, session::run_event_loop};

    fn assert_send<F: Send + 'static>(_: F) {}

    /// The event loop must be `Send` to run on the shared executor thread.
    #[test]
    fn test_run_event_loop_is_send() {
        let (_, cmd_rx) = async_channel::unbounded::<Command>();
        let (event_tx, _) = async_channel::unbounded::<Event>();
        assert_send(run_event_loop::<SmolBackend>(
            cmd_rx,
            event_tx,
            BufConfig {
                send_while_disconnected: false,
                max_buffered_msgs: 100,
                allow_send_any_time: false,
                delete_oldest: false,
                restore_msgs: false,
                persist_qos0: true,
            },
        ));
    }
}
