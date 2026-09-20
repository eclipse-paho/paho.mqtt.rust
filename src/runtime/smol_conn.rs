// paho-mqtt/src/runtime/smol_conn.rs
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

//! Smol-based connection types for the MQTT client.
//!
//! Provides transport implementations of the shared [`Connection`] trait:
//! `TcpConnection`, `UnixConnection`, `TlsConnection` (`smol-tls`), and
//! `WsConnection` (`smol-websocket`). The TLS and WebSocket configuration helpers they use
//! live in `super::tls_config` and `super::ws_util`.
//!
//! TLS support requires the `smol-tls` feature (`futures-rustls`).
//! WebSocket support requires the `smol-websocket` feature (`async-tungstenite`).

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use super::connection::Connection;
#[cfg(unix)]
use smol::net::unix::UnixStream;
use smol::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};

/////////////////////////////////////////////////////////////////////////////

/// A plain TCP transport backed by [`smol::net::TcpStream`].
pub(crate) struct TcpConnection {
    stream: TcpStream,
}

impl TcpConnection {
    /// Opens a TCP connection to `host` on `port`.
    pub(crate) async fn connect(host: &str, port: u16) -> io::Result<Self> {
        let stream = TcpStream::connect((host, port)).await?;
        stream.set_nodelay(true)?;
        Ok(Self { stream })
    }
}

/// Implements `AsyncRead`, `AsyncWrite`, and `Connection` for a newtype struct
/// whose only field is named `stream` and already implements those traits
/// via the `futures-io` traits.
macro_rules! impl_connection {
    ($ty:ty) => {
        impl AsyncRead for $ty {
            fn poll_read(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut [u8],
            ) -> Poll<io::Result<usize>> {
                Pin::new(&mut self.stream).poll_read(cx, buf)
            }
        }

        impl AsyncWrite for $ty {
            fn poll_write(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                Pin::new(&mut self.stream).poll_write(cx, buf)
            }

            fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Pin::new(&mut self.stream).poll_flush(cx)
            }

            fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Pin::new(&mut self.stream).poll_close(cx)
            }
        }

        impl Connection for $ty {}
    };
}

impl_connection!(TcpConnection);

/////////////////////////////////////////////////////////////////////////////

/// A Unix-domain socket transport backed by [`smol::net::unix::UnixStream`].
#[cfg(unix)]
pub(crate) struct UnixConnection {
    stream: UnixStream,
}

#[cfg(unix)]
impl UnixConnection {
    /// Opens a Unix-domain socket connection to `path`.
    pub(crate) async fn connect(path: &str) -> io::Result<Self> {
        let stream = UnixStream::connect(path).await?;
        Ok(Self { stream })
    }
}

#[cfg(unix)]
impl_connection!(UnixConnection);

/////////////////////////////////////////////////////////////////////////////

/// A TLS transport backed by [`futures_rustls`] (requires the `smol-tls` feature).
#[cfg(feature = "smol-tls")]
pub(crate) struct TlsConnection {
    stream: futures_rustls::client::TlsStream<TcpStream>,
}

#[cfg(feature = "smol-tls")]
impl TlsConnection {
    pub(super) fn new(stream: futures_rustls::client::TlsStream<TcpStream>) -> Self {
        Self { stream }
    }
}

#[cfg(feature = "smol-tls")]
impl_connection!(TlsConnection);

/////////////////////////////////////////////////////////////////////////////

/// A WebSocket transport wrapping an [`async_tungstenite::WebSocketStream`].
///
/// Presents a byte-stream interface (`AsyncRead` + `AsyncWrite`) over a
/// WebSocket connection.  Each flushed write becomes one binary WebSocket
/// frame; reads reassemble frames back into a byte stream.  Requires the
/// `smol-websocket` feature.
#[cfg(feature = "smol-websocket")]
pub(super) struct WsConnection<S> {
    stream: async_tungstenite::WebSocketStream<S>,
    /// Overflow bytes from the last received frame not yet consumed by the reader.
    read_buf: Vec<u8>,
    /// Position within `read_buf` of the next unread byte.
    read_pos: usize,
    /// Bytes accumulated via `poll_write`; sent as one binary frame on flush.
    write_buf: Vec<u8>,
    /// Non-`None` while waiting for the sink to be ready before `start_send`.
    pending_msg: Option<Vec<u8>>,
}

#[cfg(feature = "smol-websocket")]
impl<S: AsyncRead + AsyncWrite + Unpin> WsConnection<S> {
    pub(super) fn new(stream: async_tungstenite::WebSocketStream<S>) -> Self {
        Self {
            stream,
            read_buf: Vec::new(),
            read_pos: 0,
            write_buf: Vec::new(),
            pending_msg: None,
        }
    }
}

#[cfg(feature = "smol-websocket")]
impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for WsConnection<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        use async_tungstenite::tungstenite::Message as WsMessage;
        use futures::Stream;

        let this = self.get_mut();

        // Serve buffered bytes from a previous frame first.
        if this.read_pos < this.read_buf.len() {
            let available = &this.read_buf[this.read_pos..];
            let to_copy = buf.len().min(available.len());
            buf[..to_copy].copy_from_slice(&available[..to_copy]);
            this.read_pos += to_copy;
            if this.read_pos >= this.read_buf.len() {
                this.read_buf.clear();
                this.read_pos = 0;
            }
            return Poll::Ready(Ok(to_copy));
        }

        // Poll the WebSocket stream for the next message.
        loop {
            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(Ok(0)),
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)));
                }
                Poll::Ready(Some(Ok(msg))) => match msg {
                    WsMessage::Binary(data) => {
                        if data.is_empty() {
                            continue;
                        }
                        let to_copy = buf.len().min(data.len());
                        buf[..to_copy].copy_from_slice(&data[..to_copy]);
                        if to_copy < data.len() {
                            this.read_buf = data;
                            this.read_pos = to_copy;
                        }
                        return Poll::Ready(Ok(to_copy));
                    }
                    WsMessage::Text(text) => {
                        let data = text.into_bytes();
                        if data.is_empty() {
                            continue;
                        }
                        let to_copy = buf.len().min(data.len());
                        buf[..to_copy].copy_from_slice(&data[..to_copy]);
                        if to_copy < data.len() {
                            this.read_buf = data;
                            this.read_pos = to_copy;
                        }
                        return Poll::Ready(Ok(to_copy));
                    }
                    WsMessage::Close(_) => return Poll::Ready(Ok(0)),
                    // Control frames are handled internally by tungstenite.
                    WsMessage::Ping(_) | WsMessage::Pong(_) | WsMessage::Frame(_) => {
                        continue;
                    }
                },
            }
        }
    }
}

#[cfg(feature = "smol-websocket")]
impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for WsConnection<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().write_buf.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        use async_tungstenite::tungstenite::Message as WsMessage;
        use futures::Sink;

        let this = self.get_mut();

        // Move the accumulated write buffer into `pending_msg` for sending.
        if this.pending_msg.is_none() && !this.write_buf.is_empty() {
            this.pending_msg = Some(std::mem::take(&mut this.write_buf));
        }

        // Drive the pending message through the Sink.
        if let Some(data) = this.pending_msg.take() {
            match Pin::new(&mut this.stream).poll_ready(cx) {
                Poll::Pending => {
                    this.pending_msg = Some(data);
                    return Poll::Pending;
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)));
                }
                Poll::Ready(Ok(())) => {
                    if let Err(e) = Pin::new(&mut this.stream).start_send(WsMessage::Binary(data)) {
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)));
                    }
                }
            }
        }

        // Flush the sink to push the frame onto the wire.
        Pin::new(&mut this.stream)
            .poll_flush(cx)
            .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        use futures::Sink;
        Pin::new(&mut self.get_mut().stream)
            .poll_close(cx)
            .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))
    }
}

#[cfg(feature = "smol-websocket")]
impl<S: AsyncRead + AsyncWrite + Send + Unpin + 'static> Connection for WsConnection<S> {}
/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_connection<C: Connection>() {}

    #[test]
    fn test_tcp_connection_implements_connection() {
        assert_connection::<TcpConnection>();
    }
}
