// paho-mqtt/src/runtime/tokio_conn.rs
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

//! Tokio-based connection types for the MQTT client.
//!
//! Provides transport implementations of the shared [`Connection`] trait:
//! `TcpConnection`, `UnixConnection`, `TlsConnection` (`tls`),
//! `OpenSslConnection` (`openssl`), and `WsConnection` (`websocket`). The TLS
//! and WebSocket configuration helpers they use live in `super::tls_config` and
//! `super::ws_util`.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};
#[cfg(feature = "websocket")]
use {
    futures::{Sink, Stream},
    tokio_tungstenite::{WebSocketStream, tungstenite::Message as WsMessage},
};

use super::connection::Connection;

/////////////////////////////////////////////////////////////////////////////

/// A plain TCP transport backed by [`tokio::net::TcpStream`].
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
/// whose only field is named `stream` and already implements those traits.
macro_rules! impl_connection {
    ($ty:ty) => {
        impl AsyncRead for $ty {
            fn poll_read(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
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

            fn poll_write_vectored(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                bufs: &[io::IoSlice<'_>],
            ) -> Poll<io::Result<usize>> {
                Pin::new(&mut self.stream).poll_write_vectored(cx, bufs)
            }

            fn is_write_vectored(&self) -> bool {
                self.stream.is_write_vectored()
            }

            fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Pin::new(&mut self.stream).poll_flush(cx)
            }

            fn poll_shutdown(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<io::Result<()>> {
                Pin::new(&mut self.stream).poll_shutdown(cx)
            }
        }

        impl Connection for $ty {}
    };
}

impl_connection!(TcpConnection);

/////////////////////////////////////////////////////////////////////////////

/// A Unix-domain socket transport backed by [`tokio::net::UnixStream`].
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

/// A TLS transport backed by [`tokio_rustls`] (requires the `tls` feature).
#[cfg(feature = "tls")]
pub(crate) struct TlsConnection {
    stream: tokio_rustls::client::TlsStream<TcpStream>,
}

#[cfg(feature = "tls")]
impl TlsConnection {
    pub(super) fn new(stream: tokio_rustls::client::TlsStream<TcpStream>) -> Self {
        Self { stream }
    }
}

#[cfg(feature = "tls")]
impl_connection!(TlsConnection);

/////////////////////////////////////////////////////////////////////////////

/// An OpenSSL TLS transport (requires the `openssl` feature).
#[cfg(all(feature = "openssl", not(feature = "tls")))]
pub(crate) struct OpenSslConnection {
    stream: tokio_openssl::SslStream<TcpStream>,
}

#[cfg(all(feature = "openssl", not(feature = "tls")))]
impl OpenSslConnection {
    pub(super) fn new(stream: tokio_openssl::SslStream<TcpStream>) -> Self {
        Self { stream }
    }
}

#[cfg(all(feature = "openssl", not(feature = "tls")))]
impl_connection!(OpenSslConnection);

/////////////////////////////////////////////////////////////////////////////

/// Bytes a WebSocket transport buffers for its next frame before a write
/// must wait for that frame to be sent.
#[cfg(feature = "websocket")]
const WS_WRITE_BUF_MAX: usize = 64 * 1024;

/// A WebSocket transport wrapping a [`WebSocketStream`].
///
/// Presents a byte-stream interface (`AsyncRead` + `AsyncWrite`) over a
/// WebSocket connection.  Writes accumulate, and each flush sends what has
/// accumulated as one binary WebSocket frame, which may hold several MQTT
/// packets (MQTT §6 allows it); reads reassemble frames back into a byte
/// stream.  Requires the `websocket` feature.
///
/// Once [`WS_WRITE_BUF_MAX`] bytes are waiting, or while a frame is still
/// going out, a write drives the flush itself and accepts nothing until it
/// completes, so the transport pushes back on a writer that outruns it.
#[cfg(feature = "websocket")]
pub(super) struct WsConnection<S> {
    stream: WebSocketStream<S>,
    /// Overflow bytes from the last received binary frame not yet consumed.
    read_buf: Vec<u8>,
    /// Position within `read_buf` of the next unread byte.
    read_pos: usize,
    /// Bytes accumulated via `poll_write`; sent as one binary frame on flush.
    write_buf: Vec<u8>,
    /// Non-`None` while we are waiting for `Sink::poll_ready` to return
    /// `Ready` before calling `start_send`.
    pending_msg: Option<Vec<u8>>,
    /// The last flush returned `Pending`: a frame is still going out.
    flushing: bool,
}

#[cfg(feature = "websocket")]
impl<S: AsyncRead + AsyncWrite + Unpin> WsConnection<S> {
    pub(super) fn new(stream: WebSocketStream<S>) -> Self {
        Self {
            stream,
            read_buf: Vec::new(),
            read_pos: 0,
            write_buf: Vec::new(),
            pending_msg: None,
            flushing: false,
        }
    }

    /// Hands the accumulated bytes to the sink as one binary frame, and
    /// flushes the sink.
    fn poll_send_frame(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Move the accumulated write buffer into `pending_msg` for sending.
        if self.pending_msg.is_none() && !self.write_buf.is_empty() {
            self.pending_msg = Some(std::mem::take(&mut self.write_buf));
        }

        // If there is a pending message, drive it through the Sink.
        if let Some(data) = self.pending_msg.take() {
            match Pin::new(&mut self.stream).poll_ready(cx) {
                Poll::Pending => {
                    self.pending_msg = Some(data);
                    return Poll::Pending;
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)));
                }
                Poll::Ready(Ok(())) => {
                    if let Err(e) = Pin::new(&mut self.stream).start_send(WsMessage::Binary(data)) {
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)));
                    }
                }
            }
        }

        // Flush the sink to push the frame onto the wire.
        Pin::new(&mut self.stream)
            .poll_flush(cx)
            .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))
    }
}

#[cfg(feature = "websocket")]
impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for WsConnection<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        // Serve buffered bytes from a previous frame first.
        if this.read_pos < this.read_buf.len() {
            let available = &this.read_buf[this.read_pos..];
            let to_copy = buf.remaining().min(available.len());
            buf.put_slice(&available[..to_copy]);
            this.read_pos += to_copy;
            if this.read_pos >= this.read_buf.len() {
                this.read_buf.clear();
                this.read_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }

        // Poll the WebSocket stream for the next message.
        loop {
            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)));
                }
                Poll::Ready(Some(Ok(msg))) => match msg {
                    WsMessage::Binary(data) => {
                        if data.is_empty() {
                            continue;
                        }
                        let to_copy = buf.remaining().min(data.len());
                        buf.put_slice(&data[..to_copy]);
                        if to_copy < data.len() {
                            this.read_buf = data;
                            this.read_pos = to_copy;
                        }
                        return Poll::Ready(Ok(()));
                    }
                    WsMessage::Text(text) => {
                        let data = text.into_bytes();
                        if data.is_empty() {
                            continue;
                        }
                        let to_copy = buf.remaining().min(data.len());
                        buf.put_slice(&data[..to_copy]);
                        if to_copy < data.len() {
                            this.read_buf = data;
                            this.read_pos = to_copy;
                        }
                        return Poll::Ready(Ok(()));
                    }
                    WsMessage::Close(_) => return Poll::Ready(Ok(())),
                    // Control frames handled internally by tungstenite; skip.
                    WsMessage::Ping(_) | WsMessage::Pong(_) | WsMessage::Frame(_) => {
                        continue;
                    }
                },
            }
        }
    }
}

#[cfg(feature = "websocket")]
impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for WsConnection<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_vectored(cx, &[io::IoSlice::new(buf)])
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.flushing || this.write_buf.len() >= WS_WRITE_BUF_MAX {
            std::task::ready!(Pin::new(&mut *this).poll_flush(cx))?;
        }
        // Take only what fits under the cap, so no frame is much larger.
        let room = WS_WRITE_BUF_MAX - this.write_buf.len();
        let mut n = 0;
        for b in bufs {
            let k = b.len().min(room - n);
            this.write_buf.extend_from_slice(&b[..k]);
            n += k;
            if n == room {
                break;
            }
        }
        Poll::Ready(Ok(n))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let res = this.poll_send_frame(cx);
        this.flushing = res.is_pending();
        res
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream)
            .poll_close(cx)
            .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))
    }
}

#[cfg(feature = "websocket")]
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
