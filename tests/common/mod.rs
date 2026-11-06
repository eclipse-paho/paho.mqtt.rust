// tests/common/mod.rs
//
// A scripted MQTT broker for the Eclipse Paho MQTT Rust client tests.
//
// Some protocol behaviour cannot be provoked from a stock Mosquitto: it will
// not refuse a subscription that its configuration allows, will not impose a
// Server Keep Alive, and has no enhanced-authentication method built in.
// These tests instead drive the client against a broker scripted for the
// case at hand, built on `paho-mqtt-codec` — which also exercises the codec
// from the server's side.

#![allow(dead_code)] // each test file uses a different part of this

use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::Duration,
};

use bytes::BytesMut;
use paho_mqtt::{
    MqttVersion,
    paho_mqtt_codec::{Packet, RawPacket},
};

/// What a handler wants done in response to one packet.
pub struct Reply {
    /// Packets to send back, in order.
    pub packets: Vec<Packet>,
    /// Drop the connection once they have gone out — how a test provokes a
    /// reconnect.
    pub close: bool,
}

impl Reply {
    /// Send nothing and keep the connection.
    pub fn none() -> Self {
        Self {
            packets: Vec::new(),
            close: false,
        }
    }

    /// Send these packets, then drop the connection.
    pub fn close_after(packets: Vec<Packet>) -> Self {
        Self {
            packets,
            close: true,
        }
    }
}

impl From<Vec<Packet>> for Reply {
    fn from(packets: Vec<Packet>) -> Self {
        Self {
            packets,
            close: false,
        }
    }
}

/// A persistence store the test can look inside.
///
/// Every clone shares one map, so a test can watch what the client writes
/// while the client owns its own handle to the same store.
#[derive(Clone, Default)]
pub struct SpyPersistence(Arc<Mutex<std::collections::HashMap<String, Vec<u8>>>>);

impl SpyPersistence {
    pub fn new() -> Self {
        Self::default()
    }

    /// The keys currently held, sorted.
    pub fn keys(&self) -> Vec<String> {
        let mut k: Vec<_> = self.0.lock().unwrap().keys().cloned().collect();
        k.sort();
        k
    }

    /// Decodes the packet stored under `key`, for the given version.
    pub fn packet(&self, key: &str, ver: MqttVersion) -> Option<Packet> {
        let data = self.0.lock().unwrap().get(key).cloned()?;
        let mut buf = BytesMut::from(data.as_slice());
        let raw = RawPacket::decode(&mut buf).ok()??;
        Packet::from_raw(raw, ver).ok()
    }

    /// Writes an entry directly, to set up a store as a previous run of the
    /// program would have left it.
    pub fn preload(&self, key: &str, packet: &Packet, ver: MqttVersion) {
        let mut buf = BytesMut::new();
        packet.encode(&mut buf, ver).unwrap();
        self.0.lock().unwrap().insert(key.to_string(), buf.to_vec());
    }
}

impl paho_mqtt::ClientPersistence for SpyPersistence {
    fn open(&mut self, _client_id: &str, _server_uri: &str) -> paho_mqtt::Result<()> {
        Ok(())
    }
    fn close(&mut self) -> paho_mqtt::Result<()> {
        Ok(())
    }
    fn put(&mut self, key: &str, buffers: Vec<&[u8]>) -> paho_mqtt::Result<()> {
        self.0
            .lock()
            .unwrap()
            .insert(key.to_string(), buffers.concat());
        Ok(())
    }
    fn get(&mut self, key: &str) -> paho_mqtt::Result<Vec<u8>> {
        self.0
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| paho_mqtt::Error::Persistence(format!("key not found: {key}")))
    }
    fn remove(&mut self, key: &str) -> paho_mqtt::Result<()> {
        self.0.lock().unwrap().remove(key);
        Ok(())
    }
    fn keys(&mut self) -> paho_mqtt::Result<Vec<String>> {
        Ok(self.0.lock().unwrap().keys().cloned().collect())
    }
    fn clear(&mut self) -> paho_mqtt::Result<()> {
        self.0.lock().unwrap().clear();
        Ok(())
    }
    fn contains_key(&mut self, key: &str) -> bool {
        self.0.lock().unwrap().contains_key(key)
    }
}

/// A broker that serves one or more connections from a handler closure.
pub struct ScriptedBroker {
    /// URI to hand to `AsyncClient::new`.
    pub uri: String,
    received: Arc<Mutex<Vec<Packet>>>,
    handle: Option<JoinHandle<()>>,
}

impl ScriptedBroker {
    /// Every packet the broker has decoded so far, in arrival order.
    pub fn received(&self) -> Vec<Packet> {
        self.received.lock().unwrap().clone()
    }

    /// The packets of one type received so far.
    pub fn received_matching<T>(&self, f: impl Fn(&Packet) -> Option<T>) -> Vec<T> {
        self.received().iter().filter_map(f).collect()
    }

    /// Waits until `pred` holds over the received packets, or the timeout
    /// expires. Returns whether it held.
    pub fn wait_until(&self, timeout: Duration, pred: impl Fn(&[Packet]) -> bool) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if pred(&self.received()) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        pred(&self.received())
    }

    /// Waits for the broker thread to finish serving its connection.
    pub fn join(mut self) {
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Starts a broker that answers each packet with whatever `handler` returns.
///
/// The handler sees every decoded packet, including CONNECT, so each test
/// decides its own CONNACK — that is how the negotiated properties (Receive
/// Maximum, Server Keep Alive, Assigned Client Identifier, Topic Alias
/// Maximum) get under test. Returning packets sends them in order.
///
/// The connection is served until the client disconnects or closes it.
pub fn spawn_broker<F>(mut handler: F) -> ScriptedBroker
where
    F: FnMut(&Packet) -> Vec<Packet> + Send + 'static,
{
    spawn_broker_seq(1, move |pkt, _| Reply::from(handler(pkt)))
}

/// Starts a broker that serves up to `max_conns` connections in turn.
///
/// The handler additionally receives the zero-based number of the connection
/// the packet arrived on, so a test can answer the first connection
/// differently from the reconnect that follows it — which is how session
/// resumption gets under test.
pub fn spawn_broker_seq<F>(max_conns: usize, mut handler: F) -> ScriptedBroker
where
    F: FnMut(&Packet, usize) -> Reply + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let uri = format!("tcp://{}", listener.local_addr().unwrap());
    let received = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&received);

    let handle = std::thread::spawn(move || {
        for conn in 0..max_conns {
            let Ok((sock, _)) = listener.accept() else {
                return;
            };
            sock.set_read_timeout(Some(Duration::from_secs(30)))
                .unwrap();
            serve_connection(sock, conn, &recorded, &mut handler);
        }
    });

    ScriptedBroker {
        uri,
        received,
        handle: Some(handle),
    }
}

/// Serves one connection: decode packets, record them, and send whatever the
/// handler asks for. Generic over the stream so the same loop serves a plain
/// TCP connection and a TLS one.
fn serve_connection<S, F>(
    mut sock: S,
    conn: usize,
    recorded: &Arc<Mutex<Vec<Packet>>>,
    handler: &mut F,
) where
    S: Read + Write,
    F: FnMut(&Packet, usize) -> Reply,
{
    let mut buf = BytesMut::with_capacity(1024);
    let mut tmp = [0u8; 1024];
    // Until CONNECT names a version, decode as v5; CONNECT is
    // self-describing, so this only has to be a placeholder.
    let mut ver = MqttVersion::V5;

    loop {
        let n = match sock.read(&mut tmp) {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        buf.extend_from_slice(&tmp[..n]);

        while let Some(raw) = match RawPacket::decode(&mut buf) {
            Ok(r) => r,
            Err(_) => return,
        } {
            let Ok(packet) = Packet::from_raw(raw, ver) else {
                return;
            };
            if let Packet::Connect(c) = &packet {
                ver = c.version;
            }
            let is_disconnect = matches!(packet, Packet::Disconnect(_));
            recorded.lock().unwrap().push(packet.clone());

            let reply = handler(&packet, conn);
            for pkt in reply.packets {
                let mut out = BytesMut::new();
                if pkt.encode(&mut out, ver).is_err() || sock.write_all(&out).is_err() {
                    return;
                }
            }
            if reply.close || is_disconnect {
                return;
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
// TLS
/////////////////////////////////////////////////////////////////////////////

/// A throw-away certificate authority and a server certificate it signed.
///
/// Lets a test connect to `127.0.0.1` while the broker presents a
/// certificate naming something else, which is the only way to tell whether
/// host-name verification is actually being done.
#[cfg(any(feature = "tls", feature = "smol-tls", feature = "openssl"))]
pub struct TestCerts {
    /// PEM file holding the CA certificate, for `TlsOptions::trust_store`.
    pub ca_file: tempfile::TempPath,
    /// PEM file holding a client certificate signed by the same CA, for
    /// `TlsOptions::key_store`.
    pub client_cert_file: tempfile::TempPath,
    /// PEM file holding that certificate's private key, for
    /// `TlsOptions::private_key`.
    pub client_key_file: tempfile::TempPath,
    ca_der: Vec<u8>,
    cert_der: Vec<u8>,
    key_der: Vec<u8>,
}

#[cfg(any(feature = "tls", feature = "smol-tls", feature = "openssl"))]
impl TestCerts {
    /// Creates a CA and a server certificate valid for `server_name` only.
    pub fn new(server_name: &str) -> Self {
        Self::with_server_params(server_name, |_| {})
    }

    /// As [`new`](Self::new), with a server certificate that has expired.
    pub fn expired(server_name: &str) -> Self {
        Self::with_server_params(server_name, |params| {
            params.not_before = rcgen::date_time_ymd(2000, 1, 1);
            params.not_after = rcgen::date_time_ymd(2001, 1, 1);
        })
    }

    /// As [`new`](Self::new), with a server certificate that is not valid yet.
    pub fn not_yet_valid(server_name: &str) -> Self {
        Self::with_server_params(server_name, |params| {
            params.not_before = rcgen::date_time_ymd(2900, 1, 1);
        })
    }

    fn with_server_params(
        server_name: &str,
        adjust: impl FnOnce(&mut rcgen::CertificateParams),
    ) -> Self {
        use rcgen::{
            BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair, KeyUsagePurpose,
        };
        use std::io::Write as _;

        let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "paho-rust-v2 test CA");
        ca_params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let ca_key = KeyPair::generate().unwrap();
        let ca_cert = ca_params.clone().self_signed(&ca_key).unwrap();
        let issuer = Issuer::new(ca_params, ca_key);

        let mut params = CertificateParams::new(vec![server_name.to_string()]).unwrap();
        params
            .distinguished_name
            .push(DnType::CommonName, server_name);
        adjust(&mut params);
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &issuer).unwrap();

        // A client certificate from the same CA, for mutual TLS.
        let mut client_params = CertificateParams::new(Vec::new()).unwrap();
        client_params
            .distinguished_name
            .push(DnType::CommonName, "paho-rust-v2 test client");
        let client_key = KeyPair::generate().unwrap();
        let client_cert = client_params.signed_by(&client_key, &issuer).unwrap();

        let write = |contents: String| {
            let mut f = tempfile::NamedTempFile::new().unwrap();
            f.write_all(contents.as_bytes()).unwrap();
            f.flush().unwrap();
            f.into_temp_path()
        };

        Self {
            ca_file: write(ca_cert.pem()),
            client_cert_file: write(client_cert.pem()),
            client_key_file: write(client_key.serialize_pem()),
            ca_der: ca_cert.der().to_vec(),
            cert_der: cert.der().to_vec(),
            key_der: key.serialize_der(),
        }
    }

    /// The CA file path, for `TlsOptionsBuilder::trust_store`.
    pub fn ca_path(&self) -> &std::path::Path {
        &self.ca_file
    }
}

/// Starts a TLS broker that additionally *requires* a client certificate
/// signed by the test CA, for exercising mutual TLS.
#[cfg(feature = "tls")]
pub fn spawn_mtls_broker<F>(certs: &TestCerts, handler: F) -> ScriptedBroker
where
    F: FnMut(&Packet) -> Vec<Packet> + Send + 'static,
{
    use rustls::pki_types::CertificateDer;

    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from(certs.ca_der.clone()))
        .unwrap();
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots),
        Arc::new(rustls::crypto::ring::default_provider()),
    )
    .build()
    .unwrap();
    spawn_tls_broker_with(certs, Some(verifier), handler)
}

/// Starts a TLS broker presenting `certs`, answering from `handler`.
///
/// The returned URI uses `127.0.0.1`, which the certificate does not name —
/// so whether the handshake succeeds is exactly the question of whether the
/// client checked the name.
#[cfg(any(feature = "tls", feature = "smol-tls", feature = "openssl"))]
pub fn spawn_tls_broker<F>(certs: &TestCerts, handler: F) -> ScriptedBroker
where
    F: FnMut(&Packet) -> Vec<Packet> + Send + 'static,
{
    spawn_tls_broker_with(certs, None, handler)
}

/// Shared body of [`spawn_tls_broker`] and [`spawn_mtls_broker`]; `verifier`
/// is `Some` when a client certificate is required.
#[cfg(any(feature = "tls", feature = "smol-tls", feature = "openssl"))]
fn spawn_tls_broker_with<F>(
    certs: &TestCerts,
    verifier: Option<Arc<dyn rustls::server::danger::ClientCertVerifier>>,
    mut handler: F,
) -> ScriptedBroker
where
    F: FnMut(&Packet) -> Vec<Packet> + Send + 'static,
{
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let uri = format!("tls://{}", listener.local_addr().unwrap());
    let received = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&received);

    let builder = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap();
    let cfg = match verifier {
        Some(v) => builder.with_client_cert_verifier(v),
        None => builder.with_no_client_auth(),
    }
    .with_single_cert(
        vec![CertificateDer::from(certs.cert_der.clone())],
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(certs.key_der.clone())),
    )
    .unwrap();
    let cfg = Arc::new(cfg);

    let handle = std::thread::spawn(move || {
        let Ok((sock, _)) = listener.accept() else {
            return;
        };
        sock.set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        let Ok(conn) = rustls::ServerConnection::new(cfg) else {
            return;
        };
        let tls = rustls::StreamOwned::new(conn, sock);
        let mut h = move |p: &Packet, _: usize| Reply::from(handler(p));
        serve_connection(tls, 0, &recorded, &mut h);
    });

    ScriptedBroker {
        uri,
        received,
        handle: Some(handle),
    }
}

/////////////////////////////////////////////////////////////////////////////
// HTTP proxy
/////////////////////////////////////////////////////////////////////////////

/// A fake HTTP proxy, for testing `CONNECT` tunnelling.
pub struct TestProxy {
    /// URI to give to `ConnectOptionsBuilder::http_proxy`.
    pub uri: String,
    /// The `CONNECT` request lines the proxy received.
    requests: Arc<Mutex<Vec<String>>>,
}

impl TestProxy {
    /// The `CONNECT` requests seen so far.
    pub fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

/// Starts a proxy that answers `CONNECT` with `status`, and — for a 2xx
/// status — then pipes bytes to and from `target` so the tunnel really works.
///
/// `status` is the whole status line, e.g. `"200 Connection established"` or
/// `"407 Proxy Authentication Required"`.
pub fn spawn_proxy(status: &'static str, target: Option<String>) -> TestProxy {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let uri = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&requests);

    std::thread::spawn(move || {
        while let Ok((mut client, _)) = listener.accept() {
            let seen = Arc::clone(&seen);
            let target = target.clone();
            std::thread::spawn(move || {
                // Read up to the blank line that ends the request headers.
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                while !head.ends_with(b"\r\n\r\n") {
                    match client.read(&mut byte) {
                        Ok(1) => head.push(byte[0]),
                        _ => return,
                    }
                }
                let head = String::from_utf8_lossy(&head).to_string();
                seen.lock().unwrap().push(head.clone());

                let resp = format!("HTTP/1.1 {status}\r\n\r\n");
                if client.write_all(resp.as_bytes()).is_err() {
                    return;
                }
                if !status.starts_with('2') {
                    return;
                }

                // Tunnel: copy in both directions until either end closes.
                let Some(target) = target else { return };
                let Ok(upstream) = std::net::TcpStream::connect(&target) else {
                    return;
                };
                let (mut c_rd, mut c_wr) = (client.try_clone().unwrap(), client);
                let (mut u_rd, mut u_wr) = (upstream.try_clone().unwrap(), upstream);
                std::thread::spawn(move || {
                    let _ = std::io::copy(&mut c_rd, &mut u_wr);
                });
                let _ = std::io::copy(&mut u_rd, &mut c_wr);
            });
        }
    });

    TestProxy { uri, requests }
}

/// Starts a `wss://` broker: TLS, then a WebSocket upgrade, then MQTT in
/// binary frames — the whole stack a `wss://` URI implies.
#[cfg(all(feature = "tls", feature = "websocket"))]
pub fn spawn_wss_broker<F>(certs: &TestCerts, mut handler: F) -> (ScriptedBroker, String)
where
    F: FnMut(&Packet) -> Vec<Packet> + Send + 'static,
{
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use tungstenite::Message as WsMessage;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let uri = format!("wss://{addr}");
    let received = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&received);

    let cfg = Arc::new(
        rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(certs.cert_der.clone())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(certs.key_der.clone())),
        )
        .unwrap(),
    );

    let handle = std::thread::spawn(move || {
        let Ok((sock, _)) = listener.accept() else {
            return;
        };
        sock.set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        let Ok(conn) = rustls::ServerConnection::new(cfg) else {
            return;
        };
        let tls = rustls::StreamOwned::new(conn, sock);

        // MQTT over WebSocket negotiates the `mqtt` subprotocol, and the
        // client checks that the server agreed to it, so echo it back.
        type WsRequest = tungstenite::handshake::server::Request;
        type WsResponse = tungstenite::handshake::server::Response;
        type WsErrorResponse = tungstenite::handshake::server::ErrorResponse;

        #[allow(clippy::result_large_err)] // the shape tungstenite's callback requires
        fn add_subprotocol(
            _req: &WsRequest,
            mut resp: WsResponse,
        ) -> Result<WsResponse, WsErrorResponse> {
            resp.headers_mut().insert(
                "Sec-WebSocket-Protocol",
                tungstenite::http::HeaderValue::from_static("mqtt"),
            );
            Ok(resp)
        }

        let Ok(mut ws) = tungstenite::accept_hdr(tls, add_subprotocol) else {
            return;
        };

        let mut buf = BytesMut::with_capacity(1024);
        let mut ver = MqttVersion::V5;
        loop {
            match ws.read() {
                Ok(WsMessage::Binary(data)) => buf.extend_from_slice(&data),
                // Ping/pong and the like are handled by tungstenite itself.
                Ok(_) => continue,
                Err(_) => return,
            }

            while let Some(raw) = match RawPacket::decode(&mut buf) {
                Ok(r) => r,
                Err(_) => return,
            } {
                let Ok(packet) = Packet::from_raw(raw, ver) else {
                    return;
                };
                if let Packet::Connect(c) = &packet {
                    ver = c.version;
                }
                let is_disconnect = matches!(packet, Packet::Disconnect(_));
                recorded.lock().unwrap().push(packet.clone());

                for reply in handler(&packet) {
                    let mut out = BytesMut::new();
                    if reply.encode(&mut out, ver).is_err()
                        || ws.send(WsMessage::Binary(out.to_vec())).is_err()
                    {
                        return;
                    }
                }
                if is_disconnect {
                    return;
                }
            }
        }
    });

    (
        ScriptedBroker {
            uri: uri.clone(),
            received,
            handle: Some(handle),
        },
        uri,
    )
}
