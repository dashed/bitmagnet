//! A local TMDB over TLS, for the live transport's tests.
//!
//! Each stub has its own throwaway CA, whose certificate a test hands to
//! [`crate::ReqwestTransport::with_root_certificate`], and a leaf for
//! `127.0.0.1`. The server's ALPN offer decides the protocol, as TMDB's does:
//! [`http2`] offers only `h2`, [`http1`] only `http/1.1`.
//!
//! `/3/authentication` answers 200 with an empty body, so a client's key
//! validation passes. Any other request is answered from its `query`
//! parameter.

use std::sync::Arc;

use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_rustls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_rustls::rustls::{crypto, ServerConfig};
use tokio_rustls::TlsAcceptor;

pub(crate) struct TlsStub {
    /// `https://127.0.0.1:<port>/3`, for `TmdbConfig::base_url`.
    pub(crate) base_url: String,
    /// The CA the leaf was issued by.
    pub(crate) root: reqwest::Certificate,
    server: JoinHandle<()>,
}

impl Drop for TlsStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// An HTTP/2-only server that answers a request with the status in its `query`
/// parameter and an empty body.
pub(crate) async fn http2() -> TlsStub {
    start(b"h2", |stream| async move {
        let Ok(mut connection) = h2::server::handshake(stream).await else {
            return;
        };
        while let Some(Ok((request, mut respond))) = connection.accept().await {
            let status = if request.uri().path() == "/3/authentication" {
                200
            } else {
                query(request.uri().query().unwrap_or_default())
                    .parse()
                    .expect("a numeric query")
            };
            let response = http::Response::builder()
                .status(status)
                .body(())
                .expect("response");
            respond.send_response(response, true).expect("send");
        }
    })
    .await
}

/// An HTTP/1.1-only server that answers a request with the status line
/// `status_line` gives for its `query` parameter, written as is.
pub(crate) async fn http1(status_line: fn(&str) -> &'static [u8]) -> TlsStub {
    start(b"http/1.1", move |mut stream| async move {
        let mut request = Vec::new();
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let mut chunk = [0_u8; 1024];
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(read) => request.extend_from_slice(&chunk[..read]),
            }
        }
        let request_line = String::from_utf8_lossy(&request);
        let target = request_line.split(' ').nth(1).unwrap_or_default();
        let (path, query_string) = target.split_once('?').unwrap_or((target, ""));
        let line: &[u8] = if path == "/3/authentication" {
            b"HTTP/1.1 200 OK"
        } else {
            status_line(query(query_string))
        };
        stream
            .write_all(&[line, b"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"].concat())
            .await
            .expect("write response");
        let _ = stream.shutdown().await;
    })
    .await
}

/// The `query` parameter of a query string, undecoded.
fn query(query_string: &str) -> &str {
    query_string
        .split('&')
        .find_map(|pair| pair.strip_prefix("query="))
        .unwrap_or_default()
}

async fn start<F, Fut>(alpn: &[u8], serve: F) -> TlsStub
where
    F: Fn(tokio_rustls::server::TlsStream<TcpStream>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let ca_key = KeyPair::generate().expect("CA key");
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("CA params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).expect("CA");

    let leaf_key = KeyPair::generate().expect("leaf key");
    let leaf = CertificateParams::new(vec!["127.0.0.1".to_owned()])
        .expect("leaf params")
        .signed_by(&leaf_key, &ca, &ca_key)
        .expect("leaf");

    let mut config =
        ServerConfig::builder_with_provider(Arc::new(crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("protocol versions")
            .with_no_client_auth()
            .with_single_cert(
                vec![leaf.der().clone()],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(leaf_key.serialize_der())),
            )
            .expect("server config");
    config.alpn_protocols = vec![alpn.to_vec()];
    let acceptor = TlsAcceptor::from(Arc::new(config));

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    let serve = Arc::new(serve);
    let server = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            let serve = Arc::clone(&serve);
            tokio::spawn(async move {
                if let Ok(stream) = acceptor.accept(socket).await {
                    serve(stream).await;
                }
            });
        }
    });

    TlsStub {
        base_url: format!("https://{address}/3"),
        root: reqwest::Certificate::from_der(ca.der()).expect("root"),
        server,
    }
}
