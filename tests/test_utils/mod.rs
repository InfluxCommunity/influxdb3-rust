#![allow(dead_code)]

use arrow_flight::{
    flight_service_server::{FlightService, FlightServiceServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, Result as FlightResult, SchemaResult,
    Ticket,
};
use axum::routing::post;
use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
use futures_util::stream::Empty as EmptyStream;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use std::fs::File;
use std::io::BufReader;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::metadata::MetadataMap;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status};

pub type MockStream<T> = EmptyStream<Result<T, Status>>;

#[derive(Clone)]
pub struct CapturingFlightService {
    metadata: Arc<Mutex<Option<MetadataMap>>>,
    do_get_calls: Arc<AtomicUsize>,
    failures_remaining: Arc<AtomicUsize>,
}

impl CapturingFlightService {
    pub fn new(
        metadata: Arc<Mutex<Option<MetadataMap>>>,
        do_get_calls: Arc<AtomicUsize>,
        failures_remaining: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            metadata,
            do_get_calls,
            failures_remaining,
        }
    }
}

#[tonic::async_trait]
impl FlightService for CapturingFlightService {
    type HandshakeStream = MockStream<HandshakeResponse>;
    async fn handshake(
        &self,
        _request: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }
    type ListFlightsStream = MockStream<FlightInfo>;
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }
    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }
    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    type DoGetStream = MockStream<FlightData>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        self.do_get_calls.fetch_add(1, Ordering::SeqCst);
        if self
            .failures_remaining
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                if remaining > 0 {
                    Some(remaining - 1)
                } else {
                    None
                }
            })
            .is_ok()
        {
            return Err(Status::unavailable("transient query failure"));
        }
        *self.metadata.lock().unwrap() = Some(request.metadata().clone());
        Ok(Response::new(futures_util::stream::empty()))
    }

    type DoPutStream = MockStream<PutResult>;

    async fn do_put(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    type DoExchangeStream = MockStream<FlightData>;

    async fn do_exchange(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    type DoActionStream = MockStream<FlightResult>;

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    type ListActionsStream = MockStream<ActionType>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}

pub struct QueryServer {
    handle: JoinHandle<Result<(), tonic::transport::Error>>,
    shutdown_tx: oneshot::Sender<()>,
}

impl QueryServer {
    pub async fn shutdown(self) -> Result<(), tonic::transport::Error> {
        let _ = self.shutdown_tx.send(());
        self.handle.await.expect("server task panicked")
    }
}

pub async fn start_secure_write_server(
    listener: std::net::TcpListener,
    ca_cert_path: &str,
    server_cert_path: &str,
    server_key_path: &str,
    require_client_auth: bool,
) -> JoinHandle<()> {
    // 1. Load CA cert to verify client certificates
    let mut root_store = RootCertStore::empty();
    let ca_certs = load_certs(ca_cert_path);
    for cert in ca_certs {
        root_store.add(cert).expect("failed to add CA cert");
    }

    let client_verifier = WebPkiClientVerifier::builder(Arc::new(root_store))
        .build()
        .expect("failed to build client verifier");

    // 2. Load server .crt and .key
    let server_certs = load_certs(server_cert_path);
    let server_key = load_private_key(server_key_path);

    // TODO refactor this
    let app = Router::new().route("/api/v2/write", post(|| async { "Hello, mTLS!" }));
    println!(
        "mTLS server listening on https://{}",
        listener.local_addr().unwrap()
    );

    let handle = tokio::spawn(async move {
        let mut tls_config: ServerConfig;
        if require_client_auth {
            tls_config = ServerConfig::builder()
                .with_client_cert_verifier(client_verifier)
                .with_single_cert(server_certs, server_key)
                .expect("failed to build rustls ServerConfig");
        } else {
            tls_config = ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(server_certs, server_key)
                .expect("failed to build rustls ServerConfig");
        }

        tls_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let config = RustlsConfig::from_config(Arc::new(tls_config));
        axum_server::from_tcp_rustls(listener, config)
            .expect("Setup axum server failed")
            .serve(app.into_make_service())
            .await
            .unwrap();
    });

    handle
}

pub fn start_secure_query_server(
    server_cert_path: &str,
    server_key_path: &str,
    client_cert_path: &str,
    incoming: TcpIncoming,
    service: CapturingFlightService,
    require_client_auth: bool,
) -> QueryServer {
    let identity = read_tls_identity(server_cert_path, server_key_path);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let client_cert = read_tls_pem(client_cert_path);
    let handle = tokio::spawn(async move {
        let mut tls_config = ServerTlsConfig::new();
        tls_config = tls_config.identity(identity);
        if require_client_auth {
            tls_config = tls_config.client_ca_root(client_cert);
        }

        Server::builder()
            .tls_config(tls_config)?
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    QueryServer {
        handle,
        shutdown_tx,
    }
}

fn read_tls_identity(server_cert_path: &str, server_key_path: &str) -> Identity {
    let server_cert = std::fs::read(server_cert_path).expect("failed to read server certificate");
    let server_key = std::fs::read(server_key_path).expect("failed to read server key");
    Identity::from_pem(server_cert, server_key)
}

fn load_certs(path: &str) -> Vec<CertificateDer<'static>> {
    let file = File::open(path).unwrap_or_else(|_| panic!("cannot open certificate file"));
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .map(|res| res.expect("invalid certificate"))
        .collect()
}

fn load_private_key(path: &str) -> PrivateKeyDer<'static> {
    let file = File::open(path).unwrap_or_else(|_| panic!("cannot open private key file"));
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .expect("failed to parse private key")
        .expect("no private key found")
}

pub fn read_tls_pem(client_cert_path: &str) -> Certificate {
    let client_cert = std::fs::read(client_cert_path).expect("failed to certificate");
    Certificate::from_pem(client_cert)
}

pub struct TestCerts {
    pub ca_cert_path: String,
    pub server_cert_path: String,
    pub server_key_path: String,
    pub server_pem_path: String,
    pub client_cert_path: String,
    pub client_key_path: String,
    pub client_pem_path: String,
}

struct LeafPaths {
    cert_path: String,
    key_path: String,
    bundle_path: String,
}

fn write_temp(file_name: &str, contents: &str) -> String {
    static NEXT_FILE_ID: AtomicUsize = AtomicUsize::new(0);
    let id = NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "influxdb3-tls-{}-{id}-{file_name}",
        std::process::id()
    ));
    std::fs::write(&path, contents).unwrap();
    path.to_str().unwrap().to_string()
}

fn create_ca(common_name: &str) -> (Issuer<'static, KeyPair>, String) {
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);

    let key = KeyPair::generate().unwrap();
    let cert = params.self_signed(&key).unwrap();
    let cert_path = write_temp("ca-cert-path.pem", &cert.pem());

    (Issuer::new(params, key), cert_path)
}

fn create_leaf_cert(
    name_prefix: &str,
    subject_alt_names: Vec<String>,
    key_usage: ExtendedKeyUsagePurpose,
    issuer: &Issuer<'_, KeyPair>,
) -> LeafPaths {
    let mut params = CertificateParams::new(subject_alt_names).unwrap();
    params.extended_key_usages = vec![key_usage];

    let key = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key, issuer).unwrap();
    let key_pem = key.serialize_pem();
    let cert_pem = cert.pem();

    LeafPaths {
        cert_path: write_temp(&format!("{name_prefix}-cert-path.crt"), &cert_pem),
        key_path: write_temp(&format!("{name_prefix}-key-path.key"), &key_pem),
        bundle_path: write_temp(
            &format!("{name_prefix}-bundle-path.pem"),
            &format!("{cert_pem}{key_pem}"),
        ),
    }
}

pub fn generate_certs() -> TestCerts {
    let (ca_issuer, ca_cert_path) = create_ca("Test Root CA");

    let server = create_leaf_cert(
        "server",
        vec!["localhost".into(), "127.0.0.1".into()],
        ExtendedKeyUsagePurpose::ServerAuth,
        &ca_issuer,
    );

    let client = create_leaf_cert(
        "client",
        vec!["test-client".into()],
        ExtendedKeyUsagePurpose::ClientAuth,
        &ca_issuer,
    );

    TestCerts {
        ca_cert_path,
        server_cert_path: server.cert_path,
        server_key_path: server.key_path,
        server_pem_path: server.bundle_path,
        client_cert_path: client.cert_path,
        client_key_path: client.key_path,
        client_pem_path: client.bundle_path,
    }
}

pub fn error_chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut parts = vec![err.to_string()];
    let mut cur = err.source();
    while let Some(c) = cur {
        parts.push(c.to_string());
        cur = c.source();
    }
    parts.join(": ")
}
