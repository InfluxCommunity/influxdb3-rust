use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use arrow_flight::{
    flight_service_server::{FlightService, FlightServiceServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, Result as FlightResult, SchemaResult,
    Ticket,
};
use futures_util::stream::Empty as EmptyStream;
use influxdb3_client::{Client, ClientConfig, RetryConfig};
use rcgen::CertifiedKey;
use tokio::{net::TcpListener, sync::oneshot};
use tonic::{
    metadata::MetadataMap,
    transport::{server::TcpIncoming, Identity, Server, ServerTlsConfig},
    Request, Response, Status,
};

type MockStream<T> = EmptyStream<std::result::Result<T, Status>>;

fn fast_retry(max_retries: u32) -> RetryConfig {
    RetryConfig {
        max_retries,
        base_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(1),
        ..RetryConfig::default()
    }
}

#[derive(Clone)]
struct CapturingFlightService {
    metadata: Arc<Mutex<Option<MetadataMap>>>,
    do_get_calls: Arc<AtomicUsize>,
    failures_remaining: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl FlightService for CapturingFlightService {
    type HandshakeStream = MockStream<HandshakeResponse>;
    type ListFlightsStream = MockStream<FlightInfo>;
    type DoGetStream = MockStream<FlightData>;
    type DoPutStream = MockStream<PutResult>;
    type DoExchangeStream = MockStream<FlightData>;
    type DoActionStream = MockStream<FlightResult>;
    type ListActionsStream = MockStream<ActionType>;

    async fn handshake(
        &self,
        _request: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        self.do_get_calls.fetch_add(1, Ordering::SeqCst);
        if self.failures_remaining.load(Ordering::SeqCst) > 0 {
            self.failures_remaining.fetch_sub(1, Ordering::SeqCst);
            return Err(Status::unavailable("transient query failure"));
        }
        *self.metadata.lock().unwrap() = Some(request.metadata().clone());
        Ok(Response::new(futures_util::stream::empty()))
    }

    async fn do_put(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    async fn do_exchange(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}

#[tokio::test]
async fn query_stream_sends_metadata_headers(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let metadata = Arc::new(Mutex::new(None));
    let service = CapturingFlightService {
        metadata: Arc::clone(&metadata),
        do_get_calls: Arc::new(AtomicUsize::new(0)),
        failures_remaining: Arc::new(AtomicUsize::new(0)),
    };

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = TcpIncoming::from(listener);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let client = Client::new(
        ClientConfig::builder()
            .host(format!("http://{addr}"))
            .token("TEST_TOKEN")
            .database("db")
            .query_timeout(Duration::from_secs(5))
            .build()?,
    )
    .await?;

    let stream = client
        .sql("SELECT * FROM test")
        .header("X-Tracing-Id", "123")
        .stream()
        .await?;
    drop(stream);

    let captured = metadata.lock().unwrap().clone().unwrap();
    assert_eq!(
        captured.get("authorization").unwrap().to_str().unwrap(),
        "Bearer TEST_TOKEN"
    );
    assert_eq!(
        captured.get("x-tracing-id").unwrap().to_str().unwrap(),
        "123"
    );

    shutdown_tx.send(()).unwrap();
    server.await??;

    Ok(())
}

#[tokio::test]
async fn query_stream_retries_transient_flight_errors(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let do_get_calls = Arc::new(AtomicUsize::new(0));
    let service = CapturingFlightService {
        metadata: Arc::new(Mutex::new(None)),
        do_get_calls: Arc::clone(&do_get_calls),
        failures_remaining: Arc::new(AtomicUsize::new(1)),
    };

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = TcpIncoming::from(listener);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let client = Client::new(
        ClientConfig::builder()
            .host(format!("http://{addr}"))
            .token("TEST_TOKEN")
            .database("db")
            .query_timeout(Duration::from_secs(5))
            .build()?,
    )
    .await?;

    let stream = client
        .sql("SELECT * FROM test")
        .retry(fast_retry(1))
        .stream()
        .await?;
    drop(stream);

    assert_eq!(do_get_calls.load(Ordering::SeqCst), 2);

    shutdown_tx.send(()).unwrap();
    server.await??;

    Ok(())
}

#[tokio::test]
async fn query_stream_works_over_tls(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let do_get_calls = Arc::new(AtomicUsize::new(0));
    let service = CapturingFlightService {
        metadata: Arc::new(Mutex::new(None)),
        do_get_calls: Arc::clone(&do_get_calls),
        failures_remaining: Arc::new(AtomicUsize::new(0)),
    };

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let incoming = TcpIncoming::from(listener);

    // Self-signed cert for `localhost`; the SAN must match the host the client
    // connects to, otherwise tonic rejects the certificate.
    let CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()])?;
    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();

    // The client trusts the server cert via a CA roots file (same PEM).
    let ca_path = std::env::temp_dir().join(format!("influxdb3-tls-test-{port}.pem"));
    std::fs::write(&ca_path, &cert_pem)?;

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        Server::builder()
            .tls_config(ServerTlsConfig::new().identity(Identity::from_pem(cert_pem, key_pem)))?
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let client = Client::new(
        ClientConfig::builder()
            .host(format!("https://localhost:{port}"))
            .token("TEST_TOKEN")
            .database("db")
            .ssl_roots_path(ca_path.to_str().unwrap())
            .query_timeout(Duration::from_secs(5))
            .build()?,
    )
    .await?;

    // A successful query proves the TLS handshake works end-to-end: tonic's
    // `tls-ring` provider negotiated the connection and trusted the CA cert.
    let stream = client.sql("SELECT * FROM test").stream().await?;
    drop(stream);
    assert_eq!(do_get_calls.load(Ordering::SeqCst), 1);

    let _ = std::fs::remove_file(&ca_path);
    shutdown_tx.send(()).unwrap();
    server.await??;

    Ok(())
}
