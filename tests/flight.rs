use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
    time::Duration,
};

use arrow_flight::{FlightData, Ticket};
use futures_util::stream::Empty as EmptyStream;
use influxdb3_client::{Client, ClientConfig};
use tokio::{net::TcpListener, sync::oneshot};
use tonic::{
    codegen::{http, Body, BoxFuture, Context, Poll, Service, StdError},
    metadata::MetadataMap,
    transport::{server::TcpIncoming, Server},
    Request, Response, Status,
};

type MockStream<T> = EmptyStream<std::result::Result<T, Status>>;

#[derive(Clone)]
struct CapturingFlightService {
    metadata: Arc<Mutex<Option<MetadataMap>>>,
}

impl<B> Service<http::Request<B>> for CapturingFlightService
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        match req.uri().path() {
            "/arrow.flight.protocol.FlightService/DoGet" => {
                let metadata = Arc::clone(&self.metadata);
                Box::pin(async move {
                    let method = DoGetSvc { metadata };
                    let codec = tonic::codec::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    Ok(grpc.server_streaming(method, req).await)
                })
            }
            _ => Box::pin(async {
                Ok(http::Response::builder()
                    .status(200)
                    .header("grpc-status", "12")
                    .header("content-type", "application/grpc")
                    .body(tonic::body::empty_body())
                    .unwrap())
            }),
        }
    }
}

impl tonic::server::NamedService for CapturingFlightService {
    const NAME: &'static str = "arrow.flight.protocol.FlightService";
}

struct DoGetSvc {
    metadata: Arc<Mutex<Option<MetadataMap>>>,
}

impl tonic::server::ServerStreamingService<Ticket> for DoGetSvc {
    type Response = FlightData;
    type ResponseStream = MockStream<FlightData>;
    type Future = BoxFuture<Response<Self::ResponseStream>, Status>;

    fn call(&mut self, request: Request<Ticket>) -> Self::Future {
        let metadata = Arc::clone(&self.metadata);
        Box::pin(async move {
            *metadata.lock().unwrap() = Some(request.metadata().clone());
            Ok(Response::new(futures_util::stream::empty()))
        })
    }
}

#[tokio::test]
async fn query_stream_sends_metadata_headers(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let metadata = Arc::new(Mutex::new(None));
    let service = CapturingFlightService {
        metadata: Arc::clone(&metadata),
    };

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = TcpIncoming::from_listener(listener, true, None)?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
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
