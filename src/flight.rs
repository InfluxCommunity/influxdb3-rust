/// Arrow Flight gRPC query transport for InfluxDB 3.
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use arrow_flight::{
    decode::FlightRecordBatchStream, flight_service_client::FlightServiceClient, Ticket,
};
use arrow_schema::SchemaRef;
use bytes::Bytes;
use futures_util::{Stream, TryStreamExt};
use serde_json::{json, Value as JsonValue};
use tonic::{
    metadata::MetadataValue,
    transport::{Channel, ClientTlsConfig, Endpoint},
    Request,
};
use url::Url;

use crate::{
    error::Error,
    query::{QueryOptions, QueryResult},
};

/// Holds the gRPC channel.  `Channel` is internally an `Arc` over the HTTP/2
/// connection, so cloning is cheap and concurrent calls multiplex on the same
/// underlying transport.
pub(crate) struct FlightQueryClient {
    inner: FlightServiceClient<Channel>,
    token: Option<String>,
    auth_scheme: String,
}

impl FlightQueryClient {
    pub(crate) async fn new(
        host_url: &str,
        token: Option<&str>,
        auth_scheme: &str,
        ssl_roots_path: Option<&str>,
        connect_timeout: Duration,
    ) -> Result<Self, Error> {
        let parsed = Url::parse(host_url)?;
        let tls = parsed.scheme() == "https";

        let host_str = parsed
            .host_str()
            .ok_or_else(|| Error::Config(format!("no host in URL: {host_url}")))?;
        let port = parsed.port().unwrap_or(if tls { 443 } else { 80 });

        let endpoint_url = if tls {
            format!("https://{host_str}:{port}")
        } else {
            format!("http://{host_str}:{port}")
        };

        let endpoint: Endpoint = Channel::from_shared(endpoint_url)
            .map_err(|e| Error::Config(e.to_string()))?
            .connect_timeout(connect_timeout);

        let endpoint = if tls {
            let mut tls_config = ClientTlsConfig::new().with_native_roots();
            if let Some(path) = ssl_roots_path {
                let pem = std::fs::read(path)
                    .map_err(|e| Error::Config(format!("cannot read SSL roots '{path}': {e}")))?;
                let cert = tonic::transport::Certificate::from_pem(pem);
                tls_config = tls_config.ca_certificate(cert);
            }
            endpoint.tls_config(tls_config)?
        } else {
            endpoint
        };

        let channel = endpoint.connect().await?;
        let inner = FlightServiceClient::new(channel);

        Ok(FlightQueryClient {
            inner,
            token: token.map(str::to_owned),
            auth_scheme: auth_scheme.to_owned(),
        })
    }

    /// Open a streaming query and return a [`BatchStream`].
    ///
    /// Clones the underlying gRPC client per call; `Channel` is `Arc`-backed, so
    /// concurrent queries multiplex on the same connection.
    pub(crate) async fn stream(
        &self,
        query_str: &str,
        database: &str,
        options: &QueryOptions,
        params: Option<&HashMap<String, JsonValue>>,
    ) -> Result<BatchStream, Error> {
        let ticket_payload = build_ticket(query_str, database, options, params);
        let ticket = Ticket {
            ticket: Bytes::from(ticket_payload),
        };

        let mut request = Request::new(ticket);

        if let Some(tok) = &self.token {
            let auth_value = format!("{} {}", self.auth_scheme, tok);
            let meta: MetadataValue<tonic::metadata::Ascii> = auth_value.parse().map_err(|_| {
                Error::Config("token contains characters invalid in gRPC metadata".into())
            })?;
            request.metadata_mut().insert("authorization", meta);
        }

        for (k, v) in &options.headers {
            if let (Ok(name), Ok(val)) = (
                tonic::metadata::MetadataKey::<tonic::metadata::Ascii>::from_bytes(k.as_bytes()),
                v.parse::<MetadataValue<tonic::metadata::Ascii>>(),
            ) {
                request.metadata_mut().insert(name, val);
            }
        }

        // Channel is Arc-backed, so cloning the client is cheap.
        let mut client = self.inner.clone();
        let response = client.do_get(request).await?;
        let raw = response.into_inner();
        let batch_stream = FlightRecordBatchStream::new_from_flight_data(
            raw.map_err(|status| arrow_flight::error::FlightError::Tonic(Box::new(status))),
        )
        .map_err(|e| Error::Arrow(arrow::error::ArrowError::ExternalError(Box::new(e))));

        Ok(BatchStream {
            inner: Box::pin(batch_stream),
        })
    }

    /// Execute a query and collect all batches.
    pub(crate) async fn query(
        &self,
        query_str: &str,
        database: &str,
        options: &QueryOptions,
        params: Option<&HashMap<String, JsonValue>>,
    ) -> Result<QueryResult, Error> {
        let mut stream = self.stream(query_str, database, options, params).await?;

        let mut schema: Option<SchemaRef> = None;
        let mut batches: Vec<RecordBatch> = Vec::new();

        while let Some(batch) = stream.try_next().await? {
            if schema.is_none() {
                schema = Some(batch.schema());
            }
            batches.push(batch);
        }

        let schema = schema.unwrap_or_else(|| std::sync::Arc::new(arrow_schema::Schema::empty()));

        Ok(QueryResult { schema, batches })
    }
}

/// Streaming iterator over query result [`RecordBatch`]es.
///
/// Use this when the result is too large to materialise in memory. The
/// underlying gRPC stream is consumed lazily as you poll.
///
/// ```rust,no_run
/// # use influxdb3_client::Client;
/// # use futures_util::TryStreamExt;
/// # async fn example(client: &Client) -> influxdb3_client::Result<()> {
/// let mut stream = client.sql("SELECT * FROM huge_table").stream().await?;
/// while let Some(batch) = stream.try_next().await? {
///     println!("got {} rows", batch.num_rows());
/// }
/// # Ok(()) }
/// ```
pub struct BatchStream {
    inner: Pin<Box<dyn Stream<Item = Result<RecordBatch, Error>> + Send>>,
}

impl Stream for BatchStream {
    type Item = Result<RecordBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

/// Build the JSON ticket that InfluxDB 3 expects on its Flight `DoGet` endpoint.
fn build_ticket(
    query_str: &str,
    database: &str,
    options: &QueryOptions,
    params: Option<&HashMap<String, JsonValue>>,
) -> Vec<u8> {
    let mut ticket = json!({
        "database": database,
        "sql_query": query_str,
        "query_type": options.query_type.as_str(),
    });

    if let Some(p) = params {
        if !p.is_empty() {
            ticket["params"] = json!(p);
        }
    }

    ticket.to_string().into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{QueryOptions, QueryType};

    #[test]
    fn ticket_shape() {
        // Default SQL
        let t = build_ticket("SELECT 1", "mydb", &QueryOptions::default(), None);
        let v: serde_json::Value = serde_json::from_slice(&t).unwrap();
        assert_eq!(v["database"], "mydb");
        assert_eq!(v["sql_query"], "SELECT 1");
        assert_eq!(v["query_type"], "sql");
        assert!(v.get("params").is_none());

        // InfluxQL + params
        let opts = QueryOptions {
            query_type: QueryType::InfluxQL,
            ..Default::default()
        };
        let mut p = HashMap::new();
        p.insert("loc".into(), json!("Paris"));
        let t = build_ticket("SHOW MEASUREMENTS", "db", &opts, Some(&p));
        let v: serde_json::Value = serde_json::from_slice(&t).unwrap();
        assert_eq!(v["query_type"], "influxql");
        assert_eq!(v["params"]["loc"], "Paris");
    }
}
