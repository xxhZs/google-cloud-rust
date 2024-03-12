use backon::{ExponentialBuilder, Retryable};
use google_cloud_googleapis::cloud::bigquery::storage::v1::write_stream::Type;
use core::time::Duration;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

use google_cloud_gax::conn::{ConnectionOptions, Environment};
use google_cloud_gax::retry::RetrySetting;
use google_cloud_googleapis::cloud::bigquery::storage::v1::{
    read_session, CreateReadSessionRequest, DataFormat, ReadSession, TableSchema,
};
use google_cloud_token::TokenSourceProvider;

use crate::grpc::apiv1::bigquery_client::StreamingWriteClient;
use crate::grpc::apiv1::conn_pool::{ReadConnectionManager, DOMAIN};
use crate::http::bigquery_client::BigqueryClient;
use crate::http::bigquery_dataset_client::BigqueryDatasetClient;
use crate::http::bigquery_job_client::BigqueryJobClient;
use crate::http::bigquery_model_client::BigqueryModelClient;
use crate::http::bigquery_routine_client::BigqueryRoutineClient;
use crate::http::bigquery_row_access_policy_client::BigqueryRowAccessPolicyClient;
use crate::http::bigquery_table_client::BigqueryTableClient;
use crate::http::bigquery_tabledata_client::BigqueryTabledataClient;
use crate::http::job::get_query_results::GetQueryResultsRequest;
use crate::http::job::query::QueryRequest;
use crate::http::job::{is_script, is_select_query, JobConfiguration, JobReference, JobStatistics, JobType};
use crate::http::table::TableReference;
use crate::query::{QueryOption, QueryResult};
use crate::storage::{self, Writer};
use crate::{http, query};
use crate::grpc::apiv1::conn_pool::WriteConnectionManager;

const JOB_RETRY_REASONS: [&str; 3] = ["backendError", "rateLimitExceeded", "internalError"];

#[derive(Debug)]
pub struct ClientConfig {
    http: reqwest_middleware::ClientWithMiddleware,
    bigquery_endpoint: Cow<'static, str>,
    token_source_provider: Box<dyn TokenSourceProvider>,
    environment: Environment,
    streaming_read_config: ChannelConfig,
    debug: bool,
}

#[derive(Clone, Debug)]
pub struct ChannelConfig {
    /// num_channels is the number of gRPC channels.
    pub num_channels: usize,
    pub connect_timeout: Option<Duration>,
    pub timeout: Option<Duration>,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            num_channels: 4,
            connect_timeout: Some(Duration::from_secs(30)),
            timeout: None,
        }
    }
}

impl ClientConfig {
    pub fn new(
        http_token_source_provider: Box<dyn TokenSourceProvider>,
        grpc_token_source_provider: Box<dyn TokenSourceProvider>,
    ) -> Self {
        Self {
            http: reqwest_middleware::ClientBuilder::new(reqwest::Client::default()).build(),
            bigquery_endpoint: "https://bigquery.googleapis.com".into(),
            token_source_provider: http_token_source_provider,
            environment: Environment::GoogleCloud(grpc_token_source_provider),
            streaming_read_config: ChannelConfig::default(),
            debug: false,
        }
    }
    pub fn with_debug(mut self, value: bool) -> Self {
        self.debug = value;
        self
    }
    pub fn with_streaming_read_config(mut self, value: ChannelConfig) -> Self {
        self.streaming_read_config = value;
        self
    }
    pub fn with_http_client(mut self, value: reqwest_middleware::ClientWithMiddleware) -> Self {
        self.http = value;
        self
    }
    pub fn with_endpoint(mut self, value: impl Into<Cow<'static, str>>) -> Self {
        self.bigquery_endpoint = value.into();
        self
    }
}

use crate::http::job::get::GetJobRequest;
use crate::http::job::list::ListJobsRequest;

#[cfg(feature = "auth")]
pub use google_cloud_auth;

#[cfg(feature = "auth")]
impl ClientConfig {
    pub async fn new_with_auth() -> Result<(Self, Option<String>), google_cloud_auth::error::Error> {
        let ts_http =
            google_cloud_auth::token::DefaultTokenSourceProvider::new(Self::bigquery_http_auth_config()).await?;
        let ts_grpc =
            google_cloud_auth::token::DefaultTokenSourceProvider::new(Self::bigquery_grpc_auth_config()).await?;
        let project_id = ts_grpc.project_id.clone();
        let config = Self::new(Box::new(ts_http), Box::new(ts_grpc));
        Ok((config, project_id))
    }

    pub async fn new_with_credentials(
        credentials: google_cloud_auth::credentials::CredentialsFile,
    ) -> Result<(Self, Option<String>), google_cloud_auth::error::Error> {
        let ts_http = google_cloud_auth::token::DefaultTokenSourceProvider::new_with_credentials(
            Self::bigquery_http_auth_config(),
            Box::new(credentials.clone()),
        )
        .await?;
        let ts_grpc = google_cloud_auth::token::DefaultTokenSourceProvider::new_with_credentials(
            Self::bigquery_grpc_auth_config(),
            Box::new(credentials),
        )
        .await?;
        let project_id = ts_grpc.project_id.clone();
        let config = Self::new(Box::new(ts_http), Box::new(ts_grpc));
        Ok((config, project_id))
    }

    fn bigquery_http_auth_config() -> google_cloud_auth::project::Config<'static> {
        google_cloud_auth::project::Config {
            audience: None,
            scopes: Some(&crate::http::bigquery_client::SCOPES),
            sub: None,
        }
    }

    fn bigquery_grpc_auth_config() -> google_cloud_auth::project::Config<'static> {
        google_cloud_auth::project::Config {
            audience: Some(crate::grpc::apiv1::conn_pool::AUDIENCE),
            scopes: Some(&crate::grpc::apiv1::conn_pool::SCOPES),
            sub: None,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum QueryError {
    #[error(transparent)]
    Storage(#[from] storage::Error),
    #[error(transparent)]
    JobHttp(#[from] http::error::Error),
    #[error("job has no destination table to read : job={0:?}")]
    NoDestinationTable(JobReference),
    #[error("failed to resolve table for script job: no child jobs found : job={0:?}")]
    NoChildJobs(JobReference),
    #[error("job type must be query: job={0:?}, jobType={1:?}")]
    InvalidJobType(JobReference, String),
    #[error(transparent)]
    RunQuery(#[from] query::run::Error),
}

#[derive(Clone)]
pub struct Client {
    dataset_client: BigqueryDatasetClient,
    table_client: BigqueryTableClient,
    tabledata_client: BigqueryTabledataClient,
    job_client: BigqueryJobClient,
    routine_client: BigqueryRoutineClient,
    row_access_policy_client: BigqueryRowAccessPolicyClient,
    model_client: BigqueryModelClient,
    streaming_read_client_conn_pool: Arc<ReadConnectionManager>,
    streaming_write_client_conn_pool: Arc<WriteConnectionManager>,
}

impl Client {
    /// New client
    pub async fn new(config: ClientConfig) -> Result<Self, google_cloud_gax::conn::Error> {
        let ts = config.token_source_provider.token_source();
        let client = Arc::new(BigqueryClient::new(
            ts,
            config.bigquery_endpoint.into_owned().as_str(),
            config.http,
            config.debug,
        ));

        let read_config = config.streaming_read_config;
        let conn_options = ConnectionOptions {
            timeout: read_config.timeout,
            connect_timeout: read_config.connect_timeout,
        };

        let streaming_read_client_conn_pool =
            ReadConnectionManager::new(read_config.num_channels, &config.environment, DOMAIN, &conn_options).await?;
        let streaming_write_client_conn_pool= WriteConnectionManager::new(read_config.num_channels, &config.environment, DOMAIN, &conn_options).await?;
        Ok(Self {
            dataset_client: BigqueryDatasetClient::new(client.clone()),
            table_client: BigqueryTableClient::new(client.clone()),
            tabledata_client: BigqueryTabledataClient::new(client.clone()),
            job_client: BigqueryJobClient::new(client.clone()),
            routine_client: BigqueryRoutineClient::new(client.clone()),
            row_access_policy_client: BigqueryRowAccessPolicyClient::new(client.clone()),
            model_client: BigqueryModelClient::new(client.clone()),
            streaming_read_client_conn_pool: Arc::new(streaming_read_client_conn_pool),
            streaming_write_client_conn_pool: Arc::new(streaming_write_client_conn_pool),
        })
    }

    /// https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets
    /// [BigqueryDatasetClient](crate::http::bigquery_dataset_client::BigqueryDatasetClient)
    pub fn dataset(&self) -> &BigqueryDatasetClient {
        &self.dataset_client
    }

    /// https://cloud.google.com/bigquery/docs/reference/rest/v2/tables
    /// [BigqueryTableClient](crate::http::bigquery_table_client::BigqueryTableClient)
    pub fn table(&self) -> &BigqueryTableClient {
        &self.table_client
    }

    /// https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata
    /// [BigqueryTabledataClient](crate::http::bigquery_tabledata_client::BigqueryTabledataClient)
    pub fn tabledata(&self) -> &BigqueryTabledataClient {
        &self.tabledata_client
    }

    /// https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs
    /// [BigqueryJobClient](crate::http::bigquery_job_client::BigqueryJobClient)
    pub fn job(&self) -> &BigqueryJobClient {
        &self.job_client
    }

    /// https://cloud.google.com/bigquery/docs/reference/rest/v2/routines
    /// [BigqueryRoutineClient](crate::http::bigquery_routine_client::BigqueryRoutineClient)
    pub fn routine(&self) -> &BigqueryRoutineClient {
        &self.routine_client
    }

    /// https://cloud.google.com/bigquery/docs/reference/rest/v2/rowAccessPolicy
    /// [BigqueryRowAccessPolicyClient](crate::http::bigquery_row_access_policy_client::BigqueryRowAccessPolicyClient)
    pub fn row_access_policy(&self) -> &BigqueryRowAccessPolicyClient {
        &self.row_access_policy_client
    }

    /// https://cloud.google.com/bigquery/docs/reference/rest/v2/models
    /// [BigqueryModelClient](crate::http::bigquery_model_client::BigqueryModelClient)
    pub fn model(&self) -> &BigqueryModelClient {
        &self.model_client
    }

    /// Run query job and get result.
    /// ```rust
    /// use google_cloud_bigquery::http::job::query::QueryRequest;
    /// use google_cloud_bigquery::query::row::Row;
    /// use google_cloud_bigquery::client::Client;
    ///
    /// async fn run(client: &Client, project_id: &str) {
    ///     let request = QueryRequest {
    ///         query: "SELECT * FROM dataset.table".to_string(),
    ///         ..Default::default()
    ///     };
    ///     let mut iter = client.query::<Row>(project_id, request).await.unwrap();
    ///     while let Some(row) = iter.next().await.unwrap() {
    ///         let col1 = row.column::<String>(0);
    ///         let col2 = row.column::<Option<String>>(1);
    ///     }
    /// }
    pub async fn query<T>(&self, project_id: &str, request: QueryRequest) -> Result<query::Iterator<T>, QueryError>
    where
        T: http::query::value::StructDecodable + storage::value::StructDecodable,
    {
        self.query_with_option(project_id, request, QueryOption::default())
            .await
    }

    /// Run query job and get result.
    /// ```rust
    /// use google_cloud_bigquery::http::job::query::QueryRequest;
    /// use google_cloud_bigquery::query::row::Row;
    /// use google_cloud_bigquery::client::Client;
    /// use google_cloud_bigquery::query::QueryOption;
    /// use google_cloud_bigquery::query::ExponentialBuilder;
    ///
    /// async fn run(client: &Client, project_id: &str) {
    ///     let request = QueryRequest {
    ///         query: "SELECT * FROM dataset.table".to_string(),
    ///         ..Default::default()
    ///     };
    ///     let retry = ExponentialBuilder::default().with_max_times(10);
    ///     let option = QueryOption::default().with_retry(retry).with_enable_storage_read(true);
    ///     let mut iter = client.query_with_option::<Row>(project_id, request, option).await.unwrap();
    ///     while let Some(row) = iter.next().await.unwrap() {
    ///         let col1 = row.column::<String>(0);
    ///         let col2 = row.column::<Option<String>>(1);
    ///     }
    /// }
    pub async fn query_with_option<T>(
        &self,
        project_id: &str,
        request: QueryRequest,
        option: QueryOption,
    ) -> Result<query::Iterator<T>, QueryError>
    where
        T: http::query::value::StructDecodable + storage::value::StructDecodable,
    {
        let result = self.job_client.query(project_id, &request).await?;
        let (total_rows, page_token, rows, force_first_fetch) = if result.job_complete {
            (
                result.total_rows.unwrap_or_default(),
                result.page_token,
                result.rows.unwrap_or_default(),
                false,
            )
        } else {
            (
                self.wait_for_query(&result.job_reference, &option.retry, &request.timeout_ms)
                    .await?,
                None,
                vec![],
                true,
            )
        };

        //use storage api instead of rest API
        if option.enable_storage_read && (page_token.is_none() || page_token.as_ref().unwrap().is_empty()) {
            tracing::trace!("use storage read api for query {:?}", result.job_reference);
            let job = self
                .job_client
                .get(
                    &result.job_reference.project_id,
                    &result.job_reference.job_id,
                    &GetJobRequest {
                        location: result.job_reference.location.clone(),
                    },
                )
                .await?;
            let iter = self
                .new_storage_row_iterator_from_job::<T>(job.job_reference, job.statistics, job.configuration)
                .await?;
            return Ok(query::Iterator {
                inner: QueryResult::Storage(iter),
                total_size: total_rows,
            });
        }

        let http_query_iterator = http::query::Iterator {
            client: self.job_client.clone(),
            project_id: result.job_reference.project_id,
            job_id: result.job_reference.job_id,
            request: GetQueryResultsRequest {
                start_index: 0,
                page_token,
                max_results: request.max_results,
                timeout_ms: request.timeout_ms,
                location: result.job_reference.location,
                format_options: request.format_options,
            },
            chunk: VecDeque::from(rows),
            total_size: total_rows,
            force_first_fetch,
            _marker: PhantomData,
        };
        Ok(query::Iterator {
            inner: QueryResult::Http(http_query_iterator),
            total_size: total_rows,
        })
    }

    async fn new_storage_row_iterator_from_job<T>(
        &self,
        mut job: JobReference,
        mut statistics: Option<JobStatistics>,
        mut config: JobConfiguration,
    ) -> Result<storage::Iterator<T>, QueryError>
    where
        T: http::query::value::StructDecodable + storage::value::StructDecodable,
    {
        loop {
            tracing::trace!("check child job result {:?}, {:?}, {:?}", job, statistics, config);
            let query_config = match &config.job {
                JobType::Query(config) => config,
                _ => return Err(QueryError::InvalidJobType(job.clone(), config.job_type.clone())),
            };
            if let Some(dst) = &query_config.destination_table {
                return Ok(self.read_table(dst, None).await?);
            }
            if !is_script(&statistics, &config) {
                return Err(QueryError::NoDestinationTable(job.clone()));
            }
            let children = self
                .job_client
                .list(
                    &job.project_id,
                    &ListJobsRequest {
                        parent_job_id: job.job_id.to_string(),
                        ..Default::default()
                    },
                )
                .await?;

            let mut found = false;
            for j in children.into_iter() {
                if !is_select_query(&j.statistics, &j.configuration) {
                    continue;
                }
                job = j.job_reference;
                statistics = j.statistics;
                config = j.configuration;
                found = true;
                break;
            }
            if !found {
                break;
            }
        }
        Err(QueryError::NoChildJobs(job.clone()))
    }

    async fn wait_for_query(
        &self,
        job: &JobReference,
        builder: &ExponentialBuilder,
        timeout_ms: &Option<i64>,
    ) -> Result<i64, query::run::Error> {
        // Use get_query_results only to wait for completion, not to read results.
        let request = GetQueryResultsRequest {
            max_results: Some(0),
            timeout_ms: *timeout_ms,
            location: job.location.clone(),
            ..Default::default()
        };
        let action = || async {
            tracing::debug!("waiting for job completion {:?}", job);
            let result = self
                .job_client
                .get_query_results(&job.project_id, &job.job_id, &request)
                .await
                .map_err(query::run::Error::Http)?;
            if result.job_complete {
                Ok(result.total_rows)
            } else {
                Err(query::run::Error::JobIncomplete)
            }
        };
        action
            .retry(builder)
            .when(|e: &query::run::Error| match e {
                query::run::Error::JobIncomplete => true,
                query::run::Error::Http(http::error::Error::HttpClient(_)) => true,
                query::run::Error::Http(http::error::Error::Response(r)) => r.is_retryable(&JOB_RETRY_REASONS),
                _ => false,
            })
            .await
    }

    /// Read table data by BigQuery Storage Read API.
    /// ```rust
    /// use google_cloud_bigquery::storage::row::Row;
    /// use google_cloud_bigquery::client::Client;
    /// use google_cloud_bigquery::http::table::TableReference;
    ///
    /// async fn run(client: &Client, project_id: &str) {
    ///     let table = TableReference {
    ///         project_id: project_id.to_string(),
    ///         dataset_id: "dataset".to_string(),
    ///         table_id: "table".to_string(),
    ///     };
    ///     let mut iter = client.read_table::<Row>(&table, None).await.unwrap();
    ///     while let Some(row) = iter.next().await.unwrap() {
    ///         let col1 = row.column::<String>(0);
    ///         let col2 = row.column::<Option<String>>(1);
    ///     }
    /// }
    /// ```
    pub async fn read_table<T>(
        &self,
        table: &TableReference,
        option: Option<ReadTableOption>,
    ) -> Result<storage::Iterator<T>, storage::Error>
    where
        T: storage::value::StructDecodable,
    {
        let option = option.unwrap_or_default();

        let mut client = self.streaming_read_client_conn_pool.conn();
        let read_session = client
            .create_read_session(
                CreateReadSessionRequest {
                    parent: format!("projects/{}", table.project_id),
                    read_session: Some(ReadSession {
                        name: "".to_string(),
                        expire_time: None,
                        data_format: DataFormat::Arrow.into(),
                        table: table.resource(),
                        table_modifiers: option.session_table_modifiers,
                        read_options: option.session_read_options,
                        streams: vec![],
                        estimated_total_bytes_scanned: 0,
                        estimated_total_physical_file_size: 0,
                        estimated_row_count: 0,
                        trace_id: "".to_string(),
                        schema: option.session_schema,
                    }),
                    max_stream_count: 0,
                    preferred_min_stream_count: 0,
                },
                option.session_retry_setting,
            )
            .await?
            .into_inner();
        storage::Iterator::new(client, read_session, option.read_rows_retry_setting).await
    }

    pub fn writer_for_table(&self, table: TableReference,
        write_type: Type,
        table_schema: TableSchema,
        location: String,
        retry: Option<RetrySetting>
    ) -> Writer {
        let client = self.streaming_write_client_conn_pool.conn();
        Writer::new(client, table, write_type, table_schema, location, retry)
    }
}

#[derive(Debug, Default, Clone)]
pub struct ReadTableOption {
    session_read_options: Option<read_session::TableReadOptions>,
    session_table_modifiers: Option<read_session::TableModifiers>,
    session_schema: Option<read_session::Schema>,
    session_retry_setting: Option<RetrySetting>,
    read_rows_retry_setting: Option<RetrySetting>,
}

impl ReadTableOption {
    pub fn with_session_read_options(mut self, value: read_session::TableReadOptions) -> Self {
        self.session_read_options = Some(value);
        self
    }

    pub fn with_session_table_modifiers(mut self, value: read_session::TableModifiers) -> Self {
        self.session_table_modifiers = Some(value);
        self
    }

    pub fn with_session_schema(mut self, value: read_session::Schema) -> Self {
        self.session_schema = Some(value);
        self
    }

    pub fn with_session_retry_setting(mut self, value: RetrySetting) -> Self {
        self.session_retry_setting = Some(value);
        self
    }

    pub fn with_read_rows_retry_setting(mut self, value: RetrySetting) -> Self {
        self.read_rows_retry_setting = Some(value);
        self
    }
}

#[cfg(test)]
mod tests {
    use bigdecimal::BigDecimal;
    use google_cloud_auth::credentials::CredentialsFile;
    use google_cloud_gax::retry::RetrySetting;
    use google_cloud_googleapis::cloud::bigquery::storage::v1::write_stream::Type;
    use google_cloud_googleapis::cloud::bigquery::storage::v1::{table_field_schema, ProtoRows, ProtoSchema, TableFieldSchema, TableSchema};
    use prost_types::{field_descriptor_proto, DescriptorProto, FieldDescriptorProto};
    use serial_test::serial;
    use std::ops::AddAssign;
    use std::time::Duration;

    use time::{Date, OffsetDateTime, Time};

    use google_cloud_googleapis::cloud::bigquery::storage::v1::read_session::TableReadOptions;

    use crate::client::{Client, ClientConfig, ReadTableOption};
    use crate::http::bigquery_client::test::{create_table_schema, dataset_name, TestData};
    use crate::http::job::query::QueryRequest;
    use crate::http::table::{Table, TableFieldType, TableReference};
    use crate::http::tabledata::insert_all::{InsertAllRequest, Row};
    use crate::query;
    use crate::query::QueryOption;
    use google_cloud_googleapis::cloud::bigquery::storage::v1::append_rows_request::{ProtoData, Rows as AppendRowsRequestRows};

    #[ctor::ctor]
    fn init() {
        let _ = tracing_subscriber::fmt::try_init();
    }

    async fn create_client() -> (Client, String) {
        let (client_config, project_id) = ClientConfig::new_with_auth().await.unwrap();
        (Client::new(client_config).await.unwrap(), project_id.unwrap())
    }

    #[tokio::test]
    #[serial]
    async fn test_query_from_storage() {
        let option = QueryOption::default().with_enable_storage_read(true);
        test_query(option).await
    }

    #[tokio::test]
    async fn test_writer() {
        let credentials = CredentialsFile::new_from_file("/home/xxhx/Downloads/winter-dynamics-383822-cbec26f21d33.json".to_string()).await.unwrap();
        let config = ClientConfig::new_with_credentials(credentials).await.unwrap().0;
        // table: TableReference,
        // write_type: Type,
        // table_schema: TableSchema,
        // location: String,
        // retry: Option<RetrySetting>winter-dynamics-383822.test_bigquery_sink.t5
        let table = TableReference{
            project_id: "winter-dynamics-383822".to_string(),
            dataset_id: "test_bigquery_sink".to_string(),
            table_id: "t3".to_string()
        };
        let table_schema = TableSchema{
            fields:vec![TableFieldSchema {
                name: "v1".to_string(),
                r#type: table_field_schema::Type::Int64.into(),
                ..Default::default()
            },
            TableFieldSchema {
                name: "v2".to_string(),
                r#type: table_field_schema::Type::Int64.into(),
                ..Default::default()
            },
            TableFieldSchema {
                name: "v3".to_string(),
                r#type: table_field_schema::Type::Int64.into(),
                ..Default::default()
            },
            TableFieldSchema {
                name: "_CHANGE_TYPE".to_string(),
                r#type: table_field_schema::Type::String.into(),
                ..Default::default()
            }
            ]
        };
        let mut writer = Client::new(config).await.unwrap().writer_for_table(table,Type::Committed,table_schema,"asia-southeast1".to_string(),Some(RetrySetting::default()));
        let a = writer.create_write_stream("aaa".to_string()).await.unwrap().into_inner();
        println!("{:?}",a);

        let mut field1 = FieldDescriptorProto::default();
        field1.name = Some("v1".to_string());
        field1.number = Some(2);
        field1.r#type =Some(field_descriptor_proto::Type::Int64.into()); 

        let mut field2 = FieldDescriptorProto::default();
        field2.name = Some("v2".to_string());
        field2.number = Some(3);
        field2.r#type = Some(field_descriptor_proto::Type::Int64.into()); 

        let mut field3 = FieldDescriptorProto::default();
        field3.name = Some("v3".to_string());
        field3.number = Some(4);
        field3.r#type = Some(field_descriptor_proto::Type::Int64.into()); 

        let mut field4 = FieldDescriptorProto::default();
        field4.name = Some("_CHANGE_TYPE".to_string());
        field4.number = Some(5);
        field4.r#type = Some(field_descriptor_proto::Type::String.into()); 

        let mut descriptor = DescriptorProto::default();
        descriptor.name = Some("t3".to_string());
        descriptor.field.push(field1);
        descriptor.field.push(field2);
        descriptor.field.push(field3);
        descriptor.field.push(field4);

        let mut proto_rows =vec![];
        let mut buf = Vec::new();
        protobuf::CodedOutputStream::vec(&mut buf).write_int32(2, 1).unwrap();
        protobuf::CodedOutputStream::vec(&mut buf).write_int32(3, 2).unwrap();
        protobuf::CodedOutputStream::vec(&mut buf).write_int32(4, 3).unwrap();
        protobuf::CodedOutputStream::vec(&mut buf).write_string(5, "DELETE").unwrap();
        proto_rows.push(buf);
        let mut buf = Vec::new();
        protobuf::CodedOutputStream::vec(&mut buf).write_int32(2, 21).unwrap();
        protobuf::CodedOutputStream::vec(&mut buf).write_int32(3, 22).unwrap();
        protobuf::CodedOutputStream::vec(&mut buf).write_int32(4, 23).unwrap();
        protobuf::CodedOutputStream::vec(&mut buf).write_string(5, "DELETE").unwrap();
        proto_rows.push(buf);
        let pp = ProtoRows{
            serialized_rows:proto_rows
        };
        let schema = ProtoSchema{proto_descriptor:Some(descriptor)};

        let rows = AppendRowsRequestRows::ProtoRows(ProtoData { writer_schema: Some(schema), rows: Some(pp) });
        let b = writer.append_rows(vec![rows],a.name.clone()).await.unwrap();
        println!("{:?}",b);
        for i in b.into_inner().message().await.unwrap(){
            println!("saed{:?}",i);
        }
        // for i in b.into_inner().
        let b = writer.finalize_write_stream(a.name).await.unwrap();
        println!("{:?}",b);
    }

    #[tokio::test]
    #[serial]
    async fn test_query_from_rest() {
        let option = QueryOption::default();
        test_query(option).await
    }

    async fn test_query(option: QueryOption) {
        let (client, project_id) = create_client().await;
        let mut iterator = client
            .query_with_option::<query::row::Row>(
                &project_id,
                QueryRequest {
                    max_results: Some(2),
                    query: "SELECT
                        'A',
                        TIMESTAMP_MICROS(1230219000000019),
                        100,
                        0.432899,
                        DATE(2023,9,1),
                        TIME(15, 30, 01),
                        NULL,
                        ['A','B'],
                        [TIMESTAMP_MICROS(1230219000000019), TIMESTAMP_MICROS(1230219000000020)],
                        [100,200],
                        [0.432899,0.432900],
                        [DATE(2023,9,1),DATE(2023,9,2)],
                        [TIME_ADD(TIME(15,30,1), INTERVAL 10 MICROSECOND),TIME(0, 0, 0),TIME(23,59,59)],
                        b'test',
                        true,
                        [b'test',b'test2'],
                        [true,false],
                        cast('-5.7896044618658097711785492504343953926634992332820282019728792003956564819968E+38' as BIGNUMERIC),
                        cast('5.7896044618658097711785492504343953926634992332820282019728792003956564819967E+38' as BIGNUMERIC),
                        cast('-9.9999999999999999999999999999999999999E+28' as NUMERIC),
                        cast('9.9999999999999999999999999999999999999E+28' as NUMERIC),
                        [cast('-5.7896044618658097711785492504343953926634992332820282019728792003956564819968E+38' as BIGNUMERIC),cast('5.7896044618658097711785492504343953926634992332820282019728792003956564819967E+38' as BIGNUMERIC)]
                    ".to_string(),
                    ..Default::default()
                },
                option,
            )
            .await
            .unwrap();

        assert_eq!(1, iterator.total_size);

        while let Some(row) = iterator.next().await.unwrap() {
            let v: String = row.column(0).unwrap();
            assert_eq!(v, "A");
            let v: OffsetDateTime = row.column(1).unwrap();
            assert_eq!(v.unix_timestamp_nanos(), 1230219000000019000);
            let v: i64 = row.column(2).unwrap();
            assert_eq!(v, 100);
            let v: f64 = row.column(3).unwrap();
            assert_eq!(v, 0.432899);
            let v: Date = row.column(4).unwrap();
            assert_eq!(v, time::macros::date!(2023 - 09 - 01));
            let v: Time = row.column(5).unwrap();
            assert_eq!(v, time::macros::time!(15:30:01));
            let v: Option<String> = row.column(6).unwrap();
            assert!(v.is_none());
            let v: Option<OffsetDateTime> = row.column(6).unwrap();
            assert!(v.is_none());
            let v: Option<i64> = row.column(6).unwrap();
            assert!(v.is_none());
            let v: Option<f64> = row.column(6).unwrap();
            assert!(v.is_none());
            let v: Option<Date> = row.column(6).unwrap();
            assert!(v.is_none());
            let v: Option<Time> = row.column(6).unwrap();
            assert!(v.is_none());
            let v: Option<Vec<Time>> = row.column(6).unwrap();
            assert!(v.is_none());
            let v: Option<BigDecimal> = row.column(6).unwrap();
            assert!(v.is_none());
            let v: Option<bool> = row.column(6).unwrap();
            assert!(v.is_none());
            let v: Option<String> = row.column(6).unwrap();
            assert!(v.is_none());
            let v: Option<Vec<u8>> = row.column(6).unwrap();
            assert!(v.is_none());

            let v: Vec<String> = row.column(7).unwrap();
            assert_eq!(v, vec!["A", "B"]);
            let v: Vec<OffsetDateTime> = row.column(8).unwrap();
            assert_eq!(v[0].unix_timestamp_nanos(), 1230219000000019000);
            assert_eq!(v[1].unix_timestamp_nanos(), 1230219000000020000);
            let v: Vec<i64> = row.column(9).unwrap();
            assert_eq!(v, vec![100, 200]);
            let v: Vec<f64> = row.column(10).unwrap();
            assert_eq!(v, vec![0.432899, 0.432900]);
            let v: Vec<Date> = row.column(11).unwrap();
            assert_eq!(v[0], time::macros::date!(2023 - 09 - 01));
            assert_eq!(v[1], time::macros::date!(2023 - 09 - 02));
            let v: Vec<Time> = row.column(12).unwrap();
            let mut tm = time::macros::time!(15:30:01);
            tm.add_assign(Duration::from_micros(10));
            assert_eq!(v[0], tm);
            assert_eq!(v[1], time::macros::time!(0:0:0));
            assert_eq!(v[2], time::macros::time!(23:59:59));

            let v: Vec<u8> = row.column(13).unwrap();
            assert_eq!(v, b"test");
            let v: bool = row.column(14).unwrap();
            assert!(v);
            let v: Vec<Vec<u8>> = row.column(15).unwrap();
            assert_eq!(v[0], b"test");
            assert_eq!(v[1], b"test2");
            let v: Vec<bool> = row.column(16).unwrap();
            assert!(v[0]);
            assert!(!v[1]);
            let v: BigDecimal = row.column(17).unwrap();
            assert_eq!(
                v.to_string(),
                "-578960446186580977117854925043439539266.34992332820282019728792003956564819968"
            );
            let v: BigDecimal = row.column(18).unwrap();
            assert_eq!(
                v.to_string(),
                "578960446186580977117854925043439539266.34992332820282019728792003956564819967"
            );
            let v: BigDecimal = row.column(19).unwrap();
            assert_eq!(v.to_string(), "-99999999999999999999999999999.999999999");
            let v: BigDecimal = row.column(20).unwrap();
            assert_eq!(v.to_string(), "99999999999999999999999999999.999999999");
            let v: Vec<BigDecimal> = row.column(21).unwrap();
            assert_eq!(
                v[0].to_string(),
                "-578960446186580977117854925043439539266.34992332820282019728792003956564819968"
            );
            assert_eq!(
                v[1].to_string(),
                "578960446186580977117854925043439539266.34992332820282019728792003956564819967"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_query_table_from_storage() {
        test_query_table(None, QueryOption::default().with_enable_storage_read(true)).await
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_query_table_from_rest() {
        test_query_table(Some(1), QueryOption::default()).await
    }

    async fn insert(client: &Client, project: &str, dataset: &str, table: &str, size: usize, now: &OffsetDateTime) {
        let mut table1 = Table::default();
        table1.table_reference.dataset_id = dataset.to_string();
        table1.table_reference.project_id = project.to_string();
        table1.table_reference.table_id = table.to_string();
        table1.schema = Some(create_table_schema());
        let _table1 = client.table_client.create(&table1).await.unwrap();
        let mut req = InsertAllRequest::<TestData>::default();
        for i in 0..size {
            req.rows.push(Row {
                insert_id: None,
                json: TestData::default(i, *now + Duration::from_secs(i as u64)),
            });
        }
        client.tabledata().insert(project, dataset, table, &req).await.unwrap();
    }

    async fn test_query_table(max_results: Option<i64>, option: QueryOption) {
        let dataset = dataset_name("table");
        let (client, project_id) = create_client().await;
        let now = OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp()).unwrap();
        let table = format!("test_query_table_{}", now.unix_timestamp());
        insert(&client, &project_id, &dataset, &table, 3, &now).await;

        // query
        let mut data_as_row: Vec<TestData> = vec![];
        let mut iterator_as_row = client
            .query_with_option::<query::row::Row>(
                &project_id,
                QueryRequest {
                    max_results,
                    query: format!("SELECT * FROM {}.{}", dataset, table),
                    ..Default::default()
                },
                option.clone(),
            )
            .await
            .unwrap();
        while let Some(row) = iterator_as_row.next().await.unwrap() {
            data_as_row.push(TestData {
                col_string: row.column(0).unwrap(),
                col_number: row.column(1).unwrap(),
                col_number_array: row.column(2).unwrap(),
                col_timestamp: row.column(3).unwrap(),
                col_json: row.column(4).unwrap(),
                col_json_array: row.column(5).unwrap(),
                col_struct: row.column(6).unwrap(),
                col_struct_array: row.column(7).unwrap(),
                col_binary: row.column(8).unwrap(),
            });
        }
        let mut data_as_struct: Vec<TestData> = vec![];
        let mut iterator_as_struct = client
            .query_with_option::<TestData>(
                &project_id,
                QueryRequest {
                    query: format!("SELECT * FROM {}.{}", dataset, table),
                    ..Default::default()
                },
                option,
            )
            .await
            .unwrap();
        while let Some(row) = iterator_as_struct.next().await.unwrap() {
            data_as_struct.push(row);
        }
        assert_eq!(iterator_as_struct.total_size, 3);
        assert_eq!(iterator_as_row.total_size, 3);
        assert_eq!(data_as_struct.len(), 3);
        assert_eq!(data_as_row.len(), 3);

        assert_data(&now, data_as_struct);
        assert_data(&now, data_as_row);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_read_table() {
        let dataset = dataset_name("table");
        let (client, project_id) = create_client().await;
        let now = OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp()).unwrap();
        let table = format!("test_read_table_{}", now.unix_timestamp());
        insert(&client, &project_id, &dataset, &table, 3, &now).await;

        let table = TableReference {
            project_id,
            dataset_id: dataset.to_string(),
            table_id: table.to_string(),
        };
        let mut iterator_as_struct = client.read_table::<TestData>(&table, None).await.unwrap();

        let option = ReadTableOption {
            session_read_options: Some(TableReadOptions {
                row_restriction: "col_string = \"test_0\"".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut iterator_as_row = client
            .read_table::<crate::storage::row::Row>(&table, Some(option))
            .await
            .unwrap();
        let mut data_as_row: Vec<TestData> = vec![];
        let mut data_as_struct: Vec<TestData> = vec![];
        let mut finish1 = false;
        let mut finish2 = false;
        loop {
            tokio::select! {
                row = iterator_as_struct.next() => {
                    if let Some(row) = row.unwrap() {
                        tracing::info!("read struct some");
                        data_as_struct.push(row);
                    }else {
                        tracing::info!("read struct none");
                        finish1 = true;
                        if finish1 && finish2 {
                            break;
                        }
                    }
                },
                row = iterator_as_row.next() => {
                    if let Some(row) = row.unwrap() {
                        tracing::info!("read row some");
                        data_as_row.push(TestData {
                            col_string: row.column(0).unwrap(),
                            col_number: row.column(1).unwrap(),
                            col_number_array: row.column(2).unwrap(),
                            col_timestamp: row.column(3).unwrap(),
                            col_json: row.column(4).unwrap(),
                            col_json_array: row.column(5).unwrap(),
                            col_struct: row.column(6).unwrap(),
                            col_struct_array: row.column(7).unwrap(),
                            col_binary: row.column(8).unwrap(),
            }           );
                    }else {
                        tracing::info!("read row none");
                        finish2 = true;
                        if finish1 && finish2 {
                            break;
                        }
                    }
                }
            }
        }
        assert_eq!(data_as_struct.len(), 3);
        assert_eq!(data_as_row.len(), 1);

        assert_data(&now, data_as_struct);
        assert_data(&now, data_as_row);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_query_job_incomplete_from_storage() {
        test_query_job_incomplete(None, QueryOption::default().with_enable_storage_read(true)).await
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_query_job_incomplete_from_rest() {
        test_query_job_incomplete(Some(4999), QueryOption::default()).await
    }

    async fn test_query_job_incomplete(max_results: Option<i64>, option: QueryOption) {
        let dataset = dataset_name("table");
        let (client, project_id) = create_client().await;
        let now = OffsetDateTime::now_utc();
        let table = format!("test_query_job_incomplete_{}", now.unix_timestamp());
        const SIZE: usize = 10000;
        insert(&client, &project_id, &dataset, &table, SIZE, &now).await;

        let mut data: Vec<query::row::Row> = vec![];
        let mut iter = client
            .query_with_option::<query::row::Row>(
                &project_id,
                QueryRequest {
                    timeout_ms: Some(5), // pass wait_for_query
                    use_query_cache: Some(false),
                    max_results,
                    query: format!("SELECT 1 FROM {}.{}", dataset, table),
                    ..Default::default()
                },
                option,
            )
            .await
            .unwrap();
        while let Some(row) = iter.next().await.unwrap() {
            data.push(row);
        }
        assert_eq!(iter.total_size, SIZE as i64);
        assert_eq!(data.len(), SIZE);
    }

    fn assert_data(now: &OffsetDateTime, data: Vec<TestData>) {
        for (i, d) in data.iter().enumerate() {
            assert_eq!(&TestData::default(i, *now + Duration::from_secs(i as u64)), d);
        }
    }
}
