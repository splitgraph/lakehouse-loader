use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::{Bytes, BytesMut};
use clap::{Parser, Subcommand};
use deltalake::kernel::{Action, Add, Protocol};
use deltalake::logstore::{default_logstore, LogStore};
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::transaction::CommitBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::storage::{factories, ObjectStoreFactory};
use deltalake::DeltaResult;
use futures::stream::iter;
use futures::{pin_mut, TryStream, TryStreamExt};
use futures::{stream::BoxStream, StreamExt};
use object_store::aws::{AmazonS3ConfigKey, S3ConditionalPut};
use object_store::{MultipartUpload, PutMultipartOpts, PutPayload};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use thiserror::Error;

use deltalake::writer::create_add;

use log::{debug, info, warn};
use object_store::path::Path;
use object_store::{
    prefix::PrefixStore, DynObjectStore, GetOptions, GetResult, ListResult, ObjectMeta,
    ObjectStore, PutOptions, PutResult,
};
use std::fmt::Display;
use std::fs::File;
use std::ops::Range;

use url::Url;

use std::sync::Arc;
use tempfile::{NamedTempFile, TempPath};
use tokio::fs::File as AsyncFile;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::sync::Semaphore;
use uuid::Uuid;

pub mod pg_arrow_source;
mod pg_datetime;
mod pg_numeric;
use pg_arrow_source::{ArrowBuilder, PgArrowSource};

#[derive(Debug, Parser)]
#[command(name = "lhl")]
#[command(about = "Lakehouse Data Loader", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(arg_required_else_help = true)]
    ParquetToDelta {
        source_file: String,
        target_url: Url,
        #[clap(long, short, action)]
        overwrite: bool,
    },
    #[command(arg_required_else_help = true)]
    PgToDelta {
        connection_string: Url,
        target_url: Url,
        #[clap(long, short, action, help("SQL text to extract the data"))]
        query: String,
        #[clap(long, short, action)]
        overwrite: bool,
        #[clap(
            long,
            short,
            action,
            default_value_t = 10000,
            help("Number of rows to process per batch")
        )]
        batch_size: usize,
    },
}

const MAX_ROW_GROUP_SIZE: usize = 122880;
const PARTITION_FILE_BUFFER_SIZE: usize = 128 * 1024;
const PARTITION_FILE_MIN_PART_SIZE: usize = 5 * 1024 * 1024;
const PARTITION_FILE_UPLOAD_MAX_CONCURRENCY: usize = 2;

/// Open a temporary file to write partition and return a handle and a writer for it.
fn temp_partition_file_writer(
    arrow_schema: SchemaRef,
) -> Result<(TempPath, ArrowWriter<File>), DataLoadingError> {
    let partition_file = NamedTempFile::new().expect("Open a temporary file to write partition");
    let path = partition_file.into_temp_path();

    let file_writer = File::options().write(true).open(&path)?;

    let writer_properties = WriterProperties::builder()
        .set_max_row_group_size(MAX_ROW_GROUP_SIZE)
        .set_compression(Compression::SNAPPY)
        // .set_bloom_filter_enabled(true) TODO: not enough disk
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
        .build();
    let writer = ArrowWriter::try_new(file_writer, arrow_schema, Some(writer_properties))?;
    Ok((path, writer))
}

#[derive(Error, Debug)]
pub enum DataLoadingError {
    #[error("parquet error")]
    ParquetError(#[from] parquet::errors::ParquetError),
    #[error("postgres error")]
    PostgresError(tokio_postgres::Error),
    #[error("arrow error")]
    ArrowError(#[from] arrow::error::ArrowError),
    #[error("Delta Table error")]
    DeltaTableError(#[from] deltalake::DeltaTableError),
    #[error("I/O error")]
    IoError(#[from] std::io::Error),
    #[error("Object store error")]
    ObjectStoreError(#[from] object_store::Error),
    #[error("join error")]
    JoinError(#[from] tokio::task::JoinError),
}

// Export a stream of record batches into a Delta Table in a certain object store
// mostly copied from Seafowl

pub async fn record_batches_to_object_store(
    record_batch_stream: impl TryStream<Item = Result<RecordBatch, DataLoadingError>>,
    schema: SchemaRef,
    store: Arc<dyn ObjectStore>,
    prefix: &Path,
    max_partition_size: u32,
) -> Result<Vec<Add>, DataLoadingError> {
    let mut current_partition_size = 0;
    let (mut current_partition_file_path, mut writer) = temp_partition_file_writer(schema.clone())?;
    let mut partition_file_paths = vec![current_partition_file_path];
    let mut partition_metadata = vec![];
    let mut tasks = vec![];

    pin_mut!(record_batch_stream);

    while let Some(mut batch) = record_batch_stream.try_next().await? {
        let mut leftover_partition_capacity =
            (max_partition_size - current_partition_size) as usize;

        while batch.num_rows() > leftover_partition_capacity {
            if leftover_partition_capacity > 0 {
                // Fill up the remaining capacity in the slice
                writer
                    .write(&batch.slice(0, leftover_partition_capacity))
                    .map_err(DataLoadingError::from)?;
                // Trim away the part that made it to the current partition
                batch = batch.slice(
                    leftover_partition_capacity,
                    batch.num_rows() - leftover_partition_capacity,
                );
            }

            // Roll-over into the next partition: close partition writer, reset partition size
            // counter and open new temp file + writer.
            let file_metadata = writer.close().map_err(DataLoadingError::from)?;
            partition_metadata.push(file_metadata);

            current_partition_size = 0;
            leftover_partition_capacity = max_partition_size as usize;

            (current_partition_file_path, writer) = temp_partition_file_writer(schema.clone())?;
            partition_file_paths.push(current_partition_file_path);
        }

        current_partition_size += batch.num_rows() as u32;
        writer.write(&batch).map_err(DataLoadingError::from)?;
    }
    let file_metadata = writer.close().map_err(DataLoadingError::from)?;
    partition_metadata.push(file_metadata);

    info!("Starting upload of partition objects");
    let partitions_uuid = Uuid::new_v4();

    let sem = Arc::new(Semaphore::new(PARTITION_FILE_UPLOAD_MAX_CONCURRENCY));
    for (part, (partition_file_path, metadata)) in partition_file_paths
        .into_iter()
        .zip(partition_metadata)
        .enumerate()
    {
        let permit = Arc::clone(&sem).acquire_owned().await.ok();

        let store = store.clone();
        let prefix = prefix.clone();
        let handle: tokio::task::JoinHandle<Result<Add, DataLoadingError>> =
            tokio::task::spawn(async move {
                // Move the ownership of the semaphore permit into the task
                let _permit = permit;

                // This is taken from delta-rs `PartitionWriter::next_data_path`
                let file_name = format!("part-{part:0>5}-{partitions_uuid}-c000.snappy.parquet");
                let location = prefix.child(file_name.clone());

                let size = tokio::fs::metadata(
                    partition_file_path
                        .to_str()
                        .expect("Temporary Parquet file in the FS root"),
                )
                .await?
                .len() as i64;

                let file = AsyncFile::open(partition_file_path).await?;
                let mut reader = BufReader::with_capacity(PARTITION_FILE_BUFFER_SIZE, file);
                let mut part_buffer = BytesMut::with_capacity(PARTITION_FILE_MIN_PART_SIZE);

                let mut multipart_upload = store.put_multipart(&location).await?;

                let error: std::io::Error;
                let mut eof_counter = 0;

                loop {
                    match reader.read_buf(&mut part_buffer).await {
                        Ok(0) if part_buffer.is_empty() => {
                            // We've reached EOF and there are no pending writes to flush.
                            // As per the docs size = 0 doesn't seem to guarantee that we've reached EOF, so we use
                            // a heuristic: if we encounter Ok(0) 3 times in a row it's safe to assume it's EOF.
                            // Another potential workaround is to use `stream_position` + `stream_len` to determine
                            // whether we've reached the end (`stream_len` is nightly-only experimental API atm)
                            eof_counter += 1;
                            if eof_counter >= 3 {
                                break;
                            } else {
                                continue;
                            }
                        }
                        Ok(size)
                            if size != 0 && part_buffer.len() < PARTITION_FILE_MIN_PART_SIZE =>
                        {
                            // Keep filling the part buffer until it surpasses the minimum required size
                            eof_counter = 0;
                            continue;
                        }
                        Ok(_) => {
                            let part_size = part_buffer.len();
                            debug!("Uploading part with {} bytes", part_size);
                            match multipart_upload
                                .put_part(part_buffer[..part_size].to_vec().into())
                                .await
                            {
                                Ok(_) => {
                                    part_buffer.clear();
                                    continue;
                                }
                                Err(err) => error = err.into(),
                            }
                        }
                        Err(err) => error = err,
                    }

                    warn!(
                        "Aborting multipart partition upload due to an error: {:?}",
                        error
                    );
                    multipart_upload.abort().await?;
                    return Err(DataLoadingError::IoError(error));
                }

                multipart_upload.complete().await?;

                // Create the corresponding Add action; currently we don't support partition columns
                // which simplifies things.
                let add =
                    create_add(&Default::default(), file_name, size, &metadata, -1, &None).unwrap();

                Ok(add)
            });
        tasks.push(handle);
    }

    futures::future::join_all(tasks)
        .await
        .into_iter()
        .map(|t| t?)
        .collect::<Result<Vec<Add>, DataLoadingError>>()
}

fn keys_from_env(predicate: fn(&str) -> bool) -> Vec<(String, String)> {
    std::env::vars_os()
        .flat_map(|(k, v)| {
            if let (Some(key), Some(value)) = (k.to_str(), v.to_str()) {
                predicate(key).then(|| (key.to_ascii_lowercase(), value.to_string()))
            } else {
                None
            }
        })
        .collect()
}

pub fn object_store_keys_from_env(url_scheme: &str) -> Vec<(String, String)> {
    match url_scheme {
        "s3" | "s3a" => keys_from_env(|k| k.starts_with("AWS_")),
        "gs" => keys_from_env(|k| k.starts_with("GOOGLE_")),
        "az" | "adl" | "azure" | "abfs" | "abfss" => keys_from_env(|k| k.starts_with("AZURE_")),
        _ => vec![],
    }
}

#[derive(Debug, Clone)]
pub struct CompatObjectStore {
    pub inner: Arc<DynObjectStore>,
    pub table_url: Url,
    pub path: Path,
}

impl Display for CompatObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl ObjectStoreFactory for CompatObjectStore {
    fn parse_url_opts(
        &self,
        _url: &Url,
        _options: &deltalake::storage::StorageOptions,
    ) -> DeltaResult<(deltalake::storage::ObjectStoreRef, deltalake::Path)> {
        Ok((self.inner.clone(), deltalake::Path::from("/")))
    }
}

impl CompatObjectStore {
    fn log_store(&self) -> Arc<dyn LogStore> {
        let prefix_store: PrefixStore<CompatObjectStore> =
            PrefixStore::new(self.clone(), self.path.clone());

        (default_logstore(
            Arc::from(prefix_store),
            &self.table_url,
            &Default::default(),
        )) as _
    }
}

#[async_trait::async_trait]
impl ObjectStore for CompatObjectStore {
    async fn put(
        &self,
        location: &Path,
        put_payload: PutPayload,
    ) -> object_store::Result<PutResult> {
        self.inner.put(location, put_payload).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        put_payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, put_payload, opts).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        multipart_opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner
            .put_multipart_opts(location, multipart_opts)
            .await
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        self.inner.get(location).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> object_store::Result<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, object_store::Result<Path>>,
    ) -> BoxStream<'a, object_store::Result<Path>> {
        locations
            .map(|location| async {
                let location = location?;
                self.delete(&location).await?;
                Ok(location)
            })
            .buffered(10)
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    // S3 doesn't support atomic rename/copy_if_not_exists without a DynamoDB lock
    // but we don't care since we're just creating a table from a single writer
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        if self.table_url.scheme() == "s3" || self.table_url.scheme() == "s3a" {
            return self.inner.copy(from, to).await;
        }
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        if self.table_url.scheme() == "s3" || self.table_url.scheme() == "s3a" {
            return self.inner.rename(from, to).await;
        }
        self.inner.rename_if_not_exists(from, to).await
    }
}

async fn record_batches_to_delta(
    record_batch_stream: impl TryStream<Item = Result<RecordBatch, DataLoadingError>>,
    schema: SchemaRef,
    target_url: Url,
    overwrite: bool,
) -> Result<(), DataLoadingError> {
    let mut config = object_store_keys_from_env(target_url.scheme());
    config.push((AmazonS3ConfigKey::ConditionalPut.as_ref().to_string(), S3ConditionalPut::ETagMatch.to_string()));

    // Handle some deltalake weirdness
    let (store, path) = object_store::parse_url_opts(&target_url, config).unwrap();
    let store = Arc::from(CompatObjectStore {
        inner: Arc::from(store),
        table_url: target_url.clone(),
        path: path.clone(),
    });
    factories().insert(target_url.clone(), store.clone());
    let log_store = store.log_store();

    // Delete existing contents
    let existing = store
        .list(Some(&path))
        .map_ok(|m| m.location)
        .boxed()
        .try_collect::<Vec<Path>>()
        .await?;
    if !existing.is_empty() && !overwrite {
        info!(
            "{} already contains data, pass --overwrite to overwrite",
            &target_url
        );
        return Ok(());
    }

    store
        .delete_stream(iter(existing).map(Ok).boxed())
        .try_collect::<Vec<Path>>()
        .await?;

    let delta_transactions = record_batches_to_object_store(
        record_batch_stream,
        schema.clone(),
        store.clone(),
        &path,
        2 * 1024 * 1024,
    )
    .await?;

    let delta_schema = deltalake::kernel::Schema::try_from(schema)?;
    let table_name = target_url.path_segments().unwrap().last().unwrap();

    let table = CreateBuilder::new()
        .with_log_store(log_store.clone())
        .with_table_name(table_name)
        .with_columns(delta_schema.fields().cloned())
        // Set the writer protocol to 1 (defaults to 2 which means that after this
        // we won't be able to write to the table without the datafusion crate support)
        .with_actions(vec![Action::Protocol(Protocol {
            min_reader_version: 1,
            min_writer_version: 1,
            reader_features: None,
            writer_features: None,
        })])
        .await?;

    let actions: Vec<Action> = delta_transactions.into_iter().map(Action::Add).collect();
    let op = DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by: None,
        predicate: None,
    };
    let version = CommitBuilder::default()
        .with_actions(actions)
        .build(Some(table.snapshot()?), table.log_store(), op)
        .await?
        .version;

    info!(
        "Created Delta Table {:?} at {:?}, version {:?}",
        table_name, target_url, version
    );
    Ok(())
}

pub async fn do_main(args: Cli) -> Result<(), DataLoadingError> {
    match args.command {
        Commands::ParquetToDelta {
            source_file,
            target_url,
            overwrite,
        } => {
            let file = tokio::fs::File::open(source_file).await?;
            let record_batch_reader = ParquetRecordBatchStreamBuilder::new(file)
                .await?
                .build()
                .unwrap();
            let schema = record_batch_reader.schema().clone();
            info!("File schema: {}", schema);
            record_batches_to_delta(
                record_batch_reader.map_err(DataLoadingError::ParquetError),
                schema,
                target_url,
                overwrite,
            )
            .await
        }
        Commands::PgToDelta {
            connection_string,
            query,
            target_url,
            overwrite,
            batch_size,
        } => {
            let mut source = PgArrowSource::new(connection_string.as_ref(), &query, batch_size)
                .await
                .map_err(DataLoadingError::PostgresError)?;
            let arrow_schema = source.get_arrow_schema();
            let record_batch_stream = source.get_record_batch_stream();
            info!("Rowset schema: {}", arrow_schema);
            record_batches_to_delta(record_batch_stream, arrow_schema, target_url, overwrite).await
        }
    }

    // TODO
    //   - custom parquet writer settings (e.g. bloom filters)
    //   - bypass the disk, write files to ram one-by-one and emit them out
    //   - (later) sorting on certain cols via datafusion
    //   - (later) using actual tpc code + https://github.com/andygrove/tpctools
}
