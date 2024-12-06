use clap::{Parser, Subcommand};
use delta_destination::record_batches_to_delta;
use error::DataLoadingError;
use futures::TryStreamExt;
use iceberg_destination::record_batches_to_iceberg;
use log::info;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use url::Url;

pub mod delta_destination;
pub mod error;
pub mod iceberg_destination;
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
    #[command(arg_required_else_help = true)]
    ParquetToIceberg {
        source_file: String,
        target_url: Url,
        #[clap(long, short, action)]
        overwrite: bool,
        #[clap(long, action)]
        append: bool,
    },
    #[command(arg_required_else_help = true)]
    PgToIceberg {
        connection_string: Url,
        target_url: Url,
        #[clap(long, short, action, help("SQL text to extract the data"))]
        query: String,
        #[clap(long, short, action)]
        overwrite: bool,
        #[clap(long, action)]
        append: bool,
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

const OPTIMISTIC_CONCURRENCY_RETRIES: u32 = 3;

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
        Commands::ParquetToIceberg {
            source_file,
            target_url,
            overwrite,
            append,
        } => {
            for _ in 0..OPTIMISTIC_CONCURRENCY_RETRIES {
                let file = tokio::fs::File::open(&source_file).await?;
                let record_batch_reader = ParquetRecordBatchStreamBuilder::new(file)
                    .await?
                    .build()
                    .unwrap();
                let arrow_schema = record_batch_reader.schema().clone();
                let record_batch_stream =
                    record_batch_reader.map_err(DataLoadingError::ParquetError);
                info!("File schema: {}", arrow_schema);
                match record_batches_to_iceberg(
                    record_batch_stream,
                    arrow_schema,
                    target_url.clone(),
                    overwrite,
                    append,
                )
                .await
                {
                    Err(DataLoadingError::OptimisticConcurrencyError()) => {
                        info!("Optimistic concurrency error. Retrying");
                        continue;
                    }
                    Err(e) => {
                        return Err(e);
                    }
                    Ok(_) => {
                        break;
                    }
                }
            }
            Ok(())
        }
        Commands::PgToIceberg {
            connection_string,
            target_url,
            query,
            overwrite,
            append,
            batch_size,
        } => {
            for _ in 0..OPTIMISTIC_CONCURRENCY_RETRIES {
                let mut source = PgArrowSource::new(connection_string.as_ref(), &query, batch_size)
                    .await
                    .map_err(DataLoadingError::PostgresError)?;
                let arrow_schema = source.get_arrow_schema();
                let record_batch_stream = source.get_record_batch_stream();
                info!("Rowset schema: {}", arrow_schema);
                match record_batches_to_iceberg(
                    record_batch_stream,
                    arrow_schema,
                    target_url.clone(),
                    overwrite,
                    append,
                )
                .await
                {
                    Err(DataLoadingError::OptimisticConcurrencyError()) => {
                        info!("Optimistic concurrency error. Retrying");
                        continue;
                    }
                    Err(e) => {
                        return Err(e);
                    }
                    Ok(_) => {
                        break;
                    }
                }
            }
            Ok(())
        }
    }
    // TODO
    //   - custom parquet writer settings (e.g. bloom filters)
    //   - bypass the disk, write files to ram one-by-one and emit them out
    //   - (later) sorting on certain cols via datafusion
    //   - (later) using actual tpc code + https://github.com/andygrove/tpctools
}
