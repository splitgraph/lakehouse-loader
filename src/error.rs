use thiserror::Error;

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
