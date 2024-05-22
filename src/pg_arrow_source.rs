use std::env;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{self, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_schema::TimeUnit;
use futures::Stream;
use futures::StreamExt;
use native_tls::TlsConnector;
use postgres::types::{FromSql, Type};
use postgres::Row;
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::{Config, Error, RowStream};

#[allow(clippy::enum_variant_names)]
pub enum ArrowBuilder {
    BooleanBuilder(array::BooleanBuilder),
    Int8Builder(array::Int8Builder),
    Int16Builder(array::Int16Builder),
    Int32Builder(array::Int32Builder),
    Int64Builder(array::Int64Builder),
    Float32Builder(array::Float32Builder),
    Float64Builder(array::Float64Builder),
    TimestampMicrosecondBuilder(array::TimestampMicrosecondBuilder),
    StringBuilder(array::StringBuilder),
    BinaryBuilder(array::BinaryBuilder),
}
use crate::{ArrowBuilder::*, DataLoadingError};

// tokio-postgres provides awkward Rust type conversions for Postgres TIMESTAMP and TIMESTAMPTZ values
// It's easier just to handle the raw values ourselves
struct UnixEpochMicrosecondOffset(i64);
const J2000_EPOCH_OFFSET: i64 = 946_684_800_000_000; // Number of us from 1970-01-01 to 2000-01-01

impl FromSql<'_> for UnixEpochMicrosecondOffset {
    fn from_sql(_ty: &Type, buf: &[u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let byte_array: [u8; 8] = buf.try_into()?;
        let offset = i64::from_be_bytes(byte_array) + J2000_EPOCH_OFFSET;
        Ok(Self(offset))
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::TIMESTAMP || *ty == Type::TIMESTAMPTZ
    }
}
impl From<UnixEpochMicrosecondOffset> for i64 {
    fn from(val: UnixEpochMicrosecondOffset) -> Self {
        val.0
    }
}

impl ArrowBuilder {
    pub fn from_pg_type(pg_type: &Type) -> Self {
        match *pg_type {
            Type::BOOL => BooleanBuilder(array::BooleanBuilder::new()),
            Type::CHAR => Int8Builder(array::Int8Builder::new()),
            Type::INT2 => Int16Builder(array::Int16Builder::new()),
            Type::INT4 => Int32Builder(array::Int32Builder::new()),
            Type::INT8 => Int64Builder(array::Int64Builder::new()),
            Type::FLOAT4 => Float32Builder(array::Float32Builder::new()),
            Type::FLOAT8 => Float64Builder(array::Float64Builder::new()),
            Type::TIMESTAMP => {
                TimestampMicrosecondBuilder(array::TimestampMicrosecondBuilder::new())
            }
            Type::TIMESTAMPTZ => TimestampMicrosecondBuilder(
                array::TimestampMicrosecondBuilder::new().with_data_type(DataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some("UTC".into()),
                )),
            ),
            Type::TEXT => StringBuilder(array::StringBuilder::new()),
            Type::BYTEA => BinaryBuilder(array::BinaryBuilder::new()),
            _ => panic!("Unsupported type: {}", pg_type),
        }
    }
    // Append a value from a tokio-postgres row to the ArrowBuilder
    pub fn append_option(&mut self, row: &Row, column_idx: usize) {
        match self {
            BooleanBuilder(ref mut builder) => {
                builder.append_option(row.get::<usize, Option<bool>>(column_idx))
            }
            Int8Builder(ref mut builder) => {
                builder.append_option(row.get::<usize, Option<i8>>(column_idx))
            }
            Int16Builder(ref mut builder) => {
                builder.append_option(row.get::<usize, Option<i16>>(column_idx))
            }
            Int32Builder(ref mut builder) => {
                builder.append_option(row.get::<usize, Option<i32>>(column_idx))
            }
            Int64Builder(ref mut builder) => {
                builder.append_option(row.get::<usize, Option<i64>>(column_idx))
            }
            Float32Builder(ref mut builder) => {
                builder.append_option(row.get::<usize, Option<f32>>(column_idx))
            }
            Float64Builder(ref mut builder) => {
                builder.append_option(row.get::<usize, Option<f64>>(column_idx))
            }
            TimestampMicrosecondBuilder(ref mut builder) => builder.append_option(
                row.get::<usize, Option<UnixEpochMicrosecondOffset>>(column_idx)
                    .map(UnixEpochMicrosecondOffset::into),
            ),
            StringBuilder(ref mut builder) => {
                builder.append_option(row.get::<usize, Option<&str>>(column_idx))
            }
            BinaryBuilder(ref mut builder) => {
                builder.append_option(row.get::<usize, Option<&[u8]>>(column_idx))
            }
        }
    }
    pub fn finish(&mut self) -> Arc<dyn array::Array> {
        match self {
            BooleanBuilder(builder) => Arc::new(builder.finish()),
            Int8Builder(builder) => Arc::new(builder.finish()),
            Int16Builder(builder) => Arc::new(builder.finish()),
            Int32Builder(builder) => Arc::new(builder.finish()),
            Int64Builder(builder) => Arc::new(builder.finish()),
            Float32Builder(builder) => Arc::new(builder.finish()),
            Float64Builder(builder) => Arc::new(builder.finish()),
            TimestampMicrosecondBuilder(builder) => Arc::new(builder.finish()),
            StringBuilder(builder) => Arc::new(builder.finish()),
            BinaryBuilder(builder) => Arc::new(builder.finish()),
        }
    }
}

fn pg_type_to_arrow_type(pg_type: &Type) -> DataType {
    match *pg_type {
        Type::BOOL => DataType::Boolean,
        Type::CHAR => DataType::Int8,
        Type::INT2 => DataType::Int16,
        Type::INT4 => DataType::Int32,
        Type::INT8 => DataType::Int64,
        Type::FLOAT4 => DataType::Float32,
        Type::FLOAT8 => DataType::Float64,
        Type::TIMESTAMP => DataType::Timestamp(TimeUnit::Microsecond, None),
        Type::TIMESTAMPTZ => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        Type::TEXT => DataType::Utf8,
        Type::BYTEA => DataType::Binary,
        _ => panic!("Unsupported type: {}. Explicitly cast the relevant columns to text in order to store them as strings.", pg_type),
    }
}

pub struct PgArrowSource {
    batch_size: usize,
    pg_row_stream: Pin<Box<RowStream>>,
    pg_types: Vec<Type>,
    arrow_schema: Arc<Schema>,
}

impl PgArrowSource {
    pub fn get_arrow_schema(&self) -> Arc<Schema> {
        self.arrow_schema.clone()
    }
    pub async fn new(
        connection_string: &str,
        query_text: &str,
        batch_size: usize,
    ) -> Result<Self, Error> {
        let mut config = connection_string.parse::<Config>().unwrap();
        if let Ok(pg_password) = env::var("PGPASSWORD") {
            config.password(pg_password);
        }

        let native_tls_connector = TlsConnector::builder().build().unwrap();
        let postgres_tls_connector = MakeTlsConnector::new(native_tls_connector);
        let (client, connection) = config.connect(postgres_tls_connector).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // Prepare a statement with the query text to find out its schema
        let prepare_result = client.prepare(query_text).await?;
        let postgres_columns = prepare_result.columns();

        // Transform schema
        let (pg_types, arrow_fields): (Vec<_>, Vec<_>) = postgres_columns
            .iter()
            .map(|c| {
                let pg_type = c.type_().clone();
                let arrow_type = pg_type_to_arrow_type(&pg_type);
                (pg_type, Field::new(c.name(), arrow_type, true))
            })
            .unzip();
        let arrow_schema = Arc::new(Schema::new(arrow_fields.clone()));

        // Start query execution
        let params: Vec<String> = Vec::new();
        let pg_row_stream = Box::pin(client.query_raw(query_text, &params).await?);

        Ok(PgArrowSource {
            batch_size,
            pg_row_stream,
            pg_types,
            arrow_schema,
        })
    }
    pub fn get_record_batch_stream(
        &mut self,
    ) -> impl Stream<Item = Result<RecordBatch, DataLoadingError>> + '_ {
        let pg_types = &self.pg_types;
        let arrow_schema = self.get_arrow_schema();
        (&mut self.pg_row_stream).chunks(self.batch_size).map(
            move |chunk| -> Result<RecordBatch, DataLoadingError> {
                let mut builders: Vec<ArrowBuilder> =
                    pg_types.iter().map(ArrowBuilder::from_pg_type).collect();
                for result in chunk {
                    let row = result.map_err(DataLoadingError::PostgresError)?;
                    assert_eq!(row.len(), builders.len());
                    for (column_idx, builder) in builders.iter_mut().enumerate() {
                        builder.append_option(&row, column_idx);
                    }
                }
                let arrow_arrays = builders
                    .iter_mut()
                    .map(|builder| builder.finish())
                    .collect();
                RecordBatch::try_new(arrow_schema.clone(), arrow_arrays)
                    .map_err(DataLoadingError::ArrowError)
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use postgres::types::{FromSql, Type};

    use super::UnixEpochMicrosecondOffset;

    #[test]
    fn test_just_after_j2000() {
        let offset =
            UnixEpochMicrosecondOffset::from_sql(&Type::TIMESTAMP, &[0, 0, 0, 0, 0, 0, 1, 2])
                .unwrap();
        assert_eq!(offset.0, 946_684_800_000_000 + 256 + 2);
    }
    #[test]
    fn test_just_before_j2000() {
        let offset = UnixEpochMicrosecondOffset::from_sql(
            &Type::TIMESTAMP,
            &[255, 255, 255, 255, 255, 255, 255, 255],
        )
        .unwrap();
        assert_eq!(offset.0, 946_684_800_000_000 - 1);
    }
}
