use core::str;
use itertools::izip;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaRef};
use futures::{pin_mut, StreamExt, TryStream};
use iceberg::io::FileIO;
use iceberg::spec::{
    BoundPartitionSpec, DataContentType, DataFileFormat, FormatVersion, Manifest,
    ManifestContentType, ManifestEntry, ManifestFile, ManifestListWriter, ManifestMetadata,
    ManifestStatus, ManifestWriter, Operation, Snapshot, SnapshotReference, SnapshotRetention,
    Struct, Summary, TableMetadata, TableMetadataBuilder,
};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder};
use iceberg::TableCreation;
use log::info;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;
use url::Url;
use uuid::Uuid;

use crate::error::DataLoadingError;

// Defines how to behave with existing tables
#[derive(Debug, Clone, PartialEq)]
enum WriteMode {
    CreateExclusive, // Error out if the table already exists
    Overwrite,       // Overwrite existing table data
    Append,          // Append to existing table data
}

fn create_file_io(target_url: String) -> Result<FileIO, DataLoadingError> {
    let mut file_io_props: Vec<(String, String)> = vec![];
    if let Ok(aws_endpoint) = std::env::var("AWS_ENDPOINT") {
        file_io_props.push(("s3.endpoint".to_string(), aws_endpoint));
    }

    let file_io = FileIO::from_path(target_url.clone())?
        .with_props(file_io_props)
        .build()?;
    Ok(file_io)
}

// Create an empty table metadata object that contains no snapshots
fn create_empty_metadata(
    iceberg_schema: &iceberg::spec::Schema,
    target_url: String,
) -> Result<TableMetadata, DataLoadingError> {
    let table_creation = TableCreation::builder()
        .name("dummy_name".to_string()) // Required by TableCreationBuilder. Doesn't affect output
        .schema(iceberg_schema.clone())
        .location(target_url.to_string())
        .build();

    let table_metadata = TableMetadataBuilder::from_table_creation(table_creation)?.build()?;
    Ok(table_metadata.into())
}

// Clone an arrow schema, assigning sequential field IDs starting from 1
fn assign_field_ids(arrow_schema: &Arc<Schema>) -> Schema {
    let mut field_id_counter = 1;
    let new_fields: Vec<Field> = arrow_schema
        .fields
        .iter()
        .map(|field_ref| {
            let mut field: Field = (**field_ref).clone();
            let mut metadata = field_ref.metadata().clone();
            metadata.insert(
                PARQUET_FIELD_ID_META_KEY.to_owned(),
                field_id_counter.to_string(),
            );
            field_id_counter += 1;
            field.set_metadata(metadata);
            field
        })
        .collect();
    Schema::new_with_metadata(new_fields, arrow_schema.metadata.clone())
}

fn is_schema_aligned(
    new_arrow_schema: &Arc<Schema>,
    existing_iceberg_schema: &Arc<iceberg::spec::Schema>,
) -> Result<(), DataLoadingError> {
    let old_iceberg_struct = existing_iceberg_schema.as_struct();
    let old_iceberg_fields = old_iceberg_struct.fields();

    let new_arrow_schema_with_field_ids = assign_field_ids(new_arrow_schema);
    let new_iceberg_schema = Arc::new(iceberg::arrow::arrow_schema_to_schema(
        &new_arrow_schema_with_field_ids,
    )?);
    let new_iceberg_struct = new_iceberg_schema.as_struct();
    let new_iceberg_fields = new_iceberg_struct.fields();

    if old_iceberg_fields.len() != new_iceberg_fields.len() {
        return Err(DataLoadingError::BadInputError(format!("New data is incompatible with existing schema. Old schema has {} fields but new schema has {} fields", old_iceberg_fields.len(), new_iceberg_fields.len())));
    }
    for (i, old_iceberg_field, new_iceberg_field) in
        izip!(0.., old_iceberg_fields.iter(), new_iceberg_fields.iter())
    {
        if old_iceberg_field.required && !new_iceberg_field.required {
            return Err(DataLoadingError::BadInputError(format!("New data is incompatible with existing schema. Field {} ({}) is required in old schema but not required in new schema", i, old_iceberg_field.name)));
        }
        if old_iceberg_field.field_type != new_iceberg_field.field_type {
            return Err(DataLoadingError::BadInputError(format!("New data is incompatible with existing schema. Field {} ({}) has data type {:?} in old schema but {:?} in new schema", i, old_iceberg_field.name, old_iceberg_field.field_type, new_iceberg_field.field_type)));
        }
    }
    Ok(())
}

// Create a new TableMetadata object by updating the current snapshot of an existing TableMetadata
fn update_metadata_snapshot(
    previous_metadata: &TableMetadata,
    previous_metadata_location: Option<String>,
    snapshot: Snapshot,
) -> Result<TableMetadata, DataLoadingError> {
    let snapshot_id = snapshot.snapshot_id();
    let new_metadata: TableMetadata = TableMetadataBuilder::new_from_metadata(
        previous_metadata.clone(),
        previous_metadata_location,
    )
    .add_snapshot(snapshot)?
    .set_ref(
        "main",
        SnapshotReference::new(snapshot_id, SnapshotRetention::branch(None, None, None)),
    )?
    .build()?
    .into();
    Ok(new_metadata)
}

async fn get_manifest_files(
    file_io: &FileIO,
    table_metadata: &TableMetadata,
) -> Result<Option<Vec<ManifestFile>>, DataLoadingError> {
    let snapshot = match table_metadata.current_snapshot() {
        None => return Ok(None),
        Some(s) => s,
    };
    let manifest_list = snapshot.load_manifest_list(file_io, table_metadata).await?;
    Ok(Some(manifest_list.consume_entries().into_iter().collect()))
}

const DEFAULT_SCHEMA_ID: i32 = 0;

pub async fn record_batches_to_iceberg(
    record_batch_stream: impl TryStream<Item = Result<RecordBatch, DataLoadingError>>,
    arrow_schema: SchemaRef,
    target_url: Url,
    overwrite: bool,
    append: bool,
) -> Result<(), DataLoadingError> {
    let write_mode = match (overwrite, append) {
        (false, false) => WriteMode::CreateExclusive,
        (true, false) => WriteMode::Overwrite,
        (false, true) => WriteMode::Append,
        (true, true) => {
            return Err(DataLoadingError::BadInputError(
                "Cannot use overwrite flag with append flag".to_string(),
            ));
        }
    };

    pin_mut!(record_batch_stream);

    let file_io = create_file_io(target_url.to_string())?;

    let version_hint_location = format!("{}/metadata/version-hint.text", target_url);
    let version_hint_input = file_io.new_input(&version_hint_location)?;
    let old_version_hint: Option<u64> = if version_hint_input.exists().await? {
        if write_mode == WriteMode::CreateExclusive {
            return Err(DataLoadingError::IoError(std::io::Error::other(
                "Table exists. Pass the overwrite flag to lakehouse-loader to overwrite data",
            )));
        }
        let version_hint_bytes = version_hint_input.read().await?;
        let version_hint_string: String =
            String::from_utf8(version_hint_bytes.to_vec()).map_err(|_| {
                DataLoadingError::IcebergError(iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    "Could not parse UTF-8 in version-hint.text",
                ))
            })?;
        let version_hint_u64 = version_hint_string.trim().parse::<u64>().map_err(|_| {
            DataLoadingError::IcebergError(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                "Could not parse integer version in version-hint.text",
            ))
        })?;
        Some(version_hint_u64)
    } else {
        None
    };
    let (previous_metadata, previous_metadata_location, iceberg_schema) = match old_version_hint {
        Some(version_hint) => {
            let old_metadata_location =
                format!("{}/metadata/v{}.metadata.json", target_url, version_hint);
            let old_metadata_bytes = file_io.new_input(&old_metadata_location)?.read().await?;
            let old_metadata_string = str::from_utf8(&old_metadata_bytes).map_err(|_| {
                DataLoadingError::IcebergError(iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    "Could not parse UTF-8 in old metadata file",
                ))
            })?;
            let old_metadata =
                serde_json::from_str::<TableMetadata>(old_metadata_string).map_err(|_| {
                    DataLoadingError::IcebergError(iceberg::Error::new(
                        iceberg::ErrorKind::DataInvalid,
                        "Could not parse old metadata file",
                    ))
                })?;
            let old_iceberg_schema = old_metadata.current_schema();
            is_schema_aligned(&arrow_schema, old_iceberg_schema)?;
            (
                old_metadata.clone(),
                Some(old_metadata_location),
                old_iceberg_schema.clone(),
            )
        }
        None => {
            let arrow_schema_with_ids = assign_field_ids(&arrow_schema);
            let iceberg_schema = Arc::new(iceberg::arrow::arrow_schema_to_schema(
                &arrow_schema_with_ids,
            )?);
            let empty_metadata = create_empty_metadata(&iceberg_schema, target_url.to_string())?;
            (empty_metadata, None, iceberg_schema)
        }
    };

    let file_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        iceberg_schema.clone(),
        file_io.clone(),
        DefaultLocationGenerator::new(previous_metadata.clone()).unwrap(),
        DefaultFileNameGenerator::new(
            "part".to_string(),
            Some(Uuid::new_v4().to_string()),
            DataFileFormat::Parquet,
        ),
    );
    let mut file_writer = file_writer_builder.build().await.unwrap();

    while let Some(maybe_batch) = record_batch_stream.next().await {
        let batch = maybe_batch?;
        file_writer.write(&batch).await?;
    }
    let data_files: Vec<_> = file_writer
        .close()
        .await?
        .iter_mut()
        .map(|data_file_builder| {
            let data_file = data_file_builder
                .content(DataContentType::Data)
                .partition(Struct::empty())
                .build()
                .unwrap();
            info!("Wrote data file: {:?}", data_file.file_path());
            data_file
        })
        .collect();

    let snapshot_id = fastrand::i64(..);
    let sequence_number = previous_metadata.last_sequence_number() + 1;

    let manifest_file_path = format!("{}/metadata/manifest-{}.avro", target_url, Uuid::new_v4());
    let manifest_file_output = file_io.new_output(manifest_file_path)?;
    let manifest_writer: ManifestWriter =
        ManifestWriter::new(manifest_file_output, snapshot_id, vec![]);
    let manifest_metadata = ManifestMetadata::builder()
        .schema_id(DEFAULT_SCHEMA_ID)
        .schema(iceberg_schema.clone())
        .partition_spec(
            BoundPartitionSpec::builder(iceberg_schema.clone())
                .with_spec_id(0)
                .build()?,
        )
        .content(ManifestContentType::Data)
        .format_version(FormatVersion::V2)
        .build();
    let manifest = Manifest::new(
        manifest_metadata,
        data_files
            .iter()
            .map(|data_file| {
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .snapshot_id(snapshot_id)
                    .data_file(data_file.clone())
                    .build()
            })
            .collect(),
    );
    let new_manifest_file: ManifestFile = manifest_writer.write(manifest).await?;
    info!("Wrote manifest file: {:?}", new_manifest_file.manifest_path);

    let new_manifest_files_vec: Vec<ManifestFile> = match write_mode {
        WriteMode::CreateExclusive | WriteMode::Overwrite => vec![new_manifest_file], // Only include new manifest
        WriteMode::Append => match get_manifest_files(&file_io, &previous_metadata).await? {
            Some(mut manifest_files) => {
                // Include new manifest and all manifests from previous snapshot
                manifest_files.push(new_manifest_file);
                manifest_files
            }
            None => vec![new_manifest_file], // Only include new manifest
        },
    };

    let manifest_list_path = format!(
        "{}/metadata/manifest-list-{}.avro",
        target_url,
        Uuid::new_v4()
    );
    let manifest_file_output = file_io.new_output(manifest_list_path.clone())?;
    let mut manifest_list_writer: ManifestListWriter =
        ManifestListWriter::v2(manifest_file_output, snapshot_id, None, sequence_number);
    manifest_list_writer.add_manifests(new_manifest_files_vec.into_iter())?;
    manifest_list_writer.close().await?;
    info!("Wrote manifest list: {:?}", manifest_list_path);

    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_schema_id(DEFAULT_SCHEMA_ID)
        .with_manifest_list(manifest_list_path.clone())
        .with_sequence_number(sequence_number)
        .with_timestamp_ms(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        )
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::new(),
        })
        .build();

    let new_metadata =
        update_metadata_snapshot(&previous_metadata, previous_metadata_location, snapshot)?;
    let new_version_hint = match old_version_hint {
        Some(x) => x + 1,
        None => 0,
    };
    let new_metadata_location = format!(
        "{}/metadata/v{}.metadata.json",
        target_url, new_version_hint
    );

    if let Err(iceberg_error) = file_io
        .new_output(&new_metadata_location)?
        .write_exclusive(serde_json::to_vec(&new_metadata).unwrap().into())
        .await
    {
        if let Some(iceberg_error_source) = iceberg_error.source() {
            if let Some(opendal_error) = iceberg_error_source.downcast_ref::<opendal::Error>() {
                if opendal_error.kind() == opendal::ErrorKind::ConditionNotMatch {
                    return Err(DataLoadingError::OptimisticConcurrencyError());
                }
            }
        }
        return Err(iceberg_error.into());
    };
    info!("Wrote new metadata: {:?}", new_metadata_location);

    file_io
        .new_output(&version_hint_location)?
        .write(new_version_hint.to_string().into())
        .await?;
    info!("Wrote version hint: {:?}", version_hint_location);

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow_schema::{DataType, Field, Schema};
    use iceberg::spec::{NestedField, PrimitiveType, Type};

    use crate::iceberg_destination::is_schema_aligned;

    #[test]
    fn test_is_schema_aligned_positive() {
        let arrow_schema = Schema::new_with_metadata(
            vec![
                Field::new("a", DataType::Utf8, false),
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Boolean, false),
            ],
            HashMap::new(),
        );

        let iceberg_schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "a", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "b", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "c", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap();

        assert!(is_schema_aligned(&Arc::new(arrow_schema), &Arc::new(iceberg_schema)).is_ok());
    }

    #[test]
    fn test_is_schema_aligned_positive_renamed() {
        let arrow_schema = Schema::new_with_metadata(
            vec![
                // Fields renamed
                Field::new("x", DataType::Utf8, false),
                Field::new("y", DataType::Int32, false),
                Field::new("z", DataType::Boolean, false),
            ],
            HashMap::new(),
        );

        let iceberg_schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "a", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "b", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(3, "c", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap();

        assert!(is_schema_aligned(&Arc::new(arrow_schema), &Arc::new(iceberg_schema)).is_ok());
    }

    // OK to insert a non-nullable value into a nullable field
    #[test]
    fn test_is_schema_aligned_positive_nonnullable() {
        let arrow_schema = Schema::new_with_metadata(
            vec![
                Field::new("a", DataType::Utf8, false),
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Boolean, false),
            ],
            HashMap::new(),
        );

        let iceberg_schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "a", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(2, "b", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "c", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap();

        assert!(is_schema_aligned(&Arc::new(arrow_schema), &Arc::new(iceberg_schema)).is_ok());
    }

    #[test]
    fn test_is_schema_aligned_negative_added_field() {
        let arrow_schema = Schema::new_with_metadata(
            vec![
                Field::new("a", DataType::Utf8, false),
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Boolean, false),
                Field::new("d", DataType::Boolean, false), // Added field
            ],
            HashMap::new(),
        );

        let iceberg_schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "a", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "b", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(3, "c", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap();

        assert!(is_schema_aligned(&Arc::new(arrow_schema), &Arc::new(iceberg_schema)).is_err());
    }

    #[test]
    fn test_is_schema_aligned_negative_different_type() {
        let arrow_schema = Schema::new_with_metadata(
            vec![
                Field::new("a", DataType::Utf8, false),
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Int32, false), // Mismatched type
            ],
            HashMap::new(),
        );

        let iceberg_schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "a", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "b", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(3, "c", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap();

        assert!(is_schema_aligned(&Arc::new(arrow_schema), &Arc::new(iceberg_schema)).is_err());
    }

    #[test]
    fn test_is_schema_aligned_negative_reordered() {
        let arrow_schema = Schema::new_with_metadata(
            vec![
                // Same fields but in wrong order
                Field::new("b", DataType::Int32, false),
                Field::new("a", DataType::Utf8, false),
                Field::new("c", DataType::Boolean, false),
            ],
            HashMap::new(),
        );

        let iceberg_schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "a", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "b", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(3, "c", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap();

        assert!(is_schema_aligned(&Arc::new(arrow_schema), &Arc::new(iceberg_schema)).is_err());
    }

    // Not allowed to insert a nullable value into a non-nullable field
    #[test]
    fn test_is_schema_aligned_negative_nullable() {
        let arrow_schema = Schema::new_with_metadata(
            vec![
                Field::new("a", DataType::Utf8, true), // Nullable
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Boolean, false),
            ],
            HashMap::new(),
        );

        let iceberg_schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "a", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "b", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(3, "c", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap();

        assert!(is_schema_aligned(&Arc::new(arrow_schema), &Arc::new(iceberg_schema)).is_err());
    }
}
