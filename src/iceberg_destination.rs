use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaRef};
use futures::{pin_mut, StreamExt, TryStream};
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFileFormat, FormatVersion, Manifest, ManifestContentType, ManifestEntry,
    ManifestFile, ManifestListWriter, ManifestMetadata, ManifestStatus, ManifestWriter, Operation,
    PartitionSpec, Snapshot, Struct, Summary, TableMetadata, TableMetadataBuilder,
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

// Clone an arrow schema, assigning sequential field IDs starting from 1
fn assign_field_ids(arrow_schema: Arc<Schema>) -> Schema {
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

// Create the v0 metadata object. This one will contain no snapshot
fn create_metadata_v0(
    iceberg_schema: &iceberg::spec::Schema,
    target_url: String,
) -> Result<TableMetadata, DataLoadingError> {
    let table_creation = TableCreation::builder()
        .name("dummy_name".to_string()) // Required by TableCreationBuilder. Doesn't affect output
        .schema(iceberg_schema.clone())
        .location(target_url.to_string())
        .build();

    let table_metadata = TableMetadataBuilder::from_table_creation(table_creation)?.build()?;
    Ok(table_metadata)
}

// Create the v1 metadata object by adding a snapshot to the v0 metadata object
fn create_metadata_v1(
    metadata_v0: &TableMetadata,
    snapshot: Snapshot,
) -> Result<TableMetadata, DataLoadingError> {
    let snapshot_id = snapshot.snapshot_id();
    // Copy metadata v0, modifying current snapshot ID
    let mut metadata_v0_json = serde_json::to_value(metadata_v0).unwrap();
    if let Some(obj) = metadata_v0_json.as_object_mut() {
        obj.insert(
            "current-snapshot-id".to_string(),
            serde_json::Value::from(snapshot_id),
        );
    }
    let mut metadata_v1: TableMetadata = serde_json::from_value(metadata_v0_json).unwrap();
    metadata_v1.append_snapshot(snapshot);
    Ok(metadata_v1)
}

const DEFAULT_SCHEMA_ID: i32 = 0;

pub async fn record_batches_to_iceberg(
    record_batch_stream: impl TryStream<Item = Result<RecordBatch, DataLoadingError>>,
    arrow_schema: SchemaRef,
    target_url: Url,
) -> Result<(), DataLoadingError> {
    pin_mut!(record_batch_stream);

    let mut file_io_props: Vec<(String, String)> = vec![];
    if let Ok(aws_endpoint) = std::env::var("AWS_ENDPOINT") {
        file_io_props.push(("s3.endpoint".to_string(), aws_endpoint));
    }

    let file_io = FileIO::from_path(target_url.clone())?
        .with_props(file_io_props)
        .build()?;

    let arrow_schema_with_ids = assign_field_ids(arrow_schema.clone());
    let iceberg_schema = iceberg::arrow::arrow_schema_to_schema(&arrow_schema_with_ids)?;

    let metadata_v0 = create_metadata_v0(&iceberg_schema, target_url.to_string())?;
    let metadata_v0_location = format!("{}/metadata/v0.metadata.json", target_url);

    file_io
        .new_output(&metadata_v0_location)?
        .write(serde_json::to_vec(&metadata_v0).unwrap().into())
        .await?;
    info!("Wrote v0 metadata: {:?}", metadata_v0_location);

    let file_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        Arc::new(iceberg_schema.clone()),
        file_io.clone(),
        DefaultLocationGenerator::new(metadata_v0.clone()).unwrap(),
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
    let sequence_number = 1;

    let manifest_file_path = format!("{}/metadata/manifest-{}.avro", target_url, Uuid::new_v4());
    let manifest_file_output = file_io.new_output(manifest_file_path)?;
    let manifest_writer: ManifestWriter =
        ManifestWriter::new(manifest_file_output, snapshot_id, vec![]);
    let manifest_metadata = ManifestMetadata::builder()
        .schema_id(DEFAULT_SCHEMA_ID)
        .schema(iceberg_schema.clone())
        .partition_spec(
            PartitionSpec::builder(&iceberg_schema)
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
    let manifest_file: ManifestFile = manifest_writer.write(manifest).await?;
    info!("Wrote manifest file: {:?}", manifest_file.manifest_path);

    let manifest_list_path = format!(
        "{}/metadata/manifest-list-{}.avro",
        target_url,
        Uuid::new_v4()
    );
    let manifest_file_output = file_io.new_output(manifest_list_path.clone())?;
    let mut manifest_list_writer: ManifestListWriter = ManifestListWriter::v2(
        manifest_file_output,
        snapshot_id,
        -1, // parent_snapshot_id is optional. Ideally ManifestListWriter wouldn't write it at all
        sequence_number,
    );
    manifest_list_writer.add_manifests(vec![manifest_file].into_iter())?;
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
            other: HashMap::new(),
        })
        .build();

    let metadata_v1 = create_metadata_v1(&metadata_v0, snapshot)?;
    let metadata_v1_location = format!("{}/metadata/v1.metadata.json", target_url,);

    file_io
        .new_output(&metadata_v1_location)?
        .write(serde_json::to_vec(&metadata_v1).unwrap().into())
        .await?;
    info!("Wrote v1 metadata: {:?}", metadata_v1_location);

    Ok(())
}
