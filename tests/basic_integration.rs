use std::vec;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::DataType;
use clap::Parser;
use futures::{StreamExt, TryStreamExt};
use iceberg::spec::TableMetadata;
use lakehouse_loader::delta_destination::object_store_keys_from_env;
use lakehouse_loader::error::DataLoadingError;
use lakehouse_loader::pg_arrow_source::PgArrowSource;
use lakehouse_loader::{do_main, Cli};
use object_store::path::Path;
use regex::Regex;
use url::Url;

#[tokio::test]
async fn test_pg_to_delta_e2e() {
    let target_url = "s3://lhl-test-bucket/delta";
    // WHEN valid arguments are passed to the command
    let parsed_args = Cli::parse_from(vec![
        "lakehouse-loader",
        "pg-to-delta",
        "postgres://test-user:test-password@localhost:5432/test-db",
        "-q",
        "select * from t1 order by id",
        target_url,
    ]);
    // THEN the command runs successfully
    do_main(parsed_args).await.unwrap();

    let config = object_store_keys_from_env("s3");

    let (store, path) =
        object_store::parse_url_opts(&Url::parse(target_url).unwrap(), config).unwrap();

    let mut paths = store
        .list(Some(&path))
        .map_ok(|m| m.location)
        .boxed()
        .try_collect::<Vec<Path>>()
        .await
        .unwrap();
    paths.sort();

    assert_eq!(paths.len(), 3);
    // THEN delta log files are written
    assert_eq!(
        paths[0].to_string(),
        "delta/_delta_log/00000000000000000000.json"
    );
    assert_eq!(
        paths[1].to_string(),
        "delta/_delta_log/00000000000000000001.json"
    );
    // THEN a delta content file is written
    assert!(paths[2].to_string().starts_with("delta/part-00000-"));
    assert!(paths[2].to_string().ends_with("-c000.snappy.parquet"));
}

const DATA_FILEPATH_PATTERN: &str = r"^iceberg/data/part-00000-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}.parquet$";
const MANIFEST_FILEPATH_PATTERN: &str = r"^iceberg/metadata/manifest-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}.avro$";
const MANIFEST_LIST_FILEPATH_PATTERN: &str = r"^iceberg/metadata/manifest-list-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}.avro$";

#[tokio::test]
async fn test_pg_to_iceberg() {
    let target_url = "s3://lhl-test-bucket/iceberg";
    // WHEN valid arguments are passed to the command
    let args = vec![
        "lakehouse-loader",
        "pg-to-iceberg",
        "postgres://test-user:test-password@localhost:5432/test-db",
        "-q",
        // We have to cherry-pick fields as not all types are supported by iceberg
        "select cint4, cint8, ctext, cbool from t1 order by id",
        target_url,
    ];
    // THEN the command runs successfully
    do_main(Cli::parse_from(args.clone())).await.unwrap();

    let config = object_store_keys_from_env("s3");

    let (store, path) =
        object_store::parse_url_opts(&Url::parse(target_url).unwrap(), config).unwrap();

    // THEN iceberg data and metadata files are written
    let mut paths = store
        .list(Some(&path))
        .map_ok(|m| m.location)
        .boxed()
        .try_collect::<Vec<Path>>()
        .await
        .unwrap();
    paths.sort();
    assert_eq!(paths.len(), 5);
    assert!(Regex::new(DATA_FILEPATH_PATTERN)
        .unwrap()
        .is_match(paths[0].as_ref()));
    assert!(Regex::new(MANIFEST_FILEPATH_PATTERN)
        .unwrap()
        .is_match(paths[1].as_ref()));
    assert!(Regex::new(MANIFEST_LIST_FILEPATH_PATTERN)
        .unwrap()
        .is_match(paths[2].as_ref()));
    assert_eq!(&paths[3].to_string(), "iceberg/metadata/v0.metadata.json");
    assert_eq!(&paths[4].to_string(), "iceberg/metadata/version-hint.text");
    // THEN iceberg metadata can be parsed
    let metadata_bytes = store.get(&paths[3]).await.unwrap().bytes().await.unwrap();
    let metadata_str = core::str::from_utf8(&metadata_bytes).unwrap();
    let metadata = serde_json::from_str::<TableMetadata>(metadata_str).unwrap();
    // THEN metadata contains a single snapshot with sequence number 1
    assert_eq!(metadata.last_sequence_number(), 1);
    assert_eq!(
        metadata
            .snapshots()
            .map(|s| s.sequence_number())
            .collect::<Vec<_>>(),
        vec![1]
    );

    // WHEN we try to write to an existing table without passing the overwrite flag
    // THEN the command errors out
    let args = vec![
        "lakehouse-loader",
        "pg-to-iceberg",
        "postgres://test-user:test-password@localhost:5432/test-db",
        "-q",
        "select cint4, cint8 + 1 cint8, ctext, cbool from t1 order by id",
        target_url,
    ];
    match do_main(Cli::parse_from(args.clone())).await {
        Err(DataLoadingError::IoError(e)) => {
            assert!(e.kind() == std::io::ErrorKind::Other);
        }
        Err(e) => {
            panic!("Unexpected error type: {:?}", e);
        }
        Ok(_) => panic!("Expected command to fail but it succeeded"),
    };

    // WHEN we try to write to an existing table with a different schema
    // THEN the command errors out
    let args = vec![
        "lakehouse-loader",
        "pg-to-iceberg",
        "postgres://test-user:test-password@localhost:5432/test-db",
        "-q",
        "select cint4, cint8 cint8_newname, ctext, cbool from t1 order by id",
        target_url,
        "--overwrite",
    ];
    match do_main(Cli::parse_from(args.clone())).await {
        Err(DataLoadingError::IcebergError(e)) => {
            assert!(e.kind() == iceberg::ErrorKind::FeatureUnsupported);
        }
        Err(e) => {
            panic!("Unexpected error type: {:?}", e);
        }
        Ok(_) => panic!("Expected command to fail but it succeeded"),
    };

    // WHEN we try to write to an existing table with the same schema
    // THEN the command succeeds
    let args = vec![
        "lakehouse-loader",
        "pg-to-iceberg",
        "postgres://test-user:test-password@localhost:5432/test-db",
        "-q",
        "select cint4, cint8 + 1 cint8, ctext, cbool from t1 order by id",
        target_url,
        "--overwrite",
    ];
    assert!(do_main(Cli::parse_from(args.clone())).await.is_ok());

    // THEN iceberg data and metadata files are written
    let mut paths = store
        .list(Some(&path))
        .map_ok(|m| m.location)
        .boxed()
        .try_collect::<Vec<Path>>()
        .await
        .unwrap();
    paths.sort();
    assert_eq!(paths.len(), 9);
    assert!(Regex::new(DATA_FILEPATH_PATTERN)
        .unwrap()
        .is_match(paths[0].as_ref()));
    assert!(Regex::new(DATA_FILEPATH_PATTERN)
        .unwrap()
        .is_match(paths[1].as_ref()));
    assert!(Regex::new(MANIFEST_FILEPATH_PATTERN)
        .unwrap()
        .is_match(paths[2].as_ref()));
    assert!(Regex::new(MANIFEST_FILEPATH_PATTERN)
        .unwrap()
        .is_match(paths[3].as_ref()));
    assert!(Regex::new(MANIFEST_LIST_FILEPATH_PATTERN)
        .unwrap()
        .is_match(paths[4].as_ref()));
    assert!(Regex::new(MANIFEST_LIST_FILEPATH_PATTERN)
        .unwrap()
        .is_match(paths[5].as_ref()));
    assert_eq!(&paths[6].to_string(), "iceberg/metadata/v0.metadata.json");
    assert_eq!(&paths[7].to_string(), "iceberg/metadata/v1.metadata.json");
    assert_eq!(&paths[8].to_string(), "iceberg/metadata/version-hint.text");
    // THEN iceberg metadata can be parsed
    let metadata_bytes = store.get(&paths[7]).await.unwrap().bytes().await.unwrap();
    let metadata_str = core::str::from_utf8(&metadata_bytes).unwrap();
    let metadata = serde_json::from_str::<TableMetadata>(metadata_str).unwrap();
    // THEN metadata contains two snapshots with sequence numbers 1 and 2
    assert_eq!(metadata.last_sequence_number(), 2);
    let mut snapshot_ids = metadata
        .snapshots()
        .map(|s| s.sequence_number())
        .collect::<Vec<_>>();
    snapshot_ids.sort();
    assert_eq!(snapshot_ids, vec![1, 2]);
}

#[tokio::test]
async fn test_pg_arrow_source() {
    // WHEN 25001 rows are split into batches of 10000
    let record_batches: Vec<_> = PgArrowSource::new(
        "postgres://test-user:test-password@localhost:5432/test-db",
        "select * from t1 order by id",
        10000,
    )
    .await
    .unwrap()
    .get_record_batch_stream()
    .collect()
    .await;

    // THEN there should be 3 batches
    assert_eq!(record_batches.len(), 3);
    // THEN the first batch should have 10000 rows
    assert_eq!(record_batches[0].as_ref().unwrap().num_rows(), 10000);
    // THEN the second batch should have 10000 rows
    assert_eq!(record_batches[1].as_ref().unwrap().num_rows(), 10000);
    // THEN the third batch should have 5001 rows
    assert_eq!(record_batches[2].as_ref().unwrap().num_rows(), 5001);

    let rb1 = record_batches[0].as_ref().unwrap();

    // THEN the first 3 id values should be as expected
    let id_array = rb1.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(!id_array.is_null(0));
    assert_eq!(id_array.value(0), 1);
    assert!(!id_array.is_null(1));
    assert_eq!(id_array.value(1), 2);
    assert!(!id_array.is_null(2));
    assert_eq!(id_array.value(2), 3);

    // THEN the first 3 bool values should be as expected
    let cbool_array = rb1
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(cbool_array.is_null(0));
    assert!(!cbool_array.is_null(1));
    assert!(cbool_array.value(1));
    assert!(!cbool_array.is_null(2));
    assert!(!cbool_array.value(2));

    // THEN the first 3 "char" values should be as expected
    let cchar_array = rb1.column(2).as_any().downcast_ref::<Int8Array>().unwrap();
    assert!(cchar_array.is_null(0));
    assert!(!cchar_array.is_null(1));
    assert_eq!(cchar_array.value(1), -127);
    assert!(!cchar_array.is_null(2));
    assert_eq!(cchar_array.value(2), -126);

    // THEN the first 3 int2 values should be as expected
    let cint2_array = rb1.column(3).as_any().downcast_ref::<Int16Array>().unwrap();
    assert!(cint2_array.is_null(0));
    assert!(!cint2_array.is_null(1));
    assert_eq!(cint2_array.value(1), 1);
    assert!(!cint2_array.is_null(2));
    assert_eq!(cint2_array.value(2), 2);

    // THEN the first 3 int4 values should be as expected
    let cint4_array = rb1.column(4).as_any().downcast_ref::<Int32Array>().unwrap();
    assert!(cint4_array.is_null(0));
    assert!(!cint4_array.is_null(1));
    assert_eq!(cint4_array.value(1), 1);
    assert!(!cint4_array.is_null(2));
    assert_eq!(cint4_array.value(2), 2);

    // THEN the first 3 int8 values should be as expected
    let cint8_array = rb1.column(5).as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(cint8_array.is_null(0));
    assert!(!cint8_array.is_null(1));
    assert_eq!(cint8_array.value(1), 1);
    assert!(!cint8_array.is_null(2));
    assert_eq!(cint8_array.value(2), 2);

    // THEN the first 3 float4 values should be as expected
    let cfloat4_array = rb1
        .column(6)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert!(cfloat4_array.is_null(0));
    assert!(!cfloat4_array.is_null(1));
    assert_eq!(cfloat4_array.value(1), 1.5);
    assert!(!cfloat4_array.is_null(2));
    assert_eq!(cfloat4_array.value(2), 2.5);

    // THEN the first 3 float8 values should be as expected
    let cfloat8_array = rb1
        .column(7)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!(cfloat8_array.is_null(0));
    assert!(!cfloat8_array.is_null(1));
    assert_eq!(cfloat8_array.value(1), 1.5);
    assert!(!cfloat8_array.is_null(2));
    assert_eq!(cfloat8_array.value(2), 2.5);

    // Days between unix epoch (1970-01-01) and 2024-01-01
    let elapsed_days = 19723;
    let seconds_per_day = 86400;

    // THEN the first 3 timestamp values should be as expected
    let ctimestamp_array = rb1
        .column(8)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert!(ctimestamp_array.is_null(0));
    assert!(!ctimestamp_array.is_null(1));
    assert_eq!(
        ctimestamp_array.value(1),
        (elapsed_days * seconds_per_day + 1) * 1000000
    );
    assert!(!ctimestamp_array.is_null(2));
    assert_eq!(
        ctimestamp_array.value(2),
        (elapsed_days * seconds_per_day + 2) * 1000000
    );

    // THEN the first 3 timestamptz values should be as expected
    let ctimestamptz_array = rb1
        .column(9)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert!(ctimestamptz_array.is_null(0));
    assert!(!ctimestamptz_array.is_null(1));
    assert_eq!(
        ctimestamptz_array.value(1),
        (elapsed_days * seconds_per_day + 1) * 1000000
    );
    assert!(!ctimestamptz_array.is_null(2));
    assert_eq!(
        ctimestamptz_array.value(2),
        (elapsed_days * seconds_per_day + 2) * 1000000
    );

    // THEN the first 3 date values should be as expected
    let cdate_array = rb1
        .column(10)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert!(cdate_array.is_null(0));
    assert!(!cdate_array.is_null(1));
    assert_eq!(cdate_array.value(1), elapsed_days as i32 + 1);
    assert!(!cdate_array.is_null(2));
    assert_eq!(cdate_array.value(2), elapsed_days as i32 + 2);

    // THEN the numeric field data type should be as expected
    assert_eq!(
        *rb1.schema().field(11).data_type(),
        DataType::Decimal128(8, 3)
    );

    // THEN the first few numeric values should be as expected
    let cnumeric_array = rb1
        .column(11)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert!(cnumeric_array.is_null(0));
    assert!(!cnumeric_array.is_null(1));
    assert_eq!(cnumeric_array.value(1), 0_i128);
    assert!(!cnumeric_array.is_null(2));
    assert_eq!(cnumeric_array.value(2), 1_i128);
    assert!(!cnumeric_array.is_null(3));
    assert_eq!(cnumeric_array.value(3), -2_i128);
    assert!(!cnumeric_array.is_null(4));
    assert_eq!(cnumeric_array.value(4), 3000_i128);
    assert!(!cnumeric_array.is_null(5));
    assert_eq!(cnumeric_array.value(5), -4000_i128);
    assert!(!cnumeric_array.is_null(6));
    assert_eq!(cnumeric_array.value(6), 50001_i128);
    assert!(!cnumeric_array.is_null(7));
    assert_eq!(cnumeric_array.value(7), 99999999_i128);
    assert!(!cnumeric_array.is_null(8));
    assert_eq!(cnumeric_array.value(8), -99999999_i128);

    // THEN the first 3 text values should be as expected
    let ctext_array = rb1
        .column(12)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(ctext_array.is_null(0));
    assert!(!ctext_array.is_null(1));
    assert_eq!(ctext_array.value(1), "1");
    assert!(!ctext_array.is_null(2));
    assert_eq!(ctext_array.value(2), "2");

    // THEN the first 3 bytea values should be as expected
    let cbytea_array = rb1
        .column(13)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert!(cbytea_array.is_null(0));
    assert!(!cbytea_array.is_null(1));
    assert_eq!(cbytea_array.value(1), [0, 0, 0, 1]);
    assert!(!cbytea_array.is_null(2));
    assert_eq!(cbytea_array.value(2), [0, 0, 0, 2]);
}
