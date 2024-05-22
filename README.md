# Lakehouse Loader

Load data from Parquet and Postgres to Delta Lake

## Features

- Supports S3 and file output
- Supports larger-than-memory source data

## Usage

Download the binary from the [Releases page](./releases)

To load data from Postgres to Delta Lake:

```bash
export PGPASSWORD="my_password"
./lakehouse-loader pg-to-delta postgres://test-user@localhost:5432/test-db -q "SELECT * FROM some_table" s3://my-bucket/path/to/table
```

To load data from Parquet to Delta Lake:

```bash
./lakehouse-loader parquet-to-delta some_file.parquet s3://my-bucket/path/to/table
```

Supports standard AWS environment variables (e.g. AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_PROFILE, AWS_ENDPOINT etc).

Use the `file://` protocol to load data into a file instead.

## Limitations

- Supported datatypes: bool, char, int2, int4, int8, float4, float8, timestamp, timestamptz, text, bytea. Cast the columns in your query to `text` or another supported type if your query returns different types
- Doesn't support appending to tables, only writing new Delta Tables (pass `-o` to overwrite)
