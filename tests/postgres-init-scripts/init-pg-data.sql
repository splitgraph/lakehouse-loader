CREATE TABLE t1(
  id BIGSERIAL PRIMARY KEY,
  cbool BOOLEAN,
  cchar "char",
  cint2 SMALLINT,
  cint4 INT,
  cint8 BIGINT,
  cfloat4 REAL,
  cfloat8 DOUBLE PRECISION,
  ctimestamp TIMESTAMP,
  ctimestamptz TIMESTAMPTZ,
  ctext TEXT,
  cbytea BYTEA
);
-- Create a row of nulls
INSERT INTO t1 DEFAULT VALUES;
-- Create enough data for multiple record batches
INSERT INTO t1(
  cbool,
  cchar,
  cint2,
  cint4,
  cint8,
  cfloat4,
  cfloat8,
  ctimestamp,
  ctimestamptz,
  ctext,
  cbytea
) SELECT
  s % 2 = 1,
  (s % 256 - 128)::"char",
  s,
  s,
  s,
  s + 0.5,
  s + 0.5,
  '2024-01-01'::TIMESTAMP + s * INTERVAL '1 second',
  '2024-01-01 00:00:00+00'::TIMESTAMPTZ + s * INTERVAL '1 second',
  s::TEXT,
  int4send(s::INT)
FROM generate_series(1, 25000) AS s;
