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
  cdate DATE,
  cnumeric NUMERIC(8, 3),
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
  cdate,
  cnumeric,
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
  '2024-01-01'::DATE + s,
  s::NUMERIC / 1000,
  s::TEXT,
  int4send(s::INT)
FROM generate_series(1, 25000) AS s;

-- Set various cnumeric values
UPDATE t1 SET cnumeric = 0 WHERE id = 2;
UPDATE t1 SET cnumeric = 0.001 WHERE id = 3;
UPDATE t1 SET cnumeric = -0.002 WHERE id = 4;
UPDATE t1 SET cnumeric = 3 WHERE id = 5;
UPDATE t1 SET cnumeric = -4 WHERE id = 6;
UPDATE t1 SET cnumeric = 50.001 WHERE id = 7;
UPDATE t1 SET cnumeric = 99999.999 WHERE id = 8;
UPDATE t1 SET cnumeric = -99999.999 WHERE id = 9;
