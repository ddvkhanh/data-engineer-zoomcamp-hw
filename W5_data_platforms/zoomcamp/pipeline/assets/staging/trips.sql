/* @bruin

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table

# Custom checks validate staging invariants after each run
custom_checks:
  - name: row_count_positive
    description: Ensure the staging table is not empty after each run.
    query: |
      SELECT COUNT(*) > 0 FROM staging.trips
    value: 1
  - name: no_negative_distance
    description: No negative trip distances should be present in staging.
    query: |
      SELECT COUNT(*) = 0 FROM staging.trips WHERE trip_distance < 0
    value: 1

@bruin */

WITH raw AS (
  SELECT
    t.vendor_id,
    CAST(t.tpep_pickup_datetime AS TIMESTAMP) AS tpep_pickup_datetime,
    CAST(t.tpep_dropoff_datetime AS TIMESTAMP) AS tpep_dropoff_datetime,
    CAST(t.passenger_count AS INTEGER) AS passenger_count,
    CAST(t.trip_distance AS DOUBLE) AS trip_distance,
    CAST(t.pulocationid AS INTEGER) AS pulocationid,
    CAST(t.dolocationid AS INTEGER) AS dolocationid,
    CAST(t.payment_type AS INTEGER) AS payment_type,
    CAST(t.fare_amount AS DOUBLE) AS fare_amount,
    t.extracted_at
  FROM ingestion.trips t
  WHERE t.tpep_pickup_datetime >= '{{ start_datetime }}'
    AND t.tpep_pickup_datetime < '{{ end_datetime }}'
    AND t.tpep_pickup_datetime IS NOT NULL
),

deduped AS (
  SELECT
    vendor_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    extracted_at
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, pulocationid, dolocationid, fare_amount
        ORDER BY extracted_at DESC
      ) AS rn
    FROM raw
  ) sub
  WHERE rn = 1
)

SELECT
  d.vendor_id,
  d.tpep_pickup_datetime,
  d.tpep_dropoff_datetime,
  d.passenger_count,
  d.trip_distance,
  d.pulocationid,
  d.dolocationid,
  d.payment_type,
  d.fare_amount,
  pl.payment_type_name,
  d.extracted_at
FROM deduped d
LEFT JOIN ingestion.payment_lookup pl
  ON d.payment_type = pl.payment_type_id
WHERE d.tpep_pickup_datetime >= '{{ start_datetime }}'
  AND d.tpep_pickup_datetime < '{{ end_datetime }}'
  -- filter obviously invalid rows
  AND (d.passenger_count IS NULL OR d.passenger_count >= 0)
  AND (d.trip_distance IS NULL OR d.trip_distance >= 0)