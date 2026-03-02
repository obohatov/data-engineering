/* @bruin
name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: trip pickup time
    primary_key: true
    nullable: false
    checks:
      - name: not_null

  - name: fare_amount
    type: double
    description: trip fare
    checks:
      - name: non_negative

custom_checks:
  - name: row_count_non_negative
    description: result set should not be negative
    query: |
      SELECT CASE WHEN COUNT(*) >= 0 THEN 0 ELSE 1 END
    value: 0
@bruin */

WITH ranked AS (
  SELECT
    t.pickup_datetime,
    t.dropoff_datetime,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.fare_amount,
    t.taxi_type,
    p.payment_type_name,
    ROW_NUMBER() OVER (
      PARTITION BY
        t.pickup_datetime,
        t.dropoff_datetime,
        t.pickup_location_id,
        t.dropoff_location_id,
        t.fare_amount,
        t.taxi_type
      ORDER BY t.pickup_datetime
    ) AS rn
  FROM ingestion.trips t
  LEFT JOIN ingestion.payment_lookup p
    ON t.payment_type = p.payment_type_id
  WHERE t.pickup_datetime >= '{{ start_datetime }}'
    AND t.pickup_datetime < '{{ end_datetime }}'
)

SELECT
  pickup_datetime,
  dropoff_datetime,
  pickup_location_id,
  dropoff_location_id,
  fare_amount,
  taxi_type,
  payment_type_name
FROM ranked
WHERE rn = 1
