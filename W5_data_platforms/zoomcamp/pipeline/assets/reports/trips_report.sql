/* @bruin
name: reports.trips_report

# Report aggregates daily metrics by payment type
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table

columns:
  - name: taxi_type
    type: string
    description: Taxi type (e.g., yellow, fhv)
    primary_key: true
  - name: payment_type_name
    type: string
    description: Payment type (e.g., Credit card, Cash)
    primary_key: true
  - name: day
    type: date
    description: Date of the pickup (day)
    primary_key: true
  - name: trip_count
    type: bigint
    description: Number of trips in the day for the group
    checks:
      - name: non_negative
  - name: total_passengers
    type: bigint
    description: Sum of passenger_count for the group
  - name: total_distance
    type: double
    description: Sum of trip_distance (miles)
  - name: total_fare
    type: double
    description: Sum of fare_amount for the group
  - name: total_tips
    type: double
    description: Sum of tip_amount for the group
  - name: total_revenue
    type: double
    description: Sum of total_amount (fare + tips) for the group
  - name: avg_fare
    type: double
    description: Average fare per trip
  - name: avg_trip_distance
    type: double
    description: Average trip distance
  - name: avg_passengers
    type: double
    description: Average passengers per trip

@bruin */

SELECT
  CAST(tpep_pickup_datetime AS DATE) AS trip_date,
  COALESCE(NULLIF(taxi_type, ''), 'UNKNOWN') AS taxi_type,
  COALESCE(payment_type_name, 'UNKNOWN') AS payment_type_name,
  COUNT(*) AS trip_count,
  SUM(COALESCE(passenger_count, 0)) AS total_passengers,
  SUM(COALESCE(trip_distance, 0)) AS total_distance,
  SUM(COALESCE(fare_amount, 0)) AS total_fare,
  SUM(COALESCE(tip_amount, 0)) AS total_tips,
  SUM(COALESCE(total_amount, COALESCE(fare_amount, 0) + COALESCE(tip_amount, 0))) AS total_revenue,
  AVG(COALESCE(fare_amount, 0)) AS avg_fare,
  AVG(COALESCE(trip_distance, 0)) AS avg_trip_distance,
  AVG(COALESCE(passenger_count, 0)) AS avg_passengers
FROM staging.trips
WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
  AND tpep_pickup_datetime < '{{ end_datetime }}'
GROUP BY trip_date, taxi_type, payment_type_name
ORDER BY trip_date, taxi_type, payment_type_name
