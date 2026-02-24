
"""@bruin

# Ingest raw NYC TLC trip parquet files into a DuckDB table.
# - Asset name follows the convention: `ingestion.<asset>`
name: ingestion.trips

# Python asset that returns a DataFrame to be materialized by Bruin
type: python

# Use a lightweight Python image; Bruin runs this in an isolated container
image: python:3.11

# Destination connection (defined in .bruin.yml)
connection: duckdb-default

# Materialize as an append-only table (raw ingestion layer)
materialization:
  type: table
  strategy: append

# Define columns for metadata/lineage/quality checks
columns:
  - name: vendor_id
    type: integer
    description: VendorID from TLC data
  - name: tpep_pickup_datetime
    type: timestamp
    description: pickup timestamp
  - name: tpep_dropoff_datetime
    type: timestamp
    description: dropoff timestamp
  - name: passenger_count
    type: integer
    description: number of passengers
  - name: trip_distance
    type: float
    description: trip distance in miles
  - name: pulocationid
    type: integer
    description: pickup location id
  - name: dolocationid
    type: integer
    description: dropoff location id
  - name: payment_type
    type: integer
    description: payment type code
  - name: fare_amount
    type: float
    description: fare amount
  - name: extracted_at
    type: timestamp
    description: ingestion timestamp added by this asset

@bruin"""

import os
import json
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import pandas as pd


def _parse_vars():
    raw = os.environ.get("BRUIN_VARS")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _month_range(start: date, end: date):
    cur = date(start.year, start.month, 1)
    end_month = date(end.year, end.month, 1)
    while cur <= end_month:
        yield cur.year, cur.month
        cur = cur + relativedelta(months=1)


def materialize():
    """Fetch monthly TLC parquet files and return a concatenated DataFrame.

    Behavior:
    - Reads `BRUIN_START_DATE` and `BRUIN_END_DATE` (YYYY-MM-DD). Falls back to a single month.
    - Reads `BRUIN_VARS` JSON and looks for `taxi_types` (e.g. ["yellow", "fhv"]). Defaults to ["yellow"].
    - For each month and taxi type attempts to read the well-known public parquet URL and concatenates results.
    - Returns a DataFrame with a stable column set and an `extracted_at` timestamp column.

    Notes:
    - This implementation prefers simplicity and clear schema mapping for ingestion. Deduplication
      and heavy cleaning should be performed in the staging layer.
    """
    start_str = os.environ.get("BRUIN_START_DATE")
    end_str = os.environ.get("BRUIN_END_DATE")
    today = date.today()

    if start_str and end_str:
        start = datetime.strptime(start_str, "%Y-%m-%d").date()
        end = datetime.strptime(end_str, "%Y-%m-%d").date()
    else:
        # default: ingest current month
        start = date(today.year, today.month, 1)
        end = start

    vars = _parse_vars()
    taxi_types = vars.get("taxi_types") or ["yellow"]

    frames = []
    for taxi in taxi_types:
        for y, m in _month_range(start, end):
            fname = f"{taxi}_tripdata_{y}-{m:02d}.parquet"
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{fname}"
            try:
                df = pd.read_parquet(url, engine="pyarrow")
            except Exception:
                # skip missing months or read errors gracefully
                continue

            # Normalize common columns (many TLC datasets use similar names but differ by taxi type/year)
            colmap = {
                "VendorID": "vendor_id",
                "vendorid": "vendor_id",
                "tpep_pickup_datetime": "tpep_pickup_datetime",
                "pickup_datetime": "tpep_pickup_datetime",
                "tpep_dropoff_datetime": "tpep_dropoff_datetime",
                "dropoff_datetime": "tpep_dropoff_datetime",
                "passenger_count": "passenger_count",
                "passengers": "passenger_count",
                "trip_distance": "trip_distance",
                "PULocationID": "pulocationid",
                "pulocationid": "pulocationid",
                "DOLocationID": "dolocationid",
                "dolocationid": "dolocationid",
                "payment_type": "payment_type",
                "Payment_Type": "payment_type",
                "fare_amount": "fare_amount",
            }
            df = df.rename(columns={k: v for k, v in colmap.items() if k in df.columns})

            # Select columns we declared in the header if present; otherwise create with nulls
            out_cols = [
                "vendor_id",
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "passenger_count",
                "trip_distance",
                "pulocationid",
                "dolocationid",
                "payment_type",
                "fare_amount",
            ]

            out = pd.DataFrame()
            for c in out_cols:
                if c in df.columns:
                    out[c] = df[c]
                else:
                    out[c] = pd.NA

            out["extracted_at"] = datetime.utcnow()
            frames.append(out)

    if frames:
        result = pd.concat(frames, ignore_index=True)
    else:
        # Return an empty DataFrame with defined schema
        result = pd.DataFrame(columns=[
            "vendor_id",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "pulocationid",
            "dolocationid",
            "payment_type",
            "fare_amount",
            "extracted_at",
        ])

    return result
