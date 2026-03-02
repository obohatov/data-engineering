"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: trip pickup timestamp
  - name: dropoff_datetime
    type: timestamp
    description: trip dropoff timestamp
  - name: pickup_location_id
    type: integer
    description: pickup zone id
  - name: dropoff_location_id
    type: integer
    description: dropoff zone id
  - name: payment_type
    type: integer
    description: payment type code
  - name: fare_amount
    type: double
    description: fare amount
  - name: taxi_type
    type: string
    description: taxi service type
@bruin"""

import io
import json
import os
from typing import List

import pandas as pd
import requests


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def month_starts(start_date: str, end_date: str) -> List[pd.Timestamp]:
    start = pd.Timestamp(start_date).replace(day=1)
    end = pd.Timestamp(end_date).replace(day=1)

    months = []
    current = start
    while current < end:
        months.append(current)
        current = current + pd.offsets.MonthBegin(1)

    return months


def first_existing(df: pd.DataFrame, names, required=True):
    for name in names:
        if name in df.columns:
            return name
    if required:
        raise KeyError(f"None of these columns were found: {names}")
    return None


def load_parquet_from_url(url: str) -> pd.DataFrame:
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    return pd.read_parquet(io.BytesIO(response.content))


def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    vars_payload = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = vars_payload.get("taxi_types", ["yellow"])

    frames = []

    for month_start in month_starts(start_date, end_date):
        ym = month_start.strftime("%Y-%m")

        for taxi_type in taxi_types:
            url = f"{BASE_URL}/{taxi_type}_tripdata_{ym}.parquet"
            df = load_parquet_from_url(url)

            pickup_col = first_existing(df, ["tpep_pickup_datetime", "lpep_pickup_datetime"])
            dropoff_col = first_existing(df, ["tpep_dropoff_datetime", "lpep_dropoff_datetime"])
            pu_col = first_existing(df, ["PULocationID"], required=False)
            do_col = first_existing(df, ["DOLocationID"], required=False)
            payment_col = first_existing(df, ["payment_type"], required=False)
            fare_col = first_existing(df, ["fare_amount"], required=False)

            out = pd.DataFrame(
                {
                    "pickup_datetime": pd.to_datetime(df[pickup_col], errors="coerce"),
                    "dropoff_datetime": pd.to_datetime(df[dropoff_col], errors="coerce"),
                    "pickup_location_id": df[pu_col] if pu_col else pd.NA,
                    "dropoff_location_id": df[do_col] if do_col else pd.NA,
                    "payment_type": df[payment_col] if payment_col else pd.NA,
                    "fare_amount": df[fare_col] if fare_col else pd.NA,
                    "taxi_type": taxi_type,
                }
            )

            out = out[
                (out["pickup_datetime"] >= pd.Timestamp(start_date))
                & (out["pickup_datetime"] < pd.Timestamp(end_date))
            ]

            frames.append(out)

    if not frames:
        return pd.DataFrame(
            columns=[
                "pickup_datetime",
                "dropoff_datetime",
                "pickup_location_id",
                "dropoff_location_id",
                "payment_type",
                "fare_amount",
                "taxi_type",
            ]
        )

    result = pd.concat(frames, ignore_index=True)
    result = result.dropna(subset=["pickup_datetime"])
    return result
    