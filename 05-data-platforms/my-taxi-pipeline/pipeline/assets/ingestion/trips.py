"""@bruin

name: ingestion.trips

type: python

image: python:3.11

connection: duckdb_default

materialization:
  type: table
  strategy: append

columns:
  - name: vendor_id
    type: integer
    description: ID of the taxi vendor
  - name: rate_code_id
    type: integer
    description: Rate code ID
  - name: pickup_location_id
    type: integer
    description: Pickup location ID
  - name: dropoff_location_id
    type: integer
    description: Dropoff location ID
  - name: pickup_datetime
    type: timestamp
    description: Pickup datetime
  - name: dropoff_datetime
    type: timestamp
    description: Dropoff datetime
  - name: store_and_fwd_flag
    type: varchar
    description: Store and forward flag
  - name: passenger_count
    type: integer
    description: Number of passengers
  - name: trip_distance
    type: float
    description: Trip distance
  - name: fare_amount
    type: float
    description: Fare amount
  - name: extra
    type: float
    description: Extra charges
  - name: mta_tax
    type: float
    description: MTA tax
  - name: tip_amount
    type: float
    description: Tip amount
  - name: tolls_amount
    type: float
    description: Tolls amount
  - name: improvement_surcharge
    type: float
    description: Improvement surcharge
  - name: total_amount
    type: float
    description: Total amount
  - name: payment_type
    type: integer
    description: Payment type code
  - name: service_type
    type: varchar
    description: yellow or green
  - name: extracted_at
    type: timestamp
    description: When this row was extracted

@bruin"""

import json
import os
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime


COLUMN_MAP = {
    # green
    "vendorid": "vendor_id",
    "ratecodeid": "rate_code_id",
    "pulocationid": "pickup_location_id",
    "dolocationid": "dropoff_location_id",
    "lpep_pickup_datetime": "pickup_datetime",
    "lpep_dropoff_datetime": "dropoff_datetime",
    # yellow
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
}

KEEP_COLUMNS = [
    "vendor_id", "rate_code_id", "pickup_location_id", "dropoff_location_id",
    "pickup_datetime", "dropoff_datetime", "store_and_fwd_flag", "passenger_count",
    "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "payment_type", "service_type", "extracted_at",
]


def materialize():
    taxi_types = json.loads(os.environ.get("BRUIN_VARS", "{}")).get("taxi_types", ["yellow", "green"])
    start_date = os.environ.get("BRUIN_START_DATE", "2024-01-01")

    year = start_date[:4]
    month = start_date[5:7]

    frames = []
    for taxi_type in taxi_types:
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{taxi_type}_tripdata_{year}-{month}.csv.gz"
        response = requests.get(url)
        response.raise_for_status()
        df = pd.read_csv(BytesIO(response.content), compression="gzip")
        df.columns = [c.lower().strip() for c in df.columns]
        df = df.rename(columns=COLUMN_MAP)
        df["service_type"] = taxi_type
        df["extracted_at"] = datetime.utcnow()
        # keep only columns we care about.
        df = df[[c for c in KEEP_COLUMNS if c in df.columns]]
        frames.append(df)

    return pd.concat(frames, ignore_index=True)