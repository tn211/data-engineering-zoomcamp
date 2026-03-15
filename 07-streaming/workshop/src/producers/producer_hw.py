import json
import math
import time
import pandas as pd
from kafka import KafkaProducer

data = '/Users/benzenesea/Desktop/zoomcamp/data-engineering-zoomcamp/homework/supplements/green_tripdata_2025-10.parquet'

columns = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount',
]

df = pd.read_parquet(data, columns=columns)

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

topic_name = 'green-trips'

t0 = time.time()

for _, row in df.iterrows():
    row_dict = row.to_dict()
    row_dict['lpep_pickup_datetime'] = str(row_dict['lpep_pickup_datetime'])
    row_dict['lpep_dropoff_datetime'] = str(row_dict['lpep_dropoff_datetime'])
    for key, value in row_dict.items():
        if isinstance(value, float) and math.isnan(value):
            row_dict[key] = 0.0
    producer.send(topic_name, value=row_dict)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')