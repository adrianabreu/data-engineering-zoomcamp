import json
import time 

from kafka import KafkaProducer
import pandas as pd


def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

print(producer.bootstrap_connected())

t0 = time.time()

topic_name = 'green-trips'

csv_name = 'green_tripdata_2019-10.csv.gz'
df_green = pd.read_csv(csv_name)[['lpep_pickup_datetime','lpep_dropoff_datetime','PULocationID','DOLocationID','passenger_count','trip_distance','tip_amount']]
for row in df_green.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    producer.send(topic_name, value=row_dict)

  

t1 = time.time()

producer.flush()

t2 = time.time()
print(f'took {(t1 - t0):.2f} seconds on sending messages')
print(f'took {(t2 - t1):.2f} seconds on flushing')