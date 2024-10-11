import pandas as pd
from kafka import KafkaConsumer
from json import dumps
import json
from time import sleep
from s3fs import S3FileSystem
consumer = KafkaConsumer('demo_test', bootstrap_servers=['3.81.162.67:9092']
                            , value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# for message in consumer:
#     print(message.value)
#     sleep(1)
                                                    
s3 = S3FileSystem()

for count, i in enumerate(consumer):
    with s3.open('s3://kafka-stock-market-raw-data/stock_market_{}.json'.format(count), 'w') as file:
        json.dump(i.value , file)