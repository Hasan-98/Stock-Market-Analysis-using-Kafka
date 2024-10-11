import pandas as pd
from kafka import KafkaProducer
from json import dumps
import json
from time import sleep

producer = KafkaProducer(bootstrap_servers=['3.81.162.67:9092']
                            , value_serializer=lambda x: dumps(x).encode('utf-8'))


df = pd.read_csv('indexProcessed.csv')
while True:
    dict_stock = df.sample(1).to_dict(orient='records')[0]
    producer.send('demo_test', value=dict_stock)
    sleep(1)