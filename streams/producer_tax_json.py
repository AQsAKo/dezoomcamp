import csv
from json import dumps
from kafka import KafkaProducer
from time import sleep


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         key_serializer=lambda x: dumps(x).encode('utf-8'),
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

file = open('../data/fhv_tripdata_2019-01.csv')

csvreader = csv.reader(file)
header = next(csvreader)
for row in csvreader:
    value = {"PULocationID": int(row[3])}
    producer.send('dtc.ny_taxi.json', value=value)
    print("producing")
    sleep(1)