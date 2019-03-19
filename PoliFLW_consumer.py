from kafka import KafkaConsumer
from datetime import datetime
from elasticsearch import Elasticsearch
import time
import json

es = Elasticsearch()

if __name__ == '__main__':
    print('Running Consumer...')
    topic_name = 'single-poliflow-data'

    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)

    esid = 0

    for message in consumer:
        time.sleep(1)
        print("Next")
        esid += 1
        if esid % 1000 == 0:
            print(esid)

        msg = json.loads(message.value)
        print(msg)

        # if not 'partij' in msg:
        #     print("you must specify the party in the json wrapper")
        #     sys.exit(-1)
        
        # if 'description' in msg:
        #     esid = 1
        #     if es.indices.exists()
        