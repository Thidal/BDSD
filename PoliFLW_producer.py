import pandas as pd
import numpy as np
import requests
import json
import re
from collections import Counter
from time import sleep
from bs4 import BeautifulSoup
from kafka import KafkaConsumer, KafkaProducer

# import nltk
# from nltk.corpus import stopwords
# stop_words = set(stopwords.words('Dutch'))
# extra_stop_words = ['waar', 'onze', 'weer', 'daarom']
# stop_words.update(extra_stop_words)
# from nltk.stem.snowball import SnowballStemmer
# stemmer = SnowballStemmer('Dutch')

# from nltk.tag.perceptron import PerceptronTagger
# import os
# os.chdir(r, '')
# tagger = PerceptronTagger(load=False)
# tagger.load('model.perc.dutch_tagger_large.pickle')

def GET_POLI_DATA():
    BASE_URL = 'https://api.poliflw.nl/v0/search?scroll=1m'

    scroll_id = ''
    total_results, total_size, size = 0, 0, 100

    all_data = []
    while not total_size or total_results < total_size:
        if scroll_id:
            result = requests.get(BASE_URL + '&size=' + str(size) + '&scroll_id=' + scroll_id)
        else:
            result = requests.get(BASE_URL + '&size=' + str(size))
        
        data = result.json()

        scroll_id = data['meta']['scroll']
        total_size = data['meta']['total']

        total_results += size
        
        print('%s/%s' % (total_results, total_size))

        if 'item' in data:
            all_data += data['item']

    return all_data

def SAVE_DATA():
    with open('DataDump_ArticlesPoliFlow.json', 'w') as OUT:
        json.dump(data, OUT)

def publish_message(producer_instance, topic_name, data):
    try:
        # key_bytes = bytes(key, encoding='utf-8')
        # value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, data)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10), value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

if __name__ == '__main__':
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Pragma': 'no-cache'
    }

    kafka_producer = connect_kafka_producer()

    # data = GET_POLI_DATA()
    with open('smaller_dump.json', 'r') as data:
        data = json.load(data)
    publish_message(kafka_producer, 'poliflow-raw', data)
    if kafka_producer is not None:
        kafka_producer.close()

    # i = 0
    # kafka_producer = connect_kafka_producer()
    # while i <= 4:
    #     publish_message(kafka_producer, 'raw_recipes', 'raw', 'test')
    #     i = i + 1
    # if kafka_producer is not None:
    #     kafka_producer.close()