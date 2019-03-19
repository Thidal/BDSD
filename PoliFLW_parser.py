import json
import pandas as pd
import re
from time import sleep

from bs4 import BeautifulSoup
from kafka import KafkaConsumer, KafkaProducer


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


def parse(markup):
    return "ok"

def cleanhtml(raw_html):
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext

def computeTF(wordDict, bow):
    tfDict = {}
    bowCount = len(bow)
    for word, count in wordDict.items():
        tfDict[word] = count/float(bowCount)
    return tfDict

def JSONfile_toDataframe(json_file):
    # json_file = json_file.decode('utf-8').replace("'", '"')
    json_file = json.loads(json_file)
    print(type(json_file))
    print(json_file[0])

    last_perc = 0
    """ 
    This function converts a json-file into a pandas dataframe.
    The most important information per article will be stored in the dataframe.
    """
    
    # Empty lists to fill with article data
    dates = []
    date_granularities = []
    descriptions = []
    enrichments = []
    locations = []
    parties = []
    politicians = []
    sources = []
    titles = []
    
    all_lists = [dates,
                 date_granularities,
                 descriptions,
                 enrichments,
                 locations, 
                 parties,
                 politicians,
                 sources,
                 titles]
    
    all_searches = ['date',
                    'date_granularity',
                    'description', 
                    'enrichments',
                    'location',
                    'parties',
                    'politicians',
                    'source',
                    'title']
    
    # Loop through requested articles
    for i in range(len(json_file)):
        max_len = len(json_file)
        percentage = i / max_len * 100
        percentage = round(percentage)
        
        if percentage == last_perc:
            pass
        elif percentage != last_perc:
            print(percentage, "% Done")
            last_perc = percentage
        
        for j, searchkey in enumerate(all_searches):
            try:
                all_lists[j].append(json_file[i]['_source'][searchkey])
            except KeyError:
                all_lists[j].append(None)

    # Save as DataFrame and return
    DF_search = pd.DataFrame(
        {'date': dates,
         'date granularity': date_granularities,
         'description': descriptions,
         'enrichments': enrichments,
         'location': locations,
         'parties': parties,
         'politicians': politicians,
         'source': sources,
         'title': titles
        })
    
    return DF_search

def RefineDataframe(dataframe):
    empty_list = []

    for i in range(len(dataframe)):
        if dataframe.description[i] == None:
            empty_list.append(i)

    dataframe.drop(empty_list, inplace = True)
    dataframe = dataframe.reset_index()

    return dataframe

if __name__ == '__main__':
    print('Running Consumer...')
    parsed_records = []
    parsed_description = []
    parsed_total = []
    topic_name = 'poliflow-raw'
    parsed_topic_name = 'single-poliflow-data'

    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    
    for msg in consumer:
        df = JSONfile_toDataframe(msg.value)
        df = RefineDataframe(df)
        parsed_records = df
    consumer.close()
    sleep(1)

    o = 0

    length = len(parsed_records['index'])
    while o < length:
        partij = parsed_records['parties'].iloc[o]
        synop = parsed_records['description'].iloc[o]
        synop = cleanhtml(synop)

        json_data = {}
        json_data['partij'] = partij
        json_data['description'] = synop
        json_data = json.dumps(json_data)

        print('Publishing records...')
        producer = connect_kafka_producer()
        publish_message(producer, parsed_topic_name, json_data)

        o = o + 1
        print("--------------------------------")

    # for record in parsed_records:
    #     if record == 'description':
    #         length = len(parsed_records[record])
    #         while o < length:
    #             parsed_description.append(cleanhtml(parsed_records[record][o]))
    #             o = o + 1

    # test = parsed_description[0].split()
    # dictOfWords = { i : test[i] for i in range(0, len(test) ) }
    # testresult = computeTF(dictOfWords, 'test')
    # print(testresult)
    # table = TfIdf()
    # table.add_
    # print(parsed_records)

    # if len(parsed_records) > 0:
    #     print('Publishing records..')
    #     producer = connect_kafka_producer()
    #     # for rec in parsed_records:
    #     #     publish_message(producer, parsed_topic_name, rec)
    #     publish_message(producer, parsed_topic_name, parsed_records)