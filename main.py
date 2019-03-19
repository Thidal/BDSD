import pandas as pd
import numpy as np
import requests
import json
import re
from collections import Counter
from kafka import KafkaProducer

# import nltk
# from nltk.corpus import stopwords
# stop_words = set(stopwords.words('Dutch'))
# extra_stop_words = ['waar', 'onze', 'weer', 'daarom']
# stop_words.update(extra_stop_words)
# from nltk.stem.snowball import SnowballStemmer
# stemmer = SnowballStemmer('Dutch')

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

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

def JSONfile_toDataframe(json_file):
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
        
        # print("For " , i , " in " , len(json_file))
        # print(json_file[i])
        for j, searchkey in enumerate(all_searches):
            try:
                # print("Adding " ,json_file[i]['_source'][searchkey], " to the list")
                all_lists[j].append(json_file[i]['_source'][searchkey])
            except KeyError:
                all_lists[j].append(None)

    # Save as DataFrame and return
    DF_search = pd.DataFrame(
        {'date':dates,
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

# with open('complete-dump.json', encoding='UTF-8') as data:
    # print("Converting to dataframe...")
    # DF_all = JSONfile_toDataframe(json.load(data))
    # print("Converting done!")
    # print("Organising dataframe...")
    # DF_all = RefineDataframe(DF_all)
    # print("Organising done!")
    # print(DF_all.head(10))

with open('smaller_dump.json', encoding='UTF-8') as data:
    producer.send('kafka-python-input', 'smaller_dump.json')