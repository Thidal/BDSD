import pandas as pd
import numpy as np
import requests
import json
import re
from collections import Counter

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