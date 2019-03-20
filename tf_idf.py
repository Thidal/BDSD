import pandas as pd
import numpy as np
import json
import re
from collections import Counter

import nltk

def JSONfile_toDataframe(json_file):
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
    """
    This function removes the entries from the dataframe that do not contain a description.
    """
    empty_list = []

    # loop over dataframe to record the empty entries
    for i in range(len(dataframe)):
        if dataframe.description[i] == None:
            empty_list.append(i)

    # remove the empty entries by index and reset the index
    dataframe.drop(empty_list, inplace=True)
    dataframe = dataframe.reset_index()

    return dataframe

def TextMining_CleanTextRegex(text, regex_statement, replacement):
    """
    This function removes the html-language from the article. Using a regex statement the function
    clears all the text that is between '<' and '>'.
    """
    pattern = re.compile(regex_statement)
    return re.sub(pattern, replacement, text)


def TextMining_TokenizeText(text):
    """
    This function tokenizes the text.
    """
    return nltk.word_tokenize(text)


def TextMining_CleanNonNouns(text, word_tagger):
    """
    This function removes all non-noun words. Using a tagger loaded with a model that was trained using 8
    million records of dutch sentences / words. The tagger labels all the words in the article and then the
    function will only keep the words that are labeled as nouns.
    """
    tagged = word_tagger.tag(text)
    return [t[0] for t in tagged if 'noun' in t[1]]


def TextMining_CleanStopWords(tokenized_text, stop_words):
    """
    This function removes all the stop words from an article. Stop words are frequently occuring words that
    have a relativly low meaning / weight.
    """
    return [word.lower() for word in tokenized_text if word.lower() not in stop_words]


def TextMining_CleanSmallWords(tokenizetext, min_wordsize=2):
    """
    This function removes all words that are smaller than two characters.
    """
    return [word for word in tokenizetext if len(word) >= min_wordsize]


def Create_Filter(options_list):
    """
    This functions creates a word list with all party names.
    """
    return [opt.lower() for opt in options_list]


def TextMining_CleanCitiesParties(tokenizetext, party_list, city_list):
    """
    This function removes all words that are party names.
    This function removes all words that are city names and occur as a city in the dataframe.
    """
    party_filter = Create_Filter(party_list)
    city_filter = Create_Filter(city_list)
    return [word for word in tokenizetext if word not in set(party_filter) and word not in set(city_filter)]


def TextMining_StemWords(tokenized_text):
    """
    This functions stems all the words in the article.
    """
    return [stemmer.stem(word) for word in tokenized_text]


def TextMining_ALL(texts_list, stop_words, party_list, city_list, word_tagger):
    """
    This function combines all the text cleaning steps and cleans the articles.
    """
    # remove html code
    step1 = [TextMining_CleanTextRegex(text, '<.*?>', '') for text in texts_list]

    # remove newlines ('\n') with a space
    step2 = [TextMining_CleanTextRegex(text, '\n', ' ') for text in step1]

    # remove urls
    step3 = [TextMining_CleanTextRegex(text, r'http\S+', '') for text in step2]

    # remove everything that doesn't contain letters
    step4 = [TextMining_CleanTextRegex(text, r"[^a-zA-Z]+", ' ') for text in step3]

    # tokenize text with nltk function and convert all words to lowercase
    step5 = [TextMining_TokenizeText(text) for text in step4]

    # remove all words that are not nouns
    step6 = [TextMining_CleanNonNouns(text, word_tagger) for text in step5]

    # remove the words that are contained within the list called stopwords
    step7 = [TextMining_CleanStopWords(text, stop_words) for text in step6]

    # removes all words that have a length of 3 or less
    step8 = [TextMining_CleanCitiesParties(text, party_list, city_list) for text in step7]

    # removes all small words with length 3 or lower
    step9 = [TextMining_CleanSmallWords(text, 4) for text in step8]

    return step9


def TFIDF_UniqueWords(processed_texts):
    """
    This function finds all the unique words from the provided articles.
    """
    temp = set([word for text in processed_texts for word in text])
    return list(temp)


def TFIDF_WordDictionary(unique_word_list):
    """
    This function creates a dictionary with all the unique words and gives them an index.
    """
    return {key: index for index, key in enumerate(unique_word_list)}


def TFIDF_EmptyMatrix(texts_list, unique_word_list):
    """
    This function creates an empty matrix filled with zeros. The axes are the number of articles in the texts
    list and the number of unique words.
    """
    return np.zeros((len(texts_list), len(unique_word_list)))


def TFIDF_CountMatrix(unique_word_list, texts_list, word_dictionary):
    """
    This function filles in an empty matrix with all the occurences of every word in every article.
    """
    temp_matrix = TFIDF_EmptyMatrix(texts_list, unique_word_list)
    for i in range(len(texts_list)):
        for word in texts_list[i]:
            temp_matrix[i, word_dictionary[word]] += 1

    return temp_matrix


def TFIDF_CleanMatrix(count_matrix):
    """
    This function can remove all the articles from the matrix that do not contain any words due to the text
    cleaning operations.
    """
    return count_matrix[np.where(count_matrix.sum(axis=1) != 0)]


def TFIDF_TermFrequency(count_matrix):
    """
    This function calculates the term frequency of each word per article. The outcome is a filled in matrix with
    term frequency values for every word per article.
    """
    sums = count_matrix.sum(axis=1)
    new_matrix = 0.5 + 0.5 * (count_matrix / sums[np.newaxis].T)
    return new_matrix


def TDIDF_InversedDocumentFrequency(count_matrix):
    """
    This function calculates the inversed document frequency of every word. The outcome is an array with all the
    inversed document values per word.
    """
    occurences = np.count_nonzero(count_matrix, axis=0)
    new_array = np.log((len(count_matrix) / occurences))
    return new_array


def TFIDF_CalculateTFIDF(tf_matrix, idf_matrix):
    """
    This function combines the word frequency matrix with the inversed document frequency array in order to
    calculate all the term frequency-inversed document frequency values per word per article.
    """
    return tf_matrix * idf_matrix


def TFIDF_ALL(texts_list):
    """
    This function combines all the tf-idf functions to create a process with which the tf-idf values can be
    calculated for the provided articles.
    """
    # find all the unique words in the provided texts
    step1 = TFIDF_UniqueWords(texts_list)

    # convert the all the unique words to a dictionary with a given index
    step2 = TFIDF_WordDictionary(step1)

    # create a matrix with all the word occurences per word per text
    step3 = TFIDF_CountMatrix(step1, texts_list, step2)

    # in the matrix calculate the term frequency for every word per text
    step4 = TFIDF_TermFrequency(step3)

    # calculate the inversed document frequency for each word
    step5 = TDIDF_InversedDocumentFrequency(step3)

    # combine the word frequency and the inversed document frequency
    step6 = TFIDF_CalculateTFIDF(step4, step5)

    return [step2, step6]


def Dataframe_FindUniques(dataframe, choice):
    """
    Create a list with all the unique occurences depending on the given choice. At the moment you can choose between
    'party' and 'city' to create a new list with uniques.
    """
    mega_list = []

    if choice == 'party':
        for parties in dataframe.parties:
            for party in parties:
                mega_list.append(party)

    elif choice == 'city':
        for city in dataframe.location:
            if city is None:
                continue
            mega_list.append(city)

    mega_list = set(mega_list)

    return list(mega_list)


def Find_TFIDF_TopWords_ALL(analysis_result, number=5):
    """
    Find the n number of words that score the highest tf-idf rating per article. These are then presented in a list
    with the top words per article.
    """
    temp_word_dict = {}
    for value, key in enumerate(analysis_result[0]):
        temp_word_dict[value] = key

    top_words_inds = []
    for arr in analysis_result[1]:
        top_words_inds.append(list(np.argpartition(arr, -number)[-number:]))

    top_words_per_article = []
    for indexes in top_words_inds:
        temp = []
        for index in indexes:
            temp.append(temp_word_dict[index])
        top_words_per_article.append(temp)

    return top_words_per_article


def Find_Count_TopWords_ALL(analysis_result, number=5):
    """
    Find the n number of words that occur the most per article. These are then presented in a list with the top
    words per article.
    """
    top_words_per_article = []
    for art in analysis_result:
        temp_list = []
        most_used_words = Counter(art).most_common(number)
        for tup in most_used_words:
            temp_list.append(tup[0])

        top_words_per_article.append(temp_list)

    return top_words_per_article


def KeywordTagger_ALL(dataframe, stop_words, word_tagger, choice):
    """
    This function combines all the neccessary functions in order to create a list with the top scoring words based
    on your choice (tf-idf or count). The result is a list with the top words per article.
    """
    start_index = 0
    end_index = 1000
    result_list = []
    party_list = Dataframe_FindUniques(dataframe, 'party')
    city_list = Dataframe_FindUniques(dataframe, 'city')

    while start_index < len(dataframe):

        # select a subset of the dataframe with the start and end index, these change (with +1000) after every loop
        raw_articles = dataframe.description[start_index:end_index]
        # clean and prepare all the texts in the selected subset
        cleaned_articles = TextMining_ALL(raw_articles, stop_words, party_list, city_list, word_tagger)

        if choice == 'tfidf':

            # calculate the tf-idf of each word per text
            analysed_articles = TFIDF_ALL(cleaned_articles)

            # find the top words based on the tf-idf
            top_words = Find_TFIDF_TopWords_ALL(analysed_articles)

        elif choice == 'count':

            # find the top words based on the count
            top_words = Find_Count_TopWords_ALL(cleaned_articles)

        for l in top_words:
            result_list.append(l)

        start_index += 1000
        end_index += 1000

        print('\r{}/{}'.format(start_index, len(dataframe)), end='')

    #        if start_index == 10000:
    #            break

    print("\nTHE END!")

    return result_list

def AddColumn_toDataframe(dataframe, column_list, column_name):
    """
    This function creates a new column in the dataframe using the given list and the given column name.
    """
    dataframe[column_name] = column_list

if __name__ == '__main__':
    from nltk.corpus import stopwords
    stop_words = set(stopwords.words('Dutch'))
    extra_stop_words = ['waar', 'onze', 'weer', 'daarom']
    stop_words.update(extra_stop_words)
    from nltk.stem.snowball import SnowballStemmer
    stemmer = SnowballStemmer("dutch")

    # the section below imports a trained Dutch word-tagger we found on Github
    from nltk.tag.perceptron import PerceptronTagger
    tagger = PerceptronTagger(load=False)
    tagger.load('model.perc.dutch_tagger_large.pickle')

    # path to the data
    # file_path = r'complete-dump.json'
    file_path = r'smaller_dump2.json'

    # load the data by opening the json file
    with open(file_path, encoding='utf-8') as data:
        # convert json file to a pandas dataframe
        DF_all = JSONfile_toDataframe(json.load(data))

        # refine the dataframe by removing entries that containt no articles
        DF_all = RefineDataframe(DF_all)
        print(DF_all.head())

    # find the top words with the tf-idf method
    top_words_per_article_list_tfidf = KeywordTagger_ALL(DF_all, stop_words, tagger, 'tfidf')
    DF_all['tfidf_words'] = top_words_per_article_list_tfidf
    print(DF_all)
    # find the top words with the count method
    top_words_per_article_list_count = KeywordTagger_ALL(DF_all, stop_words, tagger, 'count')