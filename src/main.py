"""
Rajesh Sakhamuru
Huffington Post Archived Articles Retrieval
Web Article Scraper, ElasticSearch Index and Queries
12-1-2020
"""

import os
import urllib3
import json
import csv
import requests
import random
import re
import pandas as pd

from pprint import pprint
from time import sleep
from html.parser import HTMLParser

from elasticsearch import Elasticsearch
from elasticsearch import helpers

from flask import Flask, request, render_template

app = Flask(__name__)

es_client = Elasticsearch(http_compress=True)
# Order Results by Recency vs by Relevancy, True = Recency
TIME_TRAVEL = True


class MyHTMLParser(HTMLParser):
    """
    html parser just for main paragraph text in each article
    currently specific to huffingtonpost.com domain
    """
    # Initializing list
    textBody = list()
    pFlag = False

    # HTML Parser Methods
    def handle_starttag(self, startTag, attrs):
        if startTag == 'p':
            self.pFlag = True

    def handle_data(self, data):
        if self.pFlag:
            self.textBody.append(data)

    def handle_endtag(self, endTag):
        if endTag == 'p':
            self.pFlag = False

    def handle_startendtag(self, startendTag, attrs):
        pass

    def handle_comment(self, data):
        pass

    def error(self, message):
        pass


def extract(content):
    """
    Takes link as input and outputs article text as string
    :param content: article URL as string
    :return: string of text of article
    """
    parser = MyHTMLParser()
    parser.textBody = []
    parser.feed(content)
    textBody = parser.textBody
    textBody = " ".join(textBody)
    textBody = textBody.replace('\xa0', " ")
    return textBody.strip()


def scrapeAndSaveNewsArticles():
    """
    FOCUSED ARTICLE SCRAPER FOR huffingtonpost.com articles, links provided in json

    To scrape 100000 article links from the .json, it took nearly 24 hours.
    """
    http = urllib3.PoolManager()

    # change output document name if needed so already collected articles are not overwritten
    with open('documents/news_documents7.csv', 'w') as fout:
        writer = csv.writer(fout)
        writer.writerow(['category', 'headline', 'authors', 'link', 'short_description', 'date', 'body'])

        with open('documents/News_Category_Dataset_v2.json') as f:
            line = f.readline()
            cnt = 0
            while line:
                line = f.readline()
                # if cnt <= 99872:
                #     cnt += 1
                #     continue
                data = json.loads(line)
                link = data['link']
                try:
                    resp = http.request('GET', link)
                    # sleep(0.1)
                except urllib3.exceptions.MaxRetryError:
                    cnt += 1
                    print("Bad Link:", link)
                    continue

                try:
                    textBody = extract(str(resp.data.decode('utf-8')))
                except UnicodeDecodeError:
                    cnt += 1
                    print("Non-HTML link")
                    continue

                data["body"] = textBody
                writer.writerow(data.values())
                cnt += 1
                print(cnt)


def generateNewsDocsCSV():
    """
    Take generated news_documents CSV files made by scraper,
    concatenates them, cleans up 'body' and 'short_description' strings,
    outputs a new clean CSV file for use.
    """
    docs = None
    for d in range(7):
        # import data file
        try:
            if d == 0:
                docs = pd.read_csv("documents/news_documents" + str(d) + ".csv")
            else:
                docs = docs.append(pd.read_csv("documents/news_documents" + str(d) + ".csv"), ignore_index=True)
        except FileNotFoundError:
            print("File not found")

    docs.dropna(inplace=True)

    docs['bodylen'] = docs['body'].str.len()
    docs['shortlen'] = docs['short_description'].str.len()
    docs.drop(docs[docs['bodylen'] < docs['shortlen']].index, inplace=True)
    docs = docs.drop(columns=['bodylen', 'shortlen'])

    docs.reset_index(drop=True, inplace=True)
    docs['body'] = docs['body'].replace('\n', ' ', regex=True)
    docs['body'] = docs['body'].replace('\t', ' ', regex=True)
    docs['body'] = docs['body'].replace('\r', ' ', regex=True)
    docs['short_description'] = docs['short_description'].replace('\n', ' ', regex=True)
    docs['short_description'] = docs['short_description'].replace('\t', ' ', regex=True)
    docs['short_description'] = docs['short_description'].replace('\r', ' ', regex=True)

    docs.loc[27653]['body'] = docs.loc[27653]['body'].replace('PHOTO GALLERY', '')
    pgIdx = docs[docs['body'].str.contains("PHOTO GALLERY")].index
    c = 0
    for i in pgIdx:
        docs.loc[i]['body'] = docs.loc[i]['body'].split('PHOTO GALLERY', 1)[0]
        c += 1

    docs.to_csv('documents/full_news_documents.csv', index=True)


def filterKeys(document, use_these_keys):
    """
    turn pandas row into dictionary object and return it
    :param document: pandas row
    :param use_these_keys: column names of pandas df, string values
    :return:
    """
    return {key: document[key] for key in use_these_keys}


def doc_generator(df, indexName, use_these_keys):
    """
    Takes documents from pandas dataframe and passes them as dictionaries to the
    ElasticSearch indexer.
    :param df: all articles pandas dataframe
    :param indexName: string ES index name
    :param use_these_keys: list of strings, column names of dataframe to be indexed
    :return:
    """
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
            "_index": indexName,
            "_doc": "_doc",
            "_id": f"{document['id']}",
            "_source": filterKeys(document, use_these_keys),
        }


def indexDocsToES(indexName):
    """
    loads all documents to Elasticsearch indexer
    :return:
    """
    docs = pd.DataFrame()
    try:
        docs = pd.read_csv("documents/full_news_documents.csv")
    except FileNotFoundError:
        print("File not found")

    use_these_keys = ['id', 'category', 'headline', 'authors', 'link', 'short_description', 'date', 'body']
    helpers.bulk(es_client, doc_generator(docs, indexName, use_these_keys))


def queryESindexRecency(queryText: str, indexName='huffpost_news_index'):
    """
    Queries the ElasticSearch Index with provided query string for relevant articles
    This one specifically also provides priority to articles written more recently, assuming the current
    date is 2018/5/25
    :param queryText: string of query passed by user of flask webpage
    :param indexName: string name of ES index
    :return: dictionary of top 20 articles based on queryText
    """
    fuzz = False
    queryCross = {
        "from": 0,
        "size": 20,
        "query": {
            "function_score": {
                "score_mode": "sum",
                "boost_mode": "multiply",  # The documents relevance is multiplied with the sum
                "functions": [
                    {
                        # The relevancy of old posts is multiplied by at least one.
                        "weight": 1
                    },
                    {
                        # Published in last 150 days get a big boost
                        "weight": 1.5,
                        "gauss": {
                            "date": {
                                "origin": "2018-05-25",
                                "scale": "150d",
                                "decay": 0.5
                            }
                        }
                    },
                    {
                        # Published in last 1200 days get a boost
                        "weight": 1.25,
                        "linear": {
                            "date": {
                                "origin": "2018-05-25",
                                "scale": "1200d",
                                "decay": 0.5
                            }
                        }
                    }
                ],
                "query": {
                    "multi_match": {
                        "query": queryText,
                        "type": "cross_fields",
                        "fields": ['headline', 'short_description^2', 'body^3', 'authors', 'category^2'],
                        "auto_generate_synonyms_phrase_query": True,
                        "minimum_should_match": '25%'
                    }
                }
            }
        }
    }

    queryFuzzy = {
        "from": 0,
        "size": 20,
        "query": {
            "function_score": {
                "score_mode": "sum",
                "boost_mode": "multiply",  # The documents relevance is multiplied with the sum
                "functions": [
                    {
                        # The relevancy of old posts is multiplied by at least one.
                        "weight": 1
                    },
                    {
                        # Published in last 150 days get a big boost
                        "weight": 1.5,
                        "gauss": {
                            "date": {
                                "origin": "2018-05-25",
                                "scale": "150d",
                                "decay": 0.5
                            }
                        }
                    },
                    {
                        # Published in last 1200 days get a boost
                        "weight": 1.25,
                        "linear": {
                            "date": {
                                "origin": "2018-05-25",
                                "scale": "1200d",
                                "decay": 0.5
                            }
                        }
                    }
                ],
                "query": {
                    "multi_match": {
                        "query": queryText,
                        "type": "best_fields",
                        "fields": ['headline^2', 'short_description', 'body', 'authors', 'category'],
                        "auto_generate_synonyms_phrase_query": True,
                        "fuzziness": 'AUTO',
                        "max_expansions": '4'
                    }
                }
            }
        }
    }

    res = es_client.search(index=indexName, body=queryCross)

    # if results are poor, use fuzzy search instead in case of misspelled word
    if res['hits']['total']['value'] <= 3:
        res2 = es_client.search(index=indexName, body=queryFuzzy)
        if res2['hits']['total']['value'] > 3:
            fuzz = True
            res = res2

    return res, fuzz


def queryESindexRelevance(queryText: str, indexName='huffpost_news_index'):
    """
    Queries the ElasticSearch Index with provided query string for relevant articles
    :param queryText: string of query passed by user of flask webpage
    :param indexName: string name of ES index
    :return: dictionary of top 20 articles based on queryText
    """
    fuzz = False
    queryCross = {
        "from": 0,
        "size": 20,
        "query": {
            "multi_match": {
                "query": queryText,
                "type": "cross_fields",
                "fields": ['headline', 'short_description^2', 'body^3', 'authors', 'category^2'],
                "auto_generate_synonyms_phrase_query": True,
                "minimum_should_match": '25%'
            }
        }
    }

    queryFuzzy = {
        "from": 0,
        "size": 20,
        "query": {
            "multi_match": {
                "query": queryText,
                "type": "best_fields",
                "fields": ['headline^2', 'short_description', 'body', 'authors', 'category'],
                "auto_generate_synonyms_phrase_query": True,
                "fuzziness": 'AUTO',
                "max_expansions": '4'
            }
        }
    }

    res = es_client.search(index=indexName, body=queryCross)
    # if results are poor, use fuzzy search instead in case of misspelled word
    if res['hits']['total']['value'] <= 3:
        res2 = es_client.search(index=indexName, body=queryFuzzy)
        if res2['hits']['total']['value'] > 3:
            fuzz = True
            res = res2

    return res, fuzz


def startESServer():
    """
    Send console command to START ElasticSearch server
    :return:
    """
    startES = 'systemctl start elasticsearch.service'

    os.popen(startES, 'w')
    print("PLEASE WAIT 20 SECONDS... STARTING ELASTICSEARCH SERVER.\n")
    sleep(20)


def stopESServer():
    """
    Send console command to STOP ElasticSearch server
    :return:
    """
    statusES = 'systemctl status elasticsearch.service'
    stopES = 'systemctl stop elasticsearch.service'

    os.popen(stopES, 'w')
    print("\nPLEASE WAIT 20 SECONDS... STOPPING ELASTICSEARCH SERVER.\n")
    sleep(20)
    os.popen(statusES, 'w')


def main():
    """
    1. scrape and save news articles from links in News .json file (COMMENTED OUT BECAUSE CSV IS AVAILABLE)
    2. generate the combined csv and clean the article texts and data. Delete data with missing attributes
        (ALSO COMMENTED OUT BECAUSE CSV IS ALREADY AVAILABLE)
    3. load csv to ElasticSearch index if it is not already loaded
    :return: None
    """
    try:
        res = requests.get('http://localhost:9200')
        pprint(json.loads(res.content.decode('utf-8')))
    except requests.exceptions.ConnectionError:
        print("ERROR: ELASTICSEARCH Server is not running!")
        exit(-1)

    # scrapeAndSaveNewsArticles()
    # generateNewsDocsCSV() # may need to be modified based on how scrapeAndSave function file output
    if not es_client.indices.exists(index='huffpost_news_index'):
        print("PLEASE WAIT... LOADING DOCUMENTS INTO INVERTED INDEX")
        indexDocsToES('huffpost_news_index')


@app.route('/')
def my_form():
    """
    :return: renders query page with no search results
    """
    return render_template('queryPage.html')


def readStopList():
    """
    load Stop list file into list
    :return: list of stop words
    """
    f = None
    try:
        f = open('documents/stoplist.txt', 'r')
    except FileNotFoundError:
        print("ERROR: File not found.")
        exit(-1)
    if f is None:
        print("ERROR: Error loading stoplist")
        exit(-1)

    return str(f.read()).split()


@app.route('/', methods=['POST'])
def my_form_post():
    """
    Take query input from flask webpage, Query the ElasticSearch Index server,
    Generate summaries for the top 20 links given by the search query,
    and Print the top 20 relevant links and details to the flask webpage.

    :return passes ranked document list based on query to webpage
    """
    global TIME_TRAVEL
    try:
        requests.get('http://localhost:9200')
    except requests.exceptions.ConnectionError:
        print("ERROR: ELASTICSEARCH Server is not running!")
        exit(-1)

    if 'timeTravel' in request.form:
        TIME_TRAVEL = True
        return render_template('queryPage.html')
    if 'relevancy' in request.form:
        TIME_TRAVEL = False
        return render_template('queryPage.html')

    queryText = request.form['text']
    stopList = readStopList()
    if TIME_TRAVEL:
        rankedDocs, fuzz = queryESindexRecency(queryText)
    else:
        rankedDocs, fuzz = queryESindexRelevance(queryText)

    rankedDocs = rankedDocs['hits']['hits']

    queryText = re.sub(r'[^A-Za-z0-9 ]+', '', queryText)
    bodies = [doc['_source']['body'] for doc in rankedDocs]

    # generate summaries and top documents.
    if not fuzz:
        keySummaries = []
        for body in bodies:
            bod = []
            splitQuery = queryText.lower().split(' ')
            if len(splitQuery) >= 2:
                for q in splitQuery:
                    if q in stopList:
                        splitQuery.remove(q)
            if len(splitQuery) < 1:
                splitQuery = queryText.lower().split(' ')
            for word in splitQuery:
                realBodySplit = body.split('.')
                bod += [realBodySplit[i] + '...' for i, sentence in enumerate(body.lower().split('.')) if
                        word in sentence]
            bod = list(dict.fromkeys(bod))
            realBod = []
            if len(bod) > 4:
                ranIs = random.sample(range(len(bod)), 4)
                ranIs.sort()
                for i in ranIs:
                    realBod.append(bod[i])
            else:
                realBod = bod

            bod = ' '.join(realBod)
            if len(bod) > 900:
                bod = bod[:900] + "..."
            keySummaries.append(bod)

        rankedDocs = [(doc['_source']['link'],
                       doc['_source']['headline'],
                       keySummaries[i],
                       doc['_source']['authors'],
                       doc['_source']['date']) for i, doc in enumerate(rankedDocs)]

    else:
        summaries = []
        for i, doc in enumerate(rankedDocs):
            if len(doc['_source']['short_description']) < 100:
                if len(bodies[i]) > 600:
                    summaries.append(bodies[i][:600] + "...")
                else:
                    summaries.append(bodies[i] + "...")
            else:
                summaries.append(doc['_source']['short_description'])

        rankedDocs = [(doc['_source']['link'],
                       doc['_source']['headline'],
                       summaries[i],
                       doc['_source']['authors'],
                       doc['_source']['date']) for i, doc in enumerate(rankedDocs)]

    queryText = "Searching For: '" + queryText + "'"

    return render_template('queryPage.html', result=rankedDocs, queryText=queryText)


startESServer()

# load documents into ElasticSearch
main()
# start flask server for UI and taking queries
app.run()

stopESServer()
