#!/usr/bin/env python
# coding: utf-8

# In[7]:


import argparse
import logging
import requests

log = logging.getLogger(__name__)


class NYTimesSource(object):
    """
    A data loader plugin for the NY Times API.
    """

    def __init__(self):
        pass

    def connect(self, inc_column=None, max_inc_value=None):
        log.debug("Incremental Column: %r", inc_column)
        log.debug("Incremental Last Value: %r", max_inc_value)

    def disconnect(self):
        """Disconnect from the source."""
        # Nothing to do
        pass
    
    def getKeywords(self, keywordList):
        keyValueList = []
        for keyword in keywordList:
            keyValueList.append(keyword["value"])
        
        return keyValueList

    def get_news_items(self, api_key, query, page):
        url = "https://api.nytimes.com/svc/search/v2/articlesearch.json"
        params = {
            "api-key": api_key,
            "q": query,
            "page": page
        }

        response = requests.get(url, params=params)
        data = response.json()
        
        news_items = []
        if "response" in data and "docs" in data["response"]:
            for doc in data["response"]["docs"]:
                news_item = {
                    "title": doc["headline"]["main"],
                    "body": doc["snippet"],
                    "web_url": doc["web_url"],
                    "created_at": doc["pub_date"],
                    "id": doc["_id"],
                    "abstract": doc["abstract"],
                    "keywords": self.getKeywords(doc["keywords"])                   
                }
                news_items.append(news_item)

        return news_items

    def getDataBatch(self, batch_size):
        """
        Generator - Get data from source on batches.

        :returns One list for each batch. Each of those is a list of
                 dictionaries with the defined rows.
        """
        api_key = self.args.api_key
        query = self.args.query

        page = 0
        while True:
            news_items = self.get_news_items(api_key, query, page)
            if not news_items:
                break
            yield news_items
            page += 1
            if page * 10 >= batch_size:
                break

    def getSchema(self):
        """
        Return the schema of the dataset
        :returns a List containing the names of the columns retrieved from the
        source
        """

        schema = [
            "title",
            "body",
            "created_at",
            "id",
            "summary",
            "abstract",
            "keywords",
        ]

        return schema


if __name__ == "__main__":
    config = {
        "api_key": "LbD28ygn59tisg1YBCQ6uEmY5YIQ1M5S",
        "query": "Silicon Valley",
    }
    source = NYTimesSource()

    # This looks like an argparse dependency - but the Namespace class is just
    # a simple way to create an object holding attributes.
    source.args = argparse.Namespace(**config)
    
    for idx, batch in enumerate(source.getDataBatch(10)):
        print(f"{idx} Batch of {len(batch)} items")
        print("The full batch is as follow: \n")
        print(batch)
        print("\n")
        print("Values in the batch are as follow: \n")
        for item in batch:
            print(item)

