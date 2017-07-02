import requests
from bs4 import BeautifulSoup
import pymongo
import time
import operator
import pprint
import datetime


class TickerFinder(object):

    def __init__(self):
        # Connect to MongoDB
        try:
            conn = pymongo.MongoClient()
            print("Connected to MogoDB successfully!!!")
        except:
            print("Could not connect to MongoDB")

        # Every StockTwitsScraper has a specified "collection"
        self.twit_database = conn.StockTwitsDatabase


    def findTopWatched(self, file):


        collection_names = self.twit_database.collection_names()
        for ticker in collection_names:
            address = "/symbol/"+str(ticker)

            #Parse html from request to get 'stream_id' of this stock/user stream and set 'found_streamID appropriately
            html_response = requests.get('https://stocktwits.com/' + address)
            bs_obj = BeautifulSoup(html_response.content, "html.parser")
            try:
                watchers = bs_obj.find("a", class_="watchers-top").contents[0].split('\n')[1]
                watchers = watchers.replace(',','')
                if(int(watchers)>20000):
                    print("Ticker: {}  Watchers: {}".format(ticker, watchers))
            except:
                pass


if __name__ == '__main__':

    finder = TickerFinder()
    finder.findTopWatched("Stocks_to_check.txt")


