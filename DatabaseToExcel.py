from datetime import datetime, timedelta
import datetime as dt
import pandas as pd
from pandas_datareader import data, wb
import pandas_datareader as web
import numpy as np
import matplotlib.pyplot as plt
import pymongo
import matplotlib
import pprint

plt.style.use('ggplot')
import time


class DatabaseToExcel(object):

    def __init__(self, file):

        #Read tickers from file
        with open(file, "r") as ins:
            tickers = set()
            for line in ins:
                if (line.strip() != ""):
                    tickers.add(line.strip().lower())
        self.tickers = list(tickers)

        # Connect to MongoDB
        try:
            conn = pymongo.MongoClient()
            print("Connected to MogoDB successfully!!!")
        except:
            print("Could not connect to MongoDB")

        # Every StockTwitsScraper has a specified "collection"
        self.twit_database = conn.StockTwitsDatabase

    def store(self):

        for ticker in self.tickers:

            self.scored_collection = self.getAdjustedCounts(ticker).T
            # Calculate sentiment index, store in calculated_collection
            self.scored_collection = self.scored_collection.T
            calculated_collection = {}
            for day in self.scored_collection:
                bullishcount = self.scored_collection[day]['bullish']
                bearishcount = self.scored_collection[day]['bearish']
                bullindex = np.log((1 + bullishcount) / (1 + bearishcount))
                calculated_collection.update({str(day): bullindex})

            calculatedSentiment = pd.DataFrame.from_dict(calculated_collection, orient='index')
            calculatedSentiment.columns = ["Sentiment"]
            calculatedSentiment.to_csv("Historical/"+ticker+"_sentiment.csv")


    # Returns sentiment counts from past 24 hours, including pretrade
    def getAdjustedCounts(self, ticker):

        temptwitcollection = self.twit_database[ticker]

        print("Using ticker: " + ticker)
        cursor = temptwitcollection.find({})

        # sorted_lsit = list of all twits in this collection
        sorted_list = list(cursor)
        twitdataframe = pd.DataFrame.from_dict(list(sorted_list))

        # Count bullish and bearish per day, store in dict 'seendays'
        seendays = {}
        for index, row in twitdataframe.iterrows():
            if (row['sentiment']):
                dateandtime = dt.datetime.strptime(row['created_at'], '%a, %d %b %Y %H:%M:%S -%f')
                date = dateandtime.date()
                time = dateandtime.time()

                fourpm = dt.datetime(2017, 6, 26, 20, 0).time()
                eightam = dt.datetime(2017, 6, 26, 12, 0).time()

                if (time < eightam):
                    # print("Found time before 8AM: {}, inserting into previous day: {}".format(dateandtime,date - dt.timedelta(days=1) ))
                    date = date - dt.timedelta(days=1)
                    if date in seendays:
                        # print("Seen this day: " + str(date))
                        if (row['sentiment'].get('class').lower() == 'bullish'):
                            # print("Found a bullish!")
                            seendays[date]['bullish'] += 1
                        elif (row['sentiment'].get('class').lower() == 'bearish'):
                            # print("Found a bearish!")
                            seendays[date]['bearish'] += 1
                        else:
                            print("NO SENTIMENT")


                    else:
                        # print("Have not seen this day" + str(date))
                        if (row['sentiment'].get('class').lower() == 'bullish'):
                            # print("Found a bullish!")
                            seendays.update({date: {'bullish': 1, 'bearish': 0}})
                        elif (row['sentiment'].get('class').lower() == 'bearish'):
                            # print("Found a bearish!")
                            seendays.update({date: {'bullish': 0, 'bearish': 1}})
                        else:
                            print("NO SENTIMENT")


                else:
                    # print("Found regular time: {}".format(date))

                    if date in seendays:
                        # print(row)
                        # print("Seen this day: " + str(date))
                        if (row['sentiment'].get('class').lower() == 'bullish'):
                            # print("Found a bullish!")
                            seendays[date]['bullish'] += 1
                        elif (row['sentiment'].get('class').lower() == 'bearish'):
                            # print("Found a bearish!")
                            seendays[date]['bearish'] += 1
                        else:
                            print("NO SENTIMENT")


                    else:
                        # print("Have not seen this day" + str(date))
                        if (row['sentiment'].get('class').lower() == 'bullish'):
                            # print("Found a bullish!")
                            seendays.update({date: {'bullish': 1, 'bearish': 0}})
                        elif (row['sentiment'].get('class').lower() == 'bearish'):
                            # print("Found a bearish!")
                            seendays.update({date: {'bullish': 0, 'bearish': 1}})
                        else:
                            print("NO SENTIMENT")

        # Calculate sentiment index for each day, store in DF 'calculatedSentiment'
        scored_collection = pd.DataFrame.from_dict(seendays)
        return scored_collection


if __name__ == '__main__':

    file = "Best Stocks.txt"
    db2e = DatabaseToExcel(file)
    db2e.store()

