from datetime import datetime, timedelta
import datetime as dt
import pandas as pd
from pandas_datareader import data, wb
import pandas_datareader as web
import numpy as np
import matplotlib.pyplot as plt
import pymongo
import TwitScraper2

plt.style.use('ggplot')

class SentimentPrice(object):

    # Initialize connection to stock's db collection
    #self.twit_collection
    #self.start
    #self.end
    #self.bullbearcounts
    def __init__(self, ticker):
        self.ticker = ticker


        # Connect to MongoDB
        try:
            conn = pymongo.MongoClient()
            print("Connected to MogoDB successfully!!!")
        except:
            print("Could not connect to MongoDB")

        # Every StockTwitsScraper has a specified "collection"
        self.twit_database = conn.StockTwitsDatabase

        #Check if a collection exists for this ticker
        all_collections = list(self.twit_database.collection_names())
        if ticker.lower() in all_collections:
            print("Ticker '{}' is in database!".format(ticker.upper()))
            self.twit_collection = self.twit_database[self.ticker]
        else:
            print("Ticker not in database")
            exit()

    # Analyze (graph) stock's sentiment and financial data
    def analyze(self, ticker):

        # Get stock's financial history from google
        price_history = self.getFinancialHistory()

        #Get stock's sentiment history from db
        sent_history = self.getSentimentHistory()

        df = pd.merge(price_history, sent_history, left_index=True, right_index=True, how="outer")
        print(df.head(20))

    # Get stock's financial data
    def getFinancialHistory(self):

        self.start = self.findGoodStartDate()
        self.end = self.twit_collection.find().sort('id', -1)[0]['created_at']
        print("Start: {}, End: {}".format(self.start,self.end))

        df = web.DataReader(ticker, 'google', self.start, self.end)

        # Drop unimportant columns
        df.drop('High', axis=1, inplace=True)
        df.drop('Low', axis=1, inplace=True)

        return df

    # Get a good "start date" to get financial data from
    def findGoodStartDate(self):

        sortedlist = list(self.twit_collection.find().sort('id', -1))
        listsize = len(sortedlist)

        for index in range(listsize - 1):
            date = dt.datetime.strptime(sortedlist[index]['created_at'], '%a, %d %b %Y %H:%M:%S -%f')
            nextdate = dt.datetime.strptime(sortedlist[index + 1]['created_at'], '%a, %d %b %Y %H:%M:%S -%f')
            delta = date - nextdate

            # If there is a 'x' day gap in twits, start there
            if (delta.days > 100):
                print("GAP FOUND: Starting at ({}/{}) - This date: {}, next date: {}".format(index, listsize, date,
                                                                                             nextdate))
                return dt.datetime.strptime(sortedlist[index]['created_at'], '%a, %d %b %Y %H:%M:%S -%f')

            # If we have reach the last twit in collection, start there
            elif (index == listsize - 2):
                print("REACHED LAST TWIT: Starting at ({}/{}) - This date: {}".format(index, listsize, date))
                return dt.datetime.strptime(sortedlist[index + 1]['created_at'], '%a, %d %b %Y %H:%M:%S -%f')

    # Extract sentiment history from db collection
    def getSentimentHistory(self):

        self.bullbearcounts = self.getBullBearCounts()

        sma_1 = self.getSentimentIndexMA(1)

        return sma_1

    # Get raw number of bull/bear twits
    def getBullBearCounts(self):


        # Get list of all twits in this stock's db collection
        twit_list = list(self.twit_collection.find({}))
        twitdf = pd.DataFrame.from_dict(twit_list)

        # Get # of bullish/bearish per day, store in dict 'seendays'
        # Past day is from last days closing bell (4PM) to this days opening bell (8AM)
        seendays = {}
        for index, row in twitdf.iterrows():
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

        return pd.DataFrame.from_dict(seendays).T

    # Calculate sentiment 'index' metric from raw bull/bear counts (w/ moving average window)
    def getSentimentIndexMA(self, moving_average):

        # Calculate sentiment metric from raw bull/bear counts
        sentdf = pd.DataFrame()
        sentdf['bullishcount'] = self.bullbearcounts['bullish'].rolling(moving_average).sum()
        sentdf['bearishcount'] = self.bullbearcounts['bearish'].rolling(moving_average).sum()
        sentdf['Sentiment'] = np.log((1 + sentdf['bullishcount']) / (1 + sentdf['bearishcount']))

        # Drop columns used for calculations
        #sentdf.drop(['bullishcount','bearishcount'], inplace=True,axis=1)
        sentdf.index = pd.to_datetime(sentdf.index)

        return sentdf








if __name__ == '__main__':

    ticker = "cmg"
    sentimentprice = SentimentPrice(ticker)
    sentimentprice.analyze(ticker)