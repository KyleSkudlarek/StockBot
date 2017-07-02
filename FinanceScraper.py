
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


class FinanceScraper(object):

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

        all_collections = self.twit_database.collection_names()
        list_all_collections = list(all_collections)

        if ticker.lower() in list_all_collections:
            print("Ticker '{}' is in database".format(ticker.upper()))
            self.twit_collection = self.twit_database[self.ticker]
        else:
            print("Ticker not in database")
            exit()

    def getMinTwitId(self):

        if (self.twit_collection.count() > 0):
            mintwitid = self.twit_collection.find().sort('id', 1)[0]['id']
            print("Min twit id is: {}".format(mintwitid))
            return mintwitid

    def getMaxTwitId(self):

        if (self.twit_collection.count() > 0):
            maxtwitid = self.twit_collection.find().sort('id', -1)[0]['id']
            print("Max twit id is: {}".format(maxtwitid))
            return maxtwitid

    def getMinTimestamp(self):

        if (self.twit_collection.count() > 0):
            mintimestamp = self.twit_collection.find().sort('id', 1)[0]['created_at']
            print("Min twit timestamp is: {}".format(mintimestamp))
            return mintimestamp

    def getMaxTimestamp(self):

        if (self.twit_collection.count() > 0):
            maxtimestamp = self.twit_collection.find().sort('id', -1)[0]['created_at']
            print("Max twit timestamp is: {}".format(maxtimestamp))
            return maxtimestamp

    def showDistribution(self):
        alltwits = self.twit_collection.find().sort('id', 1)
        #for twit in alltwits:
            #print(str(twit['created_at']))

        twitdataframe = pd.DataFrame(list(alltwits))
        twitdataframe.index = pd.to_datetime(twitdataframe.created_at, format='%a, %d %b %Y %H:%M:%S -%f')

        twitsperday = twitdataframe.groupby(pd.TimeGrouper("D")).size().to_csv("twitsperday/"+self.ticker+'twitsperday.csv')
        ttd = pd.read_csv("twitsperday/"+self.ticker+'twitsperday.csv', parse_dates=True, index_col=0)

        fig = plt.figure(figsize=(20, 10))
        ax = fig.add_subplot(111)
        ax.set_title(self.ticker)
        ax.plot(ttd)
        #ttd.plot(ax)
        plt.show()

    def showDistributionAll(self, file):
        with open(file, "r") as ins:
            tickers = set()
            for line in ins:
                if (line.strip() != ""):
                    tickers.add(line.strip().lower())
        tickers = list(tickers)

        for ticker in tickers:
            temp_twit_collection=self.twit_database[ticker.lower()]
            alltwits = temp_twit_collection.find().sort('id', 1)
            name = temp_twit_collection.name
            twitdataframe = pd.DataFrame(list(alltwits))
            twitdataframe.index = pd.to_datetime(twitdataframe.created_at, format='%a, %d %b %Y %H:%M:%S -%f')

            twitsperday = twitdataframe.groupby(pd.TimeGrouper("D")).size().to_csv(
                "twitsperday/" + temp_twit_collection.name + 'twitsperday.csv')
            ttd = pd.read_csv("twitsperday/" + temp_twit_collection.name + 'twitsperday.csv', parse_dates=True, index_col=0)
            fig = plt.figure(figsize=(8, 6))
            ax = fig.add_subplot(111)
            ax.set_title(name)

            ax.plot(ttd)
            ax.set_xlim(dt.date(2016,1, 1), dt.date(2017,6, 1) )
            plt.show()
        exit()

    def dayparser(self, date):
        parseddate = datetime.datetime.strptime(date, '%a, %d %b %Y %H:%M:%S -%f')
        print("Parsed date: "+str(parseddate)+" , to day: "+str(parseddate.date()))
        return parseddate.date()

    def getPriceHistory(self, ticker):

        #Set up stock scraper
        start = dt.datetime(2015, 6, 1)
        end = dt.datetime(2017, 6, 21)
        df = web.DataReader(ticker, 'google', start, end)
        #  df.to_csv(str(ticker)+"tickerfinancialdata.csv")
        # df = pd.DataFrame.from_csv("tickerfinancialdata.csv")
        #Add/delete columns
        df.drop('High', axis=1, inplace=True)
        df.drop('Low', axis=1, inplace=True)
        df.drop('Volume', axis=1, inplace=True)




        # Make a 'nextdaydelta' column equal to next days 'Price Change'
        # nextdaydelta = []
        # df['Price Change'] = (df['Close'] - df["Open"]) / df["Open"]
        # for index in range(0, len(df) - 1):
        #     delta = (df.iloc[index + 1]['Price Change'])
        #     nextdaydelta.append(delta)
        # nextdaydelta.append(nextdaydelta[-1])
        # df['Next Day Delta'] = nextdaydelta

        self.scored_collection = self.getAdjustedCounts(ticker).T
        # print("SCORED COLLECTION:" )
        # print(self.scored_collection.head())
        #
        # df['Bullish'] = self.scored_collection['bullish']
        # df['Bearish'] = self.scored_collection['bearish']
        # df.drop('Bullish', axis=1, inplace=True)
        # df.drop('Bearish', axis=1, inplace=True)
        #scored_collection.to_csv(ticker+"score.csv")

        #Make a column 'xDay' equal to growth over next x days
        days=7
        df[str(days) + 'Day'] = (df['Close'].shift(-days) - df['Open'].shift(-1)) / df['Open'].shift(-1)





        #Calculate sentiment index, store in calculated_collection
        self.scored_collection=self.scored_collection.T
        calculated_collection = {}
        for day in self.scored_collection:
            bullishcount=self.scored_collection[day]['bullish']
            bearishcount = self.scored_collection[day]['bearish']

            #print(calculated_collection)
            #print(str(day)+", Bullish: {}, and Bearish: {}".format(str(scored_collection[day]['bullish']),str(scored_collection[day]['bearish'])))



            bullindex = np.log((1 + bullishcount) / (1 + bearishcount))
            calculated_collection.update({str(day): bullindex})



        #print(calculated_collection)



        # Make a 'sentiment' column equal to caculated sentiment
        sentiments = []
        for index in range(0, len(df) - 1):
            thisday = df.iloc[index].name.date()
            score = calculated_collection.get(str(thisday))
            #print("Today: {}, Sentiment: {}".format(str(thisday), score))
            sentiments.append(score)

        sentiments.append(0)
        #df['Sentiment'] = sentiments
        df['Sentiment3MA'] = self.getSentimentMA(ticker, 3)
        df['Sentiment7MA'] = self.getSentimentMA(ticker, 7)

        #ax = df.plot[['Next Day Delta','Bullish','Bearish','Sentiment','Sentiment3MA','Sentiment7MA']].plot()
        # ax = df.plot[['Next Day Delta','Bullish','Bearish','Sentiment','Sentiment3MA','Sentiment7MA']].plot()
        # df.drop()

        df.drop('Open', axis=1, inplace=True)
        df.drop('Close', axis=1, inplace=True)
        #df.drop('Price Change', axis=1, inplace=True)
        #df.drop('Sentiment7MA', axis=1, inplace=True)
        ax = df.plot(secondary_y=['Sentiment3MA', 'Sentiment7MA'], mark_right=False)
        ax.set_ylabel('Price %')
        ax.right_ax.set_ylabel('Sentiment')
        plt.show()

    def getRawCounts(self, ticker):
        temptwitcollection = self.twit_database[ticker]


        print("Using ticker: " + ticker)
        cursor = temptwitcollection.find().sort('created_at', -1)
        #sorted_lsit = list of all twits in this collection
        sorted_list = list(cursor)
        sortedtwits = []

        for x in sorted_list:
            sortedtwits.append(x)

        twitdataframe = pd.DataFrame.from_dict(list(sortedtwits))


        #Count bullish and bearish per day, store in dict 'seendays'
        seendays = {}
        for index, row in twitdataframe.iterrows():
            date = dt.datetime.strptime(row['created_at'], '%a, %d %b %Y %H:%M:%S -%f').date()
            #print(date)
            if date in seendays:
                #print("Seen this day: " + str(date))
                if (row['sentiment'].get('class').lower() == 'bullish'):
                    #print("Found a bullish!")
                    seendays[date]['bullish'] += 1
                elif (row['sentiment'].get('class').lower() == 'bearish'):
                    #print("Found a bearish!")
                    seendays[date]['bearish'] += 1
                else:
                    print("NO SENTIMENT")


            else:
                #print("Have not seen this day" + str(date))
                if (row['sentiment'].get('class').lower() == 'bullish'):
                    #print("Found a bullish!")
                    seendays.update({date: {'bullish': 1, 'bearish': 0}})
                elif (row['sentiment'].get('class').lower() == 'bearish'):
                    #print("Found a bearish!")
                    seendays.update({date: {'bullish': 0, 'bearish': 1}})
                else:
                    print("NO SENTIMENT")


        #Calculate sentiment index for each day, store in DF 'calculatedSentiment'
        scored_collection = pd.DataFrame.from_dict(seendays)
        return scored_collection

    #Returns sentimentr counts from past 24 hours, including pretrade
    def getAdjustedCounts(self, ticker):
        temptwitcollection = self.twit_database[ticker]


        print("Using ticker: " + ticker)
        cursor = temptwitcollection.find({})
        #sorted_lsit = list of all twits in this collection
        sorted_list = list(cursor)
        twitdataframe = pd.DataFrame.from_dict(list(sorted_list))


        #Count bullish and bearish per day, store in dict 'seendays'
        seendays = {}
        for index, row in twitdataframe.iterrows():
            dateandtime = dt.datetime.strptime(row['created_at'], '%a, %d %b %Y %H:%M:%S -%f')
            date = dateandtime.date()
            time = dateandtime.time()


            fourpm = dt.datetime(2017, 6, 26, 20, 0).time()
            eightam = dt.datetime(2017, 6, 26, 12, 0).time()

            if(time < eightam):
                print("Found time before 8AM: {}, inserting into previous day: {}".format(dateandtime,date - dt.timedelta(days=1) ))
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
                print("Found regular time: {}".format(date))

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




        #Calculate sentiment index for each day, store in DF 'calculatedSentiment'
        scored_collection = pd.DataFrame.from_dict(seendays)
        return scored_collection

    def getSentimentDF(self, ticker):

        # #Calculate sentiment index for each day, store in DF 'calculatedSentiment'
        # scored_collection = self.getAdjustedCounts(ticker)


        calculated_collection = {}
        for day in self.scored_collection:
            #print(str(day) + "Bullish: {}, and Bearish: {}".format(str(scored_collection[day]['bullish']),
                                                                  # str(scored_collection[day]['bearish'])))
            bullindex = np.log((1 + self.scored_collection[day]['bullish']) / (1 + self.scored_collection[day]['bearish']))
            calculated_collection.update({str(day): bullindex})
        calculatedSentiment = pd.DataFrame.from_dict(calculated_collection, orient='index')
        calculatedSentiment.columns=["Sentiment"]


        #print(calculatedSentiment.head())
        return calculatedSentiment

    def getSentimentMA(self, ticker, days):
        rawcounts = self.scored_collection.T


        rawcounts['MABear'] = rawcounts['bearish'].rolling(days).sum()
        rawcounts['MABull'] = rawcounts['bullish'].rolling(days).sum()
        rawcounts['MASent'] = np.log((1+ rawcounts['MABull'])/(1+rawcounts['MABear']))
        #print(rawcounts.head())
        return rawcounts['MASent']

    def getGrowthOverX(self, ticker, days):
        #Set up stock scraper
        start = dt.datetime(2015, 6, 1)
        end = dt.datetime(2017, 6, 21)
        df = web.DataReader(ticker, 'google', start, end)
        #  df.to_csv(str(ticker)+"tickerfinancialdata.csv")
        # df = pd.DataFrame.from_csv("tickerfinancialdata.csv")
        #Add/delete columns
        df.drop('High', axis=1, inplace=True)
        df.drop('Low', axis=1, inplace=True)
        df.drop('Volume', axis=1, inplace=True)
        df[str(days)+'Day'] = (df['Close'].shift(-days) - df['Open'].shift(-1))/df['Open'].shift(-1)
        print(df.tail())



        # Make a 'xdelta' column equal to next days 'Price Change'




if __name__ == '__main__':

    ticker = "tsla"
    scraper = FinanceScraper(ticker)
    #scraper.getGrowthOverX(ticker, 2)
    #scraper.getAdjustedCounts(ticker)
    scraper.getPriceHistory(ticker)
    #scraper.getSentimentMA(ticker, 1)



    # scraper.getMinTwitId()
    # scraper.getMaxTwitId()
    # scraper.getMinTimestamp()
    # scraper.getMaxTimestamp()
    #scraper.showDistribution()
    #scraper.generateTwitScores("ibb")
    #scraper.showDistributionAll("Top_stocks.txt")
    # pd.DataFrame(np.random.randn(120, 10).cumsum(axis=0)).plot()