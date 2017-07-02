import requests
from bs4 import BeautifulSoup
import pymongo
import operator
import time
import pprint
import datetime


class Analyzer(object):


    def __init__(self):
        try:
            conn = pymongo.MongoClient()
            print("Connected to MogoDB successfully!!!")
        except:
            print("Could not connect to MongoDB")

            # Every StockTwitsScraper has a specified "collection"
        self.twit_database = conn.StockTwitsDatabase
        self.user_database = conn.UserTwitsDatabase

    def findOddTwit(self, name):
        twit_collection=self.twit_database[name]
        cursor = twit_collection.find().sort('id', -1)

        print("Using collection name: "+twit_collection.name+" with size: "+str(twit_collection.find().count()))
        print("First twit: " + str(twit_collection.find()[0]['id']) )

        for item in cursor:
            item_date = datetime.datetime.strptime(item['created_at'], '%a, %d %b %Y %H:%M:%S -%f')
            next_item_date = datetime.datetime.strptime(cursor.next()['created_at'], '%a, %d %b %Y %H:%M:%S -%f')
            print(str("This date: "+str(item_date)+" , next date: "+str(next_item_date)))
            difference = item_date - next_item_date
            print("Difference = "+ str(difference.days))

            if(difference.days == 0):
                print("Less than a days difference!")
            elif (difference.days > 1):
                print("More than a days difference! Returning max= "+str(item['id']))
                return item['id']

            # for twit in twit_collection.find().sort("id", 0):
            #  print(str(twit))



    def findTopTickers(self):
        top_tickers = {}
        collection_names = self.twit_database.collection_names()
        #print(str(collection_names))
        for collection in collection_names:
            collection_count = self.twit_database[collection].find({}).count()
            top_tickers[collection] = collection_count


        sorted_top_tickers = sorted(top_tickers.items(), key=operator.itemgetter(1))
        for sorted_ticker in sorted_top_tickers:
            if(top_tickers[sorted_ticker[0]]>100):
                print(str(sorted_ticker[0])+" "+str(top_tickers[str(sorted_ticker[0])]))


    def findTopUsers(self):

        ins = open("findTopUsersOutput.txt", "+w")

        top_users = {}
        actual_top_users = {}
        collection_names = self.user_database.collection_names()
        print("Number of user collections: "+str(len(collection_names)))
        for collection in collection_names:
            collection_count = self.user_database[collection].find({}).count()
            top_users[collection] = collection_count

        for top_user in top_users:
            #print (str(top_user))
            if(top_users[top_user] > 50):
                actual_top_users[top_user]= top_users[top_user]
                #print("User: "+str(top_user)+", Twits: "+str(top_users[top_user]))
                ins.write(str(top_user.lower())+"\n")

        print("Number of SIZED user collections: " + str(len(actual_top_users)))
        sorted_top_users = sorted(top_users.items(), key=operator.itemgetter(1))
        for user in sorted_top_users:
            print(user)



if __name__ == '__main__':

    analyzer = Analyzer()
    #analyzer.findTopUsers()
    #
    # with open("Users.txt", "r") as ins:
    #     users = set()
    #     for line in ins:
    #         if (line.strip() != ""):
    #             users.add(line.strip().lower())
    # users = list(users)
    # print("Length of users set: "+str(len(users)))
    # user_collection_size = open("User collection sizes.txt", '+w')
    #
    # for user in users:
    #     user_collection = analyzer.user_database[user.lower()]
    #     user_collection_count = user_collection.find({}).count()
    #     if(user_collection_count>0):
    #         user_collection_size.write("User: "+str(user.lower())+", Count: "+str(user_collection_count)+"\n")



    analyzer.findTopTickers()
    #analyzer.findLastTimeInTicker("aapl")
    #analyzer.findTopUsers()
    #analyzer.findOddTwit("aapl")



