import requests
from bs4 import BeautifulSoup
import pymongo
import time
import pprint
import datetime


class StockTwitsScraper(object):

    # Finds 'stream_id' parameter to be used in Javascript query based off ticker
    def __init__(self, name, type):

        #Every StockTwitsScraper has a 'name' and 'type'
        self.type = type.lower()
        self.name = name.lower()

        # Connect to MongoDB
        try:
            conn = pymongo.MongoClient()
            print("Connected to MogoDB successfully!!!")
        except:
            print("Could not connect to MongoDB")

        #Every StockTwitsScraper has a specified "collection"
        self.twit_database = conn.StockTwitsDatabase
        self.user_database = conn.UserTwitsDatabase

        #Determine URL 'address' of this stock/user and connect to ther associated db collection
        #If stock, then the user database will be variable for each twit
        #If user, then the stock database will be variable for each twit
        if(self.type=="stock"):
            self.twit_collection = self.twit_database[self.name]
            self.twit_collection.create_index([('id', pymongo.ASCENDING)], unique=True)
            self.address = "/symbol/"+str(self.name)

        elif(self.type=="user"):
            self.user_collection = self.user_database[self.name]
            self.user_collection.create_index([('id', pymongo.ASCENDING)], unique=True)
            self.address = str(name)
            #print("User confirmed")

        #Parse html from request to get 'stream_id' of this stock/user stream and set 'found_streamID appropriately
        html_response = requests.get('https://stocktwits.com/' + self.address)
        bs_obj = BeautifulSoup(html_response.content, "html.parser")
        data_id_attr = bs_obj.find_all(attrs={"data-id": True})
        #print("data_id: " + str(data_id_attr) + ", ticker: " + self.name)
        #print(data_id_attr)
        if not data_id_attr:
            # Every StockTwitsScraper has a 'found_streamID' boolean and a 'steam_id'
            self.found_streamID = False
            self.steam_id = None
        else:
            self.found_streamID = True
            self.stream_id = data_id_attr[0].get("data-id")

    def getGoodTwitId(self):

            database_name = ""
            if (self.type == "stock"):
                database_name = self.twit_collection.name
            else:
                database_name = self.user_collection.name

            print("getGoodTwitId() called for: " + database_name)
            try:
                conn = pymongo.MongoClient()
                print("Connected to MogoDB successfully!!!")
            except:
                print("Could not connect to MongoDB")

            good_twit_found = False
            # Get odd twit to be used as 'max' of next query
            if (self.type == "stock"):
                if (self.twit_collection.count() > 0):
                    cursor = self.twit_collection.find().sort('id', -1)
                    sorted_list = list(cursor)
                    actual_sorted_list = []

                    for x in sorted_list:
                        actual_sorted_list.append(x['id'])

                    print("Sorted list:" + str(actual_sorted_list))

                    print("Using collection name: " + self.twit_collection.name + " with size: " + str(
                        self.twit_collection.find().count()))
                    firsttwit = self.twit_collection.find().sort('id', -1)[0]
                    print("First twit: " + str(firsttwit['id']) + " -- " + str(
                        firsttwit['created_at']))
                    firsttwitdate = datetime.datetime.strptime(firsttwit['created_at'], '%a, %d %b %Y %H:%M:%S -%f')

                    if(datetime.datetime.now().date()- datetime.timedelta(days=1) > firsttwitdate.date()):
                        return None
                    else:
                        for index in range(len(actual_sorted_list) - 1):
                            item = self.twit_collection.find({'id': actual_sorted_list[index]})[0]
                            next_item = self.twit_collection.find({'id': actual_sorted_list[index + 1]})[0]

                            #print("Item: " + str(item['id']) + " has next = " + str(next_item['id']))

                            item_date = datetime.datetime.strptime(item['created_at'], '%a, %d %b %Y %H:%M:%S -%f')
                            next_item_date = datetime.datetime.strptime(next_item['created_at'],
                                                                        '%a, %d %b %Y %H:%M:%S -%f')
                            # print(str("This date: " + str(item_date) + " , next date: " + str(next_item_date)))
                            difference = item_date - next_item_date
                            # print("Difference = " + str(difference.days))

                            if (difference.days == 0):
                                # print("Less than a days difference!")
                                item = next_item
                            elif (difference.days > 4):
                                good_twit_found = True
                                print("More than a days difference! item[id] = " + str(item['id']))
                                print(str("This date: " + str(item_date) + " , next date: " + str(next_item_date)))
                                print(str("This id: " + str(item['id']) + " , next id: " + str(next_item['id'])))
                                return (item['id'])



            elif (self.type == "user"):
                if (self.user_collection.count() > 0):
                    cursor = self.user_collection.find().sort('id', -1)

                    print("Using collection name: " + self.user_collection.name + " with size: " + str(
                        self.user_collection.find().count()))
                    print("First twit: " + str(self.user_collection.find()[0]['id']))

                    item = cursor[0]
                    item = cursor.next()
                    while (1):
                        try:
                            next_item = cursor.next()
                            #print("Item: " + str(item['id']) + " has next = " + str(next_item['id']))
                            item_date = datetime.datetime.strptime(item['created_at'], '%a, %d %b %Y %H:%M:%S -%f')
                            next_item_date = datetime.datetime.strptime(next_item['created_at'],
                                                                        '%a, %d %b %Y %H:%M:%S -%f')
                            print(str("This date: " + str(item_date) + " , next date: " + str(next_item_date)))
                            difference = item_date - next_item_date
                            print("Difference = " + str(difference.days))

                            if (difference.hours == 0):
                                print("Less than an hour difference!")
                                print("")
                                item = next_item
                                print("Next item: " + item['created_at'])
                            elif (difference.days > 24):
                                good_twit_found = True
                                print("More than a days difference! Returning max= " + str(item['id']))
                                return item['id']
                        except:
                            print("No good twit found")

            if not (good_twit_found):
                print("No good twit found, looking for min twit")
                return self.getMinTwitId()

    def getMinTwitId(self):
            print("getMinTwitID() called for: " + str(self.name))
            try:
                conn = pymongo.MongoClient()
                print("Connected to MogoDB successfully!!!")
            except:
                print("Could not connect to MongoDB")

            # Get min twit to be used as 'max' of next query
            if (self.type == "stock"):
                if (self.twit_collection.count() > 0):
                    return self.twit_collection.find().sort('id', 1)[0]['id']
                else:
                    print("getMinId = None")
                    return None
            elif (self.type == "user"):
                if (self.user_collection.count() > 0):
                    return self.user_collection.find().sort('id', 1)[0]['id']
                else:
                    print("getMinId = None")
                    return None

    # @'max'
    # Refers to the last twit id immediately preceding the next set of data to be parsed.
    # For example requesting 'max'=5 will get messages ids for 6-10
    def scrape(self, calls):

        #Check if this is a valid stock/user, and then find the min twit id in their collection
        # or return None for new stock/user to start querying at the current moment in time
        if(self.found_streamID):

            #Get a goodtwit id to use as starting point
            max =self.getGoodTwitId()

            #Scrape 'calls' times
            for i in range(calls):
                print("Name is: "+self.name)
                print("'Max' twit id: " + str(max))
                # # print("Stream ID found: " + scraper.stream_id)
                # print("Min twit is: " + str(max))
                print("Call: "+str(i)+" of "+str(calls))
                print("Actual time now: "+str(datetime.datetime.now()))
                #Form request headers and make the call to get json response
                payload = {}

                if(self.type=="stock"):
                    payload["stream"] = "symbol"
                    #print(payload["symbol"])
                elif(self.type=="user"):
                    payload["stream"] = "user"
                    #print(payload["stream"])

                payload["max"] = max
                print("Payload max: "+str(payload["max"]))
                payload["stream_id"] = self.stream_id
                #print("Payload stream_id: "+payload["stream_id"])
                #print("Address: "+self.address)

                r = requests.get(
                    url='https://stocktwits.com/streams/poll?' + self.address,
                    data=payload,
                    headers={
                        'X-Requested-With': 'XMLHttpRequest',
                        'HOST': 'stocktwits.com',
                        'Connection': 'keep-alive',
                        'Pragma': 'no-cache',
                        'Cache-control': 'no-cache',
                        'Accept': 'application/json',
                    }
                )
                # print(str(payload))
                # print("R: "+str(r))
                json_response = r.json()
                json_messages = json_response['messages']
                print("First twit returned in this batch: " + str(json_messages[0]['id'])+" --- "+str(json_messages[0]['created_at']))
                #print(str(json_messages))
                # parsed = json.loads(json_messages)
                # pp = pprint.PrettyPrinter(indent=4)
                # pp.pprint(json_messages)
                #print(json.dumps(json_messages, indent=4, sort_keys=True))
                #print("Response is: " + str(json_messages))
                # Set old 'max' to the last twit id from this batch to serve as query parameter for next batch
                if(json_messages!=[]):

                    max = json_messages[-1]['id']
                    print("The new 'max' for the next query is: "+str(max))
                    #print(json_messages)


                    # Iterate through JSON response to look at each twit
                    for item in json_messages:
                        # if (item['sentiment']!= None):
                        print(str(item['id'])+ " -- "+ item['body'] + " -- "+ item['created_at'])
                        unique=True

                        #If this is a stock, and determine if unique/new
                        if(self.type=="stock"):
                            if(self.twit_collection.count() > 0):
                                # Check if this twit id is already in the collection
                                if (self.twit_collection.find({'id': item['id']}).count() > 0):
                                    print("This twit has been seen")
                                    unique=False
                                else:
                                    unique=True
                                    print("This twit is unique")
                            else:
                                print("This twit is unique!")

                        # If this is a user, determine if unique/new
                        elif(self.type=="user"):
                            # Check if this user's twit was about a $ticker
                            #  store all found tickers into 'tickers'
                            body = item['body'].split()
                            tickers = []

                            for word in body:
                                if word.startswith("$"):
                                    ticker = word.split("$")[1]
                                    if(ticker.lower().isalpha()):
                                        tickers.append(ticker.lower())
                                        print("Found a ticker in this user's twit: " + ticker)
                            #If this twit is about a ticker, check if unique
                            if(len(tickers)>0):
                                if (self.user_collection.count() > 0):
                                    # Check if this twit id is already in the collection
                                    if (self.user_collection.find({'id': (item['id'])}).count() > 0):
                                        print("This twit has been seen")
                                        unique = False
                                    else:
                                        unique = True
                                        print("This twit is unique")
                                else:
                                    print("This twit is unique!")

                        # If uniqe/new tweet, check if bearish/bullish
                        if(unique):
                            if (item.get('sentiment') == None):
                                print("This sentiment was classified as Null")
                            else:
                                print("This sentiment was classified as Bearish/Bullish.")
                                print("...if new then inserting into databases...")
                                #print("Inserted this twit into database: "+word.split("$")[1]+" because of sentiment.")

                                #Insert into twit_database and user_database
                                if(self.type=="stock"):

                                    self.user_collection = self.user_database[item['user']['username'].lower()]
                                    self.user_collection.create_index([('id', pymongo.ASCENDING)], unique=True)

                                    try:
                                        self.twit_collection.insert(item)
                                        print("Inserted into twit_collection: " + str(self.twit_collection.name)+", Sentiment: " + item['sentiment'].get('class'))
                                    except:
                                        print("Already in twit collection: " + str(self.twit_collection.name))
                                    try:
                                        self.user_collection.insert(item)
                                        print("Inserted this twit into USER database: " + str(self.user_collection.name) + " because of sentiment.")
                                    except:
                                        print("Already in user collection: "+str(self.user_collection.name))

                                elif(self.type=="user"):
                                    #print(str(tickers))
                                    for ticker in tickers:
                                        #Check if the parsed 'ticker' is actually a ticker
                                        address = "/symbol/"+str(ticker)
                                        # Get 'stream_id' of this stock/user stream and set 'found_streamID appropriately
                                        html_response = requests.get('https://stocktwits.com/' + address)
                                        bs_obj = BeautifulSoup(html_response.content, "html.parser")
                                        data_id_attr = bs_obj.find_all(attrs={"data-id": True})
                                        # print("data_id: " + str(data_id_attr) + ", ticker: " + self.name)
                                        # print(data_id_attr)
                                        if not data_id_attr:
                                          print(ticker + " is not a valid ticker!")
                                        else:
                                            self.twit_collection = self.twit_database[ticker.lower()]
                                            self.twit_collection.create_index([('id', pymongo.ASCENDING)], unique=True)

                                            try:
                                                self.user_collection.insert(item)
                                                print("Inserted this twit into USER database: " + str(self.user_collection.name) + " because of sentiment.")
                                            except:
                                                print("Already in user collection: " + str(self.user_collection.name))
                                            try:
                                                self.twit_collection.insert(item)
                                                print("Inserted into twit_collection: " + str(self.twit_collection.name) + ", Sentiment: " + item['sentiment'].get('class'))
                                            except:
                                                print("Already in twit collection: "+str(self.twit_collection.name))
                        elif not (unique):
                            print("Not a unique twit!")
                            max = json_messages[-1]['id']
                            break

                    print("Finished iterating through all twits in that query.")
                else:
                    i=calls

        else:
            print("No stream id found - invalid name")






if __name__ == '__main__':

    # with open("Users.txt", "r") as ins:
    #     users = set()
    #     for line in ins:
    #         if (line.strip() != ""):
    #             users.add(line.strip().lower())
    # users=list(users)
    # print (len(users))

    # with open("Top_Stocks.txt", "r") as ins:
    #     tickers = set()
    #     for line in ins:
    #         if(line.strip()!=""):
    #             tickers.add(line.strip().lower())
    #
    # tickers=list(tickers)

    # for ticker in tickers:
    #     scraper = StockTwitsScraper(ticker, "stock")
    #     goodtime = scraper.getGoodTwitsTimestamp()
    #     if not (goodtime == False):
    #         print("Ticker {}, time {}".format(ticker, goodtime))
    #         with open("Best Stocks Backup.txt", 'a') as target:
    #             target.write(ticker+"\n")


    #
    while(1):

        # with open("Top_Stocks.txt", "r") as ins:
        #     tickers = set()
        #     for line in ins:
        #         if (line.strip() != ""):
        #             tickers.add(line.strip().lower())
        #
        # #Tickers = list of to tickers
        # tickers = list(tickers)
        #
        # #Scrape through this list of best stocks
        # for index, ticker in enumerate(tickers):
        #     print("CHECKING: " + str(index) + " / " + str(len(tickers)))
        #     print(ticker)
        #     scraper = StockTwitsScraper(ticker, "stock")
        #     collection_count = scraper.twit_database[ticker].find({}).count()
        #
        #     if(collection_count < 10000):
        #         print("SCRAPING: "+ticker)
        #         scraper.scrape(200)

        #Delete the list of best stocks just used
     #    open('Best Stocks.txt', 'w').close()
     #
     #
     scraper= StockTwitsScraper("vrx", "stock")
     scraper.scrape(1000)

    # print("Done going through all tickers!")



