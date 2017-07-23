import requests
from bs4 import BeautifulSoup
import pymongo
import datetime as dt
import sys

class StockTwitsScraper(object):

    # Connect to MongoDB and this ticker's collection
    def __init__(self, ticker):

        self.ticker = ticker.lower()

        # Connect to MongoDB
        try:
            conn = pymongo.MongoClient()
            print("Connected to MogoDB successfully")
        except:
            print("Could not connect to MongoDB!")
            sys.exit(1)

        #Connect to database and collection
        self.twit_collection = conn.StockTwitsDatabase[self.ticker]
        self.twit_collection.create_index([('id', pymongo.DESCENDING)], unique=True)
        self.user_database = conn.UserTwitsDatabase


        # Get StockTwits stream id for this ticker
        self.streamid = self.getStreamId()

        if not (self.streamid):
            print("This ticker is not on StockTwits!")
            sys.exit(1)

    # Check if this ticker is on StockTwits + find streamid
    def getStreamId(self):

        # Parse html response to get 'stream_id' of ticker
        html_response = requests.get('https://stocktwits.com/symbol/' + str(self.ticker))
        bs_obj = BeautifulSoup(html_response.content, "html.parser")
        data_id_attr = bs_obj.find_all(attrs={"data-id": True})

        if data_id_attr:
            return data_id_attr[0].get("data-id")
        else:
            return False

    # Scrape twits form StockTwits
    def scrape(self, calls):
        start = None

        # Make calls to StockTwits, return a batch of twits each time
        for i in range(calls):
            print("Call {}/{}".format(i, calls))
            batch = self.getBatch(start)

            # If there are no twits in this batch, break
            if not (batch):
                break

            # Store the twits in this batch into database

            # If we have already stored a twit in this batch, find new start
            if not (self.storeTwits(batch)):
                start = self.getStartingPoint()

            # If all twits in this batch were not stored already, set start to last twit id in batch
            else:
                start = batch[-1]['id']
                print("Starting from last twit in previous batch: {} - {}".format(start, batch[-1]['created_at']))

    # From this collection, get a good starting point to begin scraping
    def getStartingPoint(self):

        if (self.twit_collection.count() == 0):
            return None


        sortedlist = list(self.twit_collection.find().sort('id', -1))
        latestdate = dt.datetime.strptime(sortedlist[0]['created_at'], '%a, %d %b %Y %H:%M:%S -%f')
        listsize = len(sortedlist)

        for index in range(listsize-1):
            date = dt.datetime.strptime(sortedlist[index]['created_at'], '%a, %d %b %Y %H:%M:%S -%f')
            nextdate = dt.datetime.strptime(sortedlist[index+1]['created_at'], '%a, %d %b %Y %H:%M:%S -%f')
            delta = date - nextdate

            # If there is a 4 day gap in twits, start there
            if(delta.days > 2):
                print("GAP FOUND: Starting at ({}/{}) - This date: {}, next date: {}".format(index,listsize,date,nextdate))
                return sortedlist[index]['id']

            #If we have reach the last twit in collection, start there
            elif (index == listsize - 2):
                print("REACHED LAST TWIT: Starting at ({}/{}) - This date: {}".format(index,listsize,date))
                return sortedlist[index+1]['id']

    # Make request to StockTwits to get a batch of twits in json from start
    def getBatch(self, start):
        payload = {}
        payload["stream"] = "symbol"
        payload["max"] = start
        payload["stream_id"] = self.streamid

        r = requests.get(
            url='https://stocktwits.com/streams/poll?/symbol/' + self.ticker,
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

        return r.json()['messages']

    # Insert this batch of twits into database
    def storeTwits(self, batch):

        #Iterate through each twit in this batch
        for twit in batch:

            #Check if this twit has a sentiment
            if not (twit['sentiment']):
                print("No sentiment, skipping twit")

            elif (self.twit_collection.find({'id': twit['id']}).count() > 0):
                print("This twit has already been stored")
                print(twit['created_at'])
                return False

            #If this twit has not been stored, insert into collection
            elif(twit['sentiment']):
                print("{} - {} - {}".format(twit['created_at'], twit['id'], twit['body']))
                self.user_collection = self.user_database[twit['user']['username'].lower()]
                self.user_collection.create_index([('id', pymongo.ASCENDING)], unique=True)

                try:
                    self.twit_collection.insert(twit)
                    print("Sentiment was {}, inserted into ticker collection".format(twit['sentiment']['class']))
                except:
                    print("ERROR INSERTING TWIT")
                    return False
                try:
                    self.user_collection.insert(twit)
                    print("Sentiment was {}, inserted into user collection".format(twit['sentiment']['class']))
                except:
                    print("Already in user collection")

        return True

if __name__ == '__main__':

    with open("NeedMore.txt", "r") as ins:
        tickers = set()
        for line in ins:
            if (line.strip() != ""):
                tickers.add(line.strip().lower())
    tickers = list(tickers)

    for ticker in tickers:
        print("Scraping {}".format(ticker))
        scraper = StockTwitsScraper(ticker)
        scraper.scrape(2000)
    print("Done scraping")

     # scraper = StockTwitsScraper("msft")
     # scraper.scrape(2000)





