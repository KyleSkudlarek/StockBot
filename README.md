# StockBot

StockBot is an "in progress" collection of python scripts that allows a user to mine the 'twits' of niche social media website [StockTwits.com](https://stocktwits.com), store them locally in [MongoDB](https://www.mongodb.com/), and then derive and graph a "sentiment metric" from the mined twits to analyze in the context of a stocks historical price.

<br></br>

## Table of Contents

- [Components](#components)
- [Dependencies](#dependencies)

<br></br>

## Components

Currently there are several python scripts in this repo, each dedicated to performing a specific part of the overall mining/analysis process that makes up StockBot. There are also several "helper" scripts that were made to help in the overall "research process" of playing with data from StockBot (e.g. FindTopWatched.py finds the "most followed" stock tickers on StockTwits.com, so I can see which stocks have the most twit data that would be best for analysis)

### Mining
  * [TwitScraper.py](TwitScraper.py)
    * This script sends requests to StockTwits.com (based on a stock ticker and a twit ID starting point) and receives back batches of twits for the given stock and time period, and then stores them locally in a MongoDB database.
    * The script will not store twits that have already been stored, and will find a good "starting point" (twit id) based on the current collection of twits for that stock. For example, if I collected twits for ticker "AAPL" from "May 2017 - June 2017" and then I ran the mining script today, it would mine and store all twits from now until June 2017, stop when it encounters a twit from June 2017 that it has alkready stored, and automatically beging scraping twits going back from May 2017. 
  
### Analysis
  * [FinanceScraper.py](FinanceScraper.py)
    * This script aggregates all of the stored twits for a given sotkc ticker in the MongoDB database to calculate a "sentiment index" (based on number of bullish/bearish twits) over time. This script also pulls that stocks historical financial data from Google Finance, and graphs the financial data ontop of the twit "sentiment data" for easy analysis.
    
---

<br></br>

## Dependencies
  * [Pandas](https://github.com/pandas-dev/pandas)
  * [NumPy](https://github.com/numpy/numpy)
  * [Matplotlib](https://github.com/matplotlib/matplotlib)
  * [Pymongo](https://github.com/mongodb/mongo-python-driver/tree/master/pymongo)
  * [BeautifulSoup4](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)
  * [Requests](https://github.com/requests/requests)



