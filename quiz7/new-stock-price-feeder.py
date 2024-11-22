import pandas as pd
import time, datetime, sys
import os, pathlib
from datetime import datetime, timedelta
from tqdm import tqdm

from keys import twelveDataKey as api_key
from twelvedata import TDClient

# initialize the client
td = TDClient(apikey=api_key)

stocks = ['AAPL', 'MSFT']
end_date=datetime.now()
start_date=datetime(2020, 1, 1)
interval = "15min"

for stock in stocks:
    # send requests in chunks
    current_date = start_date

    while current_date < end_date:
        tech_df = td.time_series(
            symbol="AAPL,MSFT",
            interval=interval,
            # start: Jan 1, 2020
            # the line tech_df = fdr.DataReader('GOOG, MSFT', '2020')
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=(current_date + timedelta(days=30)).strftime('%Y-%m-%d'),
            outputsize=100
        ).as_pandas()

        for stock, date in tech_df.index:
            stock_data = tech_df.loc[stock, date]

            # print each value separated by a tab
            print(f"{stock}\t{date}\t{stock_data['open']}\t{stock_data['high']}\t{stock_data['low']}\t{stock_data['close']}\t{stock_data['volume']}")

            # time delay after each print 
            time.sleep(2.5)
        
        # move to next 30-day period
        current_date += timedelta(days=30)

        # add a delay period
        #time.sleep(30)