import pandas as pd
import time, datetime, sys
import os, pathlib
from datetime import datetime, timedelta
from tqdm import tqdm

from keys import twelveDataKey as api_key
from twelvedata import TDClient

# initialize the client
td = TDClient(apikey=api_key)

end_date=datetime.now()
start_date=datetime(2020, 1, 1)

tech_df = td.time_series(
    symbol="GOOG,MSFT",
    interval="1day",
    # start: Jan 1, 2020
    # the line tech_df = fdr.DataReader('GOOG, MSFT', '2020')
    start_date=start_date.strftime('%Y-%m-%d'),
    end_date=end_date.strftime('%Y-%m-%d'),
    outputsize=1000
).as_pandas()

dates = tech_df.index

init_date = dates[0][1]
last_hist_date = dates[-1][1]

init_delay_seconds = 30
interval = 5

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

print ('Sending daily GOOG and MSFT prices from %10s to %10s ...' % (str(init_date)[:10], str(last_hist_date)[:10]), flush=True, file=sys.stderr)
print ("... each day's data sent every %d seconds ..." % (interval), flush=True, file=sys.stderr)
print ('... beginning in %02d seconds ...' % (init_delay_seconds), flush=True, file=sys.stderr)
print ("... MSFT prices adjusted to match GOOG prices on %10s ..."  % (str(init_date)), flush=True, file=sys.stderr)

# show progress
for left in tqdm(range(init_delay_seconds)):
    time.sleep(0.5)

# print data
for stock, date in tech_df.index:
    stock_data = tech_df.loc[stock, date]
    print(stock_data)