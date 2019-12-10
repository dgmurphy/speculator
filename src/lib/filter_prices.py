import pandas as pd 
import numpy as np
import os.path
import sys
from datetime import datetime, timedelta
from lib.ntlogging import logging
from lib.stox_utils import *



def filter_prices(in_df, prices_start_date, prices_end_date, yearspan):

    prices_df = in_df.copy()

    symbols_input_file = FILTERED_SYMBOLS_PREFIX + str(yearspan) + "years.csv"
    filtered_prices_output_file = FILTERED_PRICES_PREFIX + str(yearspan) + "years.csv"
 
    # load symbols
    symbols_df = load_df(symbols_input_file)
    symbols = symbols_df['symbol'].tolist()

    # filter on symbols
    prices_df = prices_df[prices_df['symbol'].isin(symbols)]
    logging.info("Filtered symbols df shape " + str(prices_df.shape))

    # filter on date range
    logging.info("Filtering by date range.")
    prices_df['date'] = pd.to_datetime(prices_df['date'])
    prices_df = prices_df[(prices_df['date'] >= prices_start_date) &
                   (prices_df['date'] <= prices_end_date)].sort_values(
                   ['symbol', 'date'])

                   
    logging.info("Filtered dates df shape " + str(prices_df.shape))

    # write filtered prices
    logging.info("Writing filtered prices to " + filtered_prices_output_file)
    prices_df.to_csv(filtered_prices_output_file, index=False)
    

