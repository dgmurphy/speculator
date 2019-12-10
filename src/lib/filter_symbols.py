import pandas as pd 
import numpy as np
import os.path
import sys
from datetime import datetime, timedelta
from lib.ntlogging import logging
from lib.stox_utils import *


def filter_symbols(prices_start_date, prices_end_date):

    summary_input_file = SUMMARY_INPUT_FILE

    if not os.path.exists(summary_input_file):
        logging.critical("Location file not found: " +
                            summary_input_file)
        sys.exit()

    try:
        logging.info("Reading: " + summary_input_file)
        stox_df = pd.read_table(summary_input_file, sep=',')
        stox_df['stock_from_date'] = pd.to_datetime(stox_df['stock_from_date'])
        stox_df['stock_to_date'] = pd.to_datetime(stox_df['stock_to_date'])

    except Exception as e:
        logging.warning("Not parsed: " + summary_input_file + "\n" + str(e))
        sys.exit()
        
    # drop any symbols that don't cover at least the analysis window
    stox_df = stox_df[(stox_df['stock_from_date'] <= prices_start_date) &
                    (stox_df['stock_to_date'] >= prices_end_date)]


    return stox_df['symbol'].tolist()

