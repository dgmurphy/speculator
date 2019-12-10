import pandas as pd 
import numpy as np
import os.path
import sys
from datetime import datetime, timedelta
from pandas.plotting import register_matplotlib_converters
import matplotlib.pyplot as plt
from lib.ntlogging import logging
from lib.stox_utils import *


def plot_price(cfg):

    param_list = cfg['plot_params'].split(" ")
    if len(param_list) < 3:
        logging.warn("Plot params malformed. Skipping plot.")
        return
    else:
        logging.info(f"Plotting symbol {param_list[0].strip()}")
        logging.info(f"  from {param_list[1]}")
        logging.info(f"  to {param_list[2]}")

    register_matplotlib_converters()
    
    prices_input_file = CLEANED_PRICES_FILE
    #prices_input_file = cfg['raw_data_dir'] + cfg['raw_prices_input_file']
    try:
        logging.info("Reading " + prices_input_file)
        prices_df = pd.read_table(prices_input_file, sep=',')
        prices_df['date'] = pd.to_datetime(prices_df['date'])
        logging.info("Prices df shape " + str(prices_df.shape))
        
    except Exception as e: 
        logging.critical("Not parsed: " + prices_input_file + "\n" + str(e))
        sys.exit()   

    # param string [symbol start-date end-date] 
    #   e.g. IBM 2009-01-01 2019-01-01
    symbol = param_list[0].strip()

    start_list = param_list[1].split('-')
    start_yr = int(start_list[0])
    start_mo = int(start_list[1])
    start_d = int(start_list[2])

    end_list = param_list[2].split('-')
    end_yr = int(end_list[0])
    end_mo = int(end_list[1])
    end_d = int(end_list[2])

    date_start = pd.Timestamp(start_yr, start_mo, start_d)
    date_end = pd.Timestamp(end_yr, end_mo, end_d)
   

    # filter on date range
    logging.info("Filtering on date range")
    df = prices_df[(prices_df['date'] >= date_start) & (prices_df['date'] <= date_end)]
    df = df.sort_values(['date'])

    # get group for this symbol
    logging.info("Filtering on symbol")
    df = df.groupby('symbol').get_group(symbol)

    # write df to file
    span_str = (date_start.strftime("%Y-%m-%d") + "_" +
        date_end.strftime("%Y-%m-%d"))
    csv_name = STOX_DATA_DIR + symbol + "_" + span_str + ".csv"
    df.to_csv(csv_name, index=False, sep="\t", float_format='%.3f')

    # plot open/close price
    fig = plt.figure()
    plt.suptitle(symbol, fontsize=10)
    plt.scatter(df['date'].tolist(), df['open'], color='green', s=2)
    plt.scatter(df['date'].tolist(), df['close'], color = 'blue', s=2)

    plt_filename = STOX_DATA_DIR + symbol + "_" + span_str + ".png"
    plt.savefig(plt_filename)
    plt.show()


