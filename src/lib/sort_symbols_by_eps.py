import pandas as pd 
import numpy as np
import os.path
import sys
from datetime import datetime, timedelta
import configparser
from lib.ntlogging import logging
from lib.filter_symbols import *
from lib.stox_utils import *


def load_earnings(fname):

    try:
        logging.info("Reading " + fname)
        earn_df = pd.read_table(fname, sep=',')
        logging.info("Earnings df shape " + str(earn_df.shape))
        
    except Exception as e: 
        logging.critical("Not parsed: " + fname + "\n" + str(e))
        sys.exit()   

    return earn_df
     

def sort_symbols_by_eps(prices_start_date, prices_end_date, yearspan, limit, benchmarks):

    earnings_input_file = EARNINGS_INPUT_FILE
    sorted_symbols_output_file = FILTERED_SYMBOLS_PREFIX + str(yearspan) + "years.csv"
        
    # load earnings
    earn_df = load_earnings(earnings_input_file)

    logging.info("Filtering by date range.")
    earn_df['date'] = pd.to_datetime(earn_df['date'])

    #Filter by start / end dates
    earn_df = earn_df[(earn_df['date'] >= prices_start_date) &
                   (earn_df['date'] <= prices_end_date)]

    logging.info("Filtered dates df shape " + str(earn_df.shape))

    earn_df = earn_df.groupby('symbol')
    logging.info(str(len(earn_df)) + " symbols groups found")

    # get list of symbols whose valid listing dates cover the start-end period
    logging.info("Filtering symbols (keep symbols covering the date span)")
    symbols_span_list = filter_symbols(prices_start_date, prices_end_date)
    logging.info(f"{len(symbols_span_list)} symbols cover the date span for {str(yearspan)} year history")

    # make df of sorted symbols
    cols = ['symbol', 'avg_eps']
    lst = []
    for symbol, symbol_grp in earn_df:
        # keep only symbols that pass the symbols listing span
        if symbol in symbols_span_list:
            #logging.info("Getting mean eps for " + symbol)
            avg_eps = symbol_grp['eps'].mean()
            lst.append([symbol, avg_eps])
        
    symsort_df = pd.DataFrame(lst, columns=cols)
    #symsort_df = symsort_df.dropna()
    symsort_df = symsort_df.sort_values('avg_eps', ascending=False)

    # apply symbols limit
    symsort_df = symsort_df[:limit]

    # add the benchmark symbols
    rows_list = []
    for bsym in benchmarks:
        bdict= {"symbol":bsym, "avg_eps":999}
        rows_list.append(bdict)

    bdf = pd.DataFrame(rows_list)    
    symsort_df = bdf.append(symsort_df)


    
    # write sorted symbols
    logging.info(f"Writing {len(symsort_df)} sorted symbols to " + 
                 sorted_symbols_output_file)

    symsort_df.to_csv(sorted_symbols_output_file, index=False)
    return symsort_df['symbol']

