import pandas as pd 
import numpy as np
import os.path
import sys
from datetime import datetime, timedelta
from lib.ntlogging import logging
from lib.stox_utils import *

def load_df(df_file):

    try:
        logging.info("Reading " + df_file)
        df = pd.read_table(df_file, sep=',')
        logging.info("df shape " + str(df.shape))
        
    except Exception as e: 
        logging.critical("Not parsed: " + df_file + "\n" + str(e))
        sys.exit()   

    return df
     


def clean_prices(cfg):

    prices_input_file = RAW_PRICES_INPUT_FILE
    prices_output_file = CLEANED_PRICES_FILE
 
    # clean up the existing output file (ignore !exists error)
    try:
        os.remove(prices_output_file)
    except OSError:
        pass   

    # load prices
    prices_df = load_df(prices_input_file).groupby('symbol')

    write_header = True

    # Clean outliers 
    for symbol, sym_df in prices_df:

        sym_df = prices_df.get_group(symbol)
        logging.info(f"Cleaning outliers for {symbol}")
        sym_df = clean_outliers(sym_df, cfg['rolling_sample_window'])
        
        with open(prices_output_file, 'a') as f:
            sym_df.to_csv(f, index=False, sep=",", header=write_header)
            write_header = False

                   

    
    
