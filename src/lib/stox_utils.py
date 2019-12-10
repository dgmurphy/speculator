import pandas as pd 
import numpy as np
import configparser
from lib.ntlogging import logging

RAW_DATA_DIR = "../data/raw/"
STOX_DATA_DIR = "../data/stox/"
KAGGLE_DATA_DIR = RAW_DATA_DIR + "us-historical-stock-prices-with-earnings-data/"
RAW_PRICES_INPUT_FILE = KAGGLE_DATA_DIR + "stocks_latest/stock_prices_latest.csv"
EARNINGS_INPUT_FILE = KAGGLE_DATA_DIR + "stocks_latest/earnings_latest.csv"
SUMMARY_INPUT_FILE = KAGGLE_DATA_DIR + "dataset_summary.csv"
CLEANED_PRICES_FILE = STOX_DATA_DIR + "stock_prices_cleaned.csv"
FILTERED_SYMBOLS_PREFIX = STOX_DATA_DIR + "symbols_"
FILTERED_PRICES_PREFIX = STOX_DATA_DIR + "stock_prices_filtered_"
BUY_SELL_RESULTS_PREFIX = STOX_DATA_DIR + "buy_sell_results_"
ANALYSIS_FILE_PREFIX = STOX_DATA_DIR + "analysis_"
BLACKLIST_FILE_PREFIX = STOX_DATA_DIR + "blacklist_"
BENCHMARK_SYMBOLS = ['DIA', 'IVV']
    
def load_config():

    # Config parser
    ini_filename = "stox.ini"
    logging.info("Reading config from: " + ini_filename)
    config = configparser.ConfigParser()
    try:
        config.read(ini_filename)
    except Exception as e: 
        logging.critical("Error reading .ini file: " + ini_filename)
        logging.critical("Exception: " + str(type(e)) + " " + str(e))
        sys.exit()


    return config

def save_config(config):
    
    ini_filename = "stox.ini"
    with open(ini_filename, 'w') as configfile:
        config.write(configfile)
        logging.info("Saved " + ini_filename)


def load_df(df_file):

    try:
        logging.info("Reading " + df_file)
        df = pd.read_table(df_file, sep=',')
        logging.info("df shape " + str(df.shape))
        
    except Exception as e: 
        logging.critical("Not parsed: " + df_file + "\n" + str(e))
        sys.exit()   

    return df
     


def clean_outliers(df, window):

    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['date'])

    # drop rows with no volume & prices less than epsilon
    eps = 0.01
    df = df.loc[(df.open > eps) &
                (df.close > eps) &
                (df.high > eps) & 
                (df.close > eps) & 
                (df.volume > 0)].copy()
   
    #  keep everthing inside +/- 3 std deviations from mean
        # rolling price sampling window 
    window = int(window)   
    devs = 3 # std devs

    #df['zscore'] = (df.close - df.close.mean())/df.close.std(ddof=0)
    #df['zscore'] = df['zscore'].abs()
    df['mean'] = df['close'].rolling(window, center=True).mean()
    df['std'] = df['close'].rolling(window, center=True).std()
    df = df[(df.close <= df['mean'] + devs * df['std']) &
                        (df.close >= df['mean'] - devs * df['std'])]
    df = df.drop(['mean', 'std'], axis=1)

    return df
