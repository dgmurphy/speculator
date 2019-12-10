import pandas as pd 
import numpy as np
import os.path
import sys
from datetime import datetime, timedelta
from pandas.plotting import register_matplotlib_converters
import matplotlib.pyplot as plt
from lib.ntlogging import logging
from lib.stox_utils import *

def load_prices(prices_file):

    try:
        logging.info("Reading " + prices_file)
        prices_df = pd.read_table(prices_file, sep=',')
        prices_df['date'] = pd.to_datetime(prices_df['date'])
        logging.info("Prices df shape " + str(prices_df.shape))
        
    except Exception as e: 
        logging.critical("Not parsed: " + prices_file + "\n" + str(e))
        sys.exit()   

    return prices_df
     

def plot_price(name, df, date_start, date_end):

    print("start: " + str(date_start))
    print("end: " + str(date_end))
   
    df = df[(df['date']>=date_start) & (df['date']<=date_end)]
    df = df.sort_values(['date'])
    
    span_str = (date_start.strftime("%Y-%m-%d") + "_" +
        date_end.strftime("%Y-%m-%d"))
    csv_name = "../data/" + name + "_" + span_str + ".csv"
    df.to_csv(csv_name, index=False, sep="\t")

    fig = plt.figure()
    plt.scatter(df['date'], df['close'], color='green', s=2)
    plt.scatter(df['date'], df['close_adjusted'], s=2)
    #plt_price = plt.scatter(df['date'], df['split_coefficient'])


def main():

    proc_starttime = datetime.now()
    logging.info("Processing started: " + str(proc_starttime))

    prices_input_file = RAW_PRICES_INPUT_FILE

    register_matplotlib_converters()
        
    # load prices
    prices_df = load_prices(prices_input_file)
 
    # which symbols have splits?
    prices_df = prices_df.groupby('symbol')
    num_splits = num_rsplits = 0
    for name, group in prices_df:
        max_split = group['split_coefficient'].max()
        min_split = group['split_coefficient'].min()
        if max_split > 1.0 :
            #logging.info(str(name) + " max split " + str(max_split))
            num_splits += 1
        if min_split < 1.0:
            num_rsplits += 1
            logging.info(str(name) + " r split " + str(min_split))
    
    logging.info(str(num_splits) + " symbols with splits > 1") 
    logging.info(str(num_rsplits) + " symbols with splits < 1") 

    # plot split coeff
    #grpname = "BRK.A"
    #grp = prices_df.get_group(grpname)
    #plot_price(grpname, grp, pd.Timestamp(2009,1,1), pd.Timestamp(2019, 1, 1))


    proc_endtime = datetime.now()
    logging.info(("Total Processing time (min): " + 
        str((proc_endtime - proc_starttime).total_seconds() / 60.0)))

    #plt.show()


if __name__ == '__main__':
    main()
    print("DONE\n")
