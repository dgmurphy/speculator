import pandas as pd 
import numpy as np
import os.path
import sys
from math import floor
from datetime import datetime, timedelta
from lib.ntlogging import logging
from lib.stox_utils import FILTERED_PRICES_PREFIX, BUY_SELL_RESULTS_PREFIX

# This builds a result row for every trading day in the set.
# Transactions that are pending sale until the hold period is reached
# are put into the pending_lst.
# A sale will close out the first item from the pending list, adjust the shares
# owned at the sell date by the split coefficients, and append the sale
# to  the results_lst.  The final dataframe is built from the results_lst.

def buy_sell_v3(budget_dollars, fee_dollars, hold_days, low_price_cutoff, yearspan):

    prices_input_file = FILTERED_PRICES_PREFIX + str(yearspan) + "years.csv"

    hold_str = str(hold_days)
    budget_dollars_str = str(int(budget_dollars))
    buy_sell_output_postfix = str(yearspan) + "years_" + hold_str + "days_" + budget_dollars_str + "dollars.csv"
    buy_sell_output_file = BUY_SELL_RESULTS_PREFIX +  buy_sell_output_postfix

    # clean up the existing output file (ignore !exists error)
    try:
        os.remove(buy_sell_output_file)
    except OSError:
        pass   

    # load prices and group by symbol
    try:
        logging.info("Reading: " + prices_input_file)
        stox_df = pd.read_table(prices_input_file, sep=',')
        stox_df['date'] = pd.to_datetime(stox_df['date'])
        stox_df = stox_df.groupby('symbol')
        logging.info("Found " + str(len(stox_df)) + " symbols in price data.")
        
    except Exception as e:
        logging.warning("Not parsed: " + prices_input_file + "\n" + str(e))
        sys.exit()
    
    
    numsyms = len(stox_df)  # total number of symbols
    cant_afford = set()  # set of symbols whose unit share price exceeds budget
    penny_stocks = set()  # set of low price symbols

    results_lst = []  # completed transactions
    write_header = True  # results file header (write once flag)

    # Loop over each symbol
    symnum = 1  # symbol idx
    
    for symbol, sym_df in stox_df:

        max_gain = max_loss = 0.0
        pending_lst = []   # has buy attributes for each buy date

        # TODO make sure the sym_df is in date order ascending

        row_idx = 0
        for row in sym_df.itertuples():

            buy_price, shares_bought, status = buy(row, budget_dollars)
            split_coeff = row.split_coefficient

            if status == 'price_high':
                cant_afford.add(symbol)  # todo: count these
            elif status == 'price_low':
                penny_stocks.add(symbol) # todo: count these
            
            # add current row to pending sale list
            pending_lst.append([symbol, row_idx, row.date, shares_bought, 
                                buy_price, split_coeff])
            
            # Once the pending sales list is full, start creating results list
            if row_idx >= hold_days:  

                result_row = sell_row(row, pending_lst, fee_dollars)

                returns = float(result_row[11])

                # just for logging
                if returns > max_gain: max_gain = returns
                if returns < max_loss: max_loss = returns

                # add the result row to results list
                results_lst.append(result_row)
                
                # remove the sold row 
                del pending_lst[0]

            row_idx += 1

        # peridocially write the results list
        qmax = 100000
        if len(results_lst) >= qmax:
            logging.info(f"Writing {qmax} results to {buy_sell_output_file}")
            append_csv(buy_sell_output_file, results_lst, write_header, low_price_cutoff)
            write_header = False
            results_lst = []

        symnum += 1    # keep track of how many symbols have been processed
        logging.info(f"{symbol} \t\t[{symnum} of {numsyms}] \ttrade days: " +
                     f"{str(len(sym_df))} max_gain: {max_gain:.2f} " +
                     f"max_loss: {max_loss:.2f}")
 

    # final csv update
    if len(results_lst) > 0:
        logging.info(f"Writing {len(results_lst)} results to {buy_sell_output_file}")
        append_csv(buy_sell_output_file, results_lst, write_header, low_price_cutoff)
    
    logging.info("Zero shares bought (price exceeds budget): " + 
                 str(cant_afford))
    logging.info("Zero shares bought (price too low): " + 
                 str(penny_stocks))


# Drop unwanted rows and update csv
def append_csv(csv_file, results_lst, write_header, low_price_cutoff):

    # columns for output dataframe
    cols = ['symbol', 'interval', 'trading_days_held', 'cal_days_held',
            'buy_date', 'shares_bought', 'buy_price', 'sell_date', 
            'shares_sold', 'sell_price', 'fee', 'gain_total']

    out_df = pd.DataFrame(results_lst, columns=cols).sort_values(['symbol', 
                          'interval'], ascending=True)

    # Drop zero-shares transactions
    out_df = out_df[out_df.shares_bought > 0]

    # drop penny stock transactions
    out_df = out_df[out_df.buy_price > low_price_cutoff]

    with open(csv_file, 'a') as f:
        out_df.to_csv(f, index=False, sep=",", float_format='%.3f', 
                      header=write_header)


def sell_row(row, pending_lst, fee):

    sold_row = pending_lst[0]
    symbol = sold_row[0]
    idx = sold_row[1]
    buy_date = sold_row[2]
    shares_bought = sold_row[3]
    buy_price = sold_row[4]
    cost_dollars = shares_bought * buy_price

    # update num shares owned 
    shares_owned = shares_bought
    split_coeff = 1  # default, shouldnt need
    for pending_row in pending_lst[1:]:
        split_coeff = pending_row[5]
        split_coeff_str = f"{split_coeff:.1}"    
        if split_coeff_str != "1e+00":
            shares_owned = shares_owned * split_coeff

    sell_date = row.date
    sell_price = (float(row.open) + float(row.close)) / 2.0
    sold_dollars = shares_owned * sell_price

    cal_days = (sell_date - buy_date).days
    trading_days = len(pending_lst) - 1
    gain_total = sold_dollars - cost_dollars - fee

    result_row = [symbol, idx, trading_days, cal_days,
                  buy_date, shares_bought, buy_price,
                  sell_date, shares_owned, sell_price,
                  fee, gain_total]

    return result_row

    
def buy(row, budget_dollars):
    
    status = "ok"   # 'ok' if shares bought
                    # 'price_high' if too expensive
                    # 'price_low' if too cheap

    buy_price = (float(row.open) + float(row.close)) / 2.0
    shares_bought = 0

    # share lower price limit
    epsilon = .0001
    if buy_price < epsilon:
        status = "price_low"
    else:
        # can only buy whole shares
        shares_bought = floor(budget_dollars / buy_price)
        if shares_bought < 1:
            status = "price_high"
    
    return buy_price, shares_bought, status
    
