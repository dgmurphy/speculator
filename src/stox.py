import pandas as pd 
import numpy as np
import os.path
import sys
import configparser
from math import floor
from datetime import datetime, timedelta
from lib.ntlogging import logging
import pathlib
from pathlib import Path
from lib.filter_symbols import *
from lib.filter_prices import *
from lib.buy_sell_v3 import *
from lib.sort_symbols_by_eps import *
from lib.stox_utils import *
from lib.analyze import *
from lib.plot_price import *
from lib.clean_prices import *
from lib.test_cleaner import *
from lib.make_blacklist import *


def get_symbol_file_rows():

    input_string = input('input: year-span =>  ')
    symbol_file = FILTERED_SYMBOLS_PREFIX + years + "years.csv"

    if os.path.exists(symbol_file):
        symbol_df = pd.read_table(symbol_file, sep=',')
        return len(symbol_df)
    else:
        return 0


def run_prices_filter(cfg):

    years_back = input("input: number of prior years => ")

    logging.info("Running prices filter...")
    prices_df = load_df(CLEANED_PRICES_FILE)

    # get the timestamps for the year span
    years_int = int(years_back)
        
    end_date_str= cfg['analysis_end_date'].strip()
    end_list = end_date_str.split('-')
    end_yr = int(end_list[0])
    end_mo = int(end_list[1])
    end_d = int(end_list[2])
    prices_end_date = pd.Timestamp(end_yr, end_mo, end_d)

    prices_start_date = prices_end_date - pd.Timedelta(days=years_int * 365)
    print(f'start: {str(prices_start_date)}  end: {str(prices_end_date)}')

    filter_prices(in_df, prices_start_date, prices_end_date, years_int)
    
    #input("OK >")


def run_buy_sell(cfg):

    low_price_cutoff = float(cfg['low_price_cutoff'])

    input_string = input('input: year-span hold_days budget_dollars fee_dollars =>  ')

    args = input_string.split()
    yearspan = int(args[0])
    hold_days = int(args[1])
    budget_dollars = int(args[2])
    min_trades = int(args[3])

    logging.info("Running buy-sell...")
    buy_sell_v3(budget_dollars, fee_dollars, hold_days, low_price_cutoff, yearspan)
    #input("OK >")


def rm_stoxdir(cfg):
    stox_dir = STOX_DATA_DIR
    for p in Path(stox_dir).glob("*.*"):
        p.unlink()
    logging.info("Removed stox data.")
    #input("OK >")


def run_analysis():

    input_string = input('input: year-span hold_days budget_dollars min_trades =>  ')
    logging.info("Running analysis...")
    args = input_string.split()
    yearspan = args[0]
    hold_days = args[1]
    budget_dollars = args[2]
    min_trades = int(args[3])
    analyze(hold_days, budget_dollars, yearspan, min_trades)
    #input("OK >")


def run_auto(cfg): 

    startTime = pd.datetime.now()
    logging.info("Auto run started at " + str(startTime))

    run_clean_prices(cfg)   

    logging.info("loading cleaned prices...")
    prices_df = load_df(CLEANED_PRICES_FILE)

    logging.info("Cleaned prices df shape " + str(prices_df.shape))

    # append the benchmark symbols to the prices_df
    logging.info("appending benchmarks")
    for benchsym in BENCHMARK_SYMBOLS:
        bench_input_file = RAW_DATA_DIR + benchsym + ".csv"
        logging.info("Reading: " + bench_input_file)
        bench_df = pd.read_table(bench_input_file, sep=',')
        logging.info("bench_df shape " + str(bench_df.shape))   
        bench_df['date'] = pd.to_datetime(bench_df['date'])    
        prices_df = prices_df.append(bench_df, ignore_index = True)  

    logging.info("Cleaned prices with benchmarks df shape " + str(prices_df.shape))  

    years  = cfg['years_list'].strip()
    years_list = years.split(',')
    symbols_limit = int(cfg['symbols_limit'])

    for years_back in years_list:

        # get the timestamps for the year span
        years_int = int(years_back)
        
        end_date_str= cfg['analysis_end_date'].strip()
        end_list = end_date_str.split('-')
        end_yr = int(end_list[0])
        end_mo = int(end_list[1])
        end_d = int(end_list[2])
        prices_end_date = pd.Timestamp(end_yr, end_mo, end_d)

        prices_start_date = prices_end_date - pd.Timedelta(days=years_int * 365)
        print(f'start: {str(prices_start_date)}  end: {str(prices_end_date)}')

        # make the filtered symbols list
        sort_symbols_by_eps(prices_start_date, prices_end_date, 
            years_int, symbols_limit, BENCHMARK_SYMBOLS)


        # filter prices on date span
        logging.info("writing filtered prices")
        filter_prices(prices_df, prices_start_date, prices_end_date, years_int)

        run_buy_sell_analyze(cfg, years_int)

        budget_cfg = cfg['budget_list'].strip()
        budget_lst = budget_cfg.split(",")
        for budget in budget_lst:
            b = float(budget.strip())  
            make_blacklist(b, cfg['hold_times_list'], years_int) 

    endTime = pd.datetime.now()
    logging.info("Auto run finished at " + str(endTime))
    logging.info("Elapsed time: " + str(endTime - startTime))
    
    input("DONE with AUTO > ")


def run_buy_sell_analyze(cfg, yearspan):

    min_trades = int(cfg['analyze_min_trades'])

    holds_cfg = cfg['hold_times_list'].strip()
    budget_cfg = cfg['budget_list'].strip()

    holds_lst = holds_cfg.split(",")
    budget_lst = budget_cfg.split(",")

    low_price_cutoff = float(cfg['low_price_cutoff'])

    for budget in budget_lst:
        b = budget.strip()

        for hold in holds_lst:
            h = hold.strip()

            info_str = f"Running buy-sell with {h} days and {b} dollars..."
            logging.info(info_str)

            buy_sell_v3(int(b), 0, int(h), float(low_price_cutoff), yearspan)

            info_str = f"Running analyze with {h} days and {b} dollars..."
            logging.info(info_str)

            analyze(h, b, yearspan, min_trades)

    #input("OK > ")


def run_price_plot(cfg):
    logging.info("Running price plot...")
    plot_price(cfg)


def run_clean_prices(cfg):
    logging.info(f"Cleaning with window = {cfg['rolling_sample_window']}")
    clean_prices(cfg)
    #input("OK >")


def run_cleaner_test(cfg):
    logging.info("Running cleaner test...")
    test_cleaner(cfg)
    

def run_make_blacklist(cfg):

    input_str = input("input: year-span budget =>  ")
    args = input_str.split()
    yearspan = int(args[0])
    budget = int(args[1])
    holds = cfg['hold_times_list']

    logging.info("Running blacklist...")
    make_blacklist(budget, holds, yearspan)
    #input("OK >")



def main():

    # make the output dir if needed
    stox_dir = STOX_DATA_DIR
    pathlib.Path(stox_dir).mkdir(exist_ok=True)

    reply = "none"
    while reply != "q":

        # pass the last reply in to menu to show last command
        reply, cfg = show_menu(reply)

        if reply == '0':
            logging.shutdown()
            if os.path.exists("log-stox.log"):
                os.remove("log-stox.log")
            logging.info("Log deleted.")
            #input("OK >")

        elif reply == '1':
            rm_stoxdir(cfg)

        elif reply == "2":
            run_clean_prices(cfg)

        elif reply == "3":
            write_symbols(cfg)

        elif reply == "4":
            run_prices_filter(cfg)

        elif reply == "5":
            run_buy_sell(cfg)

        elif reply == "6":
            run_analysis()

        elif reply == "7":
            run_price_plot(cfg)

        elif reply == "8":
            run_cleaner_test(cfg)

        elif reply == "9":
            run_buy_sell_analyze(cfg)

        elif reply == "10":
            run_make_blacklist(cfg)

        elif reply == "11":
            run_auto(cfg)


# Kaggle:
# https://www.kaggle.com/tsaustin/us-historical-stock-prices-with-earnings-data

def show_menu(previous):

    config = load_config()
    cfg = config['stox']



    # menu
    prompt = "\n---- STOX MENU ----"

    prompt += "\nCommands:"           
    prompt += "\n0) Delete the log"
    prompt += "\n1) Delete generated data"
    prompt += "\n2) Process raw prices file (clean outliers)"
    prompt += "\n3) Update symbols file (filter by date & limit)"
    prompt += "\n4) Filter prices list (dates, symbols)"
    prompt += "\n5) Run buy-sell process"
    prompt += "\n6) Analyze buy-sell results"
    prompt += "\n7) Plot prices: " + cfg['plot_params']
    prompt += "\n8) Run cleaner test: " + cfg['cleaner_test_params']
    prompt += "\n9) Run buy-sell + analyze matrix"
    prompt += "\n10) Make blacklist"
    prompt += "\n11) Auto"
    prompt += "\n"
    prompt += "\nq) Quit"
    prompt += "\nLast command was: " + str(previous)
    prompt += "\nstox > "
    reply = input(prompt)

    return reply.strip(), cfg


if __name__ == '__main__':
    main()
    print("DONE\n")