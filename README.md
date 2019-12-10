# stox
A trading simulator using a historical stock price data set from Kaggle: https://www.kaggle.com/tsaustin/us-historical-stock-prices-with-earnings-data

# Pre-reqs
* pandas
* matplotlib

# Run
* edit config in stox.ini
* check file name constants in stox_utils.py
* python3 stox.py

# Steps

## Process raw prices file
Loads the RAW_PRICES_INPUT_FILE, removes rows with:
* prices < epsilon
* zero trading volume
* closing price > 3 stds away from mean in a rolling sample window

then writes the CLEANED_PRICES_FILE

## Update symbols file
Loads the SYMBOLS_FILE and EARNINGS_INPUT_FILE.

Drop any symbols that don't cover at least the analysis window.

Sort the symbols list by earnings, descending.

Drop any symbols after the symbols count limit for the analysis.

Write the results to the SYMBOLS_FILE


## Filter prices list
Read the CLEANED_PRICES_FILE.

Drop rows where the symbol is not in the SYMBOLS_FILE

Drop rows outside the analysis time window.

Write the FILTERED_PRICES_FILE


## Run buy-sell process
Read the FILTERED_PRICES_FILE and group by symbol.

Buy each symbol up to budget limit. Hold and then sell (sell fractions if split). 

Record symbol, days held, buy and sell dates, shares bought, prices, gains.

Write BUY_SELL_RESULTS_FILE.

## Run buy-sell analysis
Read the BUY_SELL_RESULTS_FILE and group by symbol.

For each symbol, find the number of black and red trades, max gain/loss transactions.

Drop symbols where few trades were made (e.g. price rose and could not afford to buy anymore).

Write the analysis output file {ANALYSIS_FILE_PREFIX + hold_days + budget_dollars + .csv}

## Make blacklist
This is hard-coded to analyze only the 1K budget buy-sell results.

For each symbol, get the percentage of in-the-black trades for each of the holds.

Return a row with the pct_black for each hold, and the avarage.

Drop any symbols that have percent black trades under the cutoff.

Sort by the average percent black trades.

Write the blacklist file {BLACKLIST_FILE_PREFIX + }

## Run benchmarks
Check the BENCHMARK_PRICES_FILE in stox_utils.py to see which symbol is being used for the benchmark.

Read the BENCHMARK_PRICES_FILE.

Run the buy-sell process for each budget and hold time for selected benchmark symbol.

Write the BENCHMARK_RESULTS_FILE for each budget and hold time.

For each BENCHMARK_RESULTS_FILE run the buy-sell analysis.

Write the analysis results to the file {BENCH_ANALYSIS_FILE_PREFIX + hold_time + budget + .csv}

Run the blacklist for the 1K budget analysis.

Write the file {BENCH_BLACKLIST_FILE_PREFIX + 1000_dollars.csv}








