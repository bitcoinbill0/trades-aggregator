import os, sys
import pandas as pd
import multiprocessing
from datetime import datetime, timedelta

# read interaval from command line args
if len(sys.argv) < 2:
    print('Error! You must provide interval in minutes as command line arguments.')
    exit(-1)

# create ohlc dir if doesn't exist
if not os.path.exists('data/ohlc'):
    os.makedirs('data/ohlc')

# load input files from disk
input_files = os.listdir(f'{os.getcwd()}/data')
input_files.sort()

# increment the volume of the current candle
def inc_vol(row, current_candle):
    if row['side'] == 'Sell':
        current_candle['buy_volume'] += row['size']
    else:
        current_candle['sell_volume'] += row['size']

# save a new candle
def add_candle(current_candle, candlesticks, ts):
    current_candle['timestamp'] = ts
    candlesticks.append(current_candle)
    print(f'Added candle for {ts}')

# build candles for dataframe from input file
def build_candles(df, candlesticks):
    current_candle = {}
    ts = None
    for index, row in df.iterrows():
        ts = datetime.strptime(row['timestamp'].split('.')[0], '%Y-%m-%dD%H:%M:%S')
        if not current_candle:
            current_candle['open'] = row['price']
            current_candle['low'] = row['price']
            current_candle['high'] = row['price']
            current_candle['buy_volume'] = 0
            current_candle['sell_volume'] = 0
            current_candle['timestamp'] = ts
            inc_vol(row, current_candle)
        else:
            current_candle['close'] = row['price']
            inc_vol(row, current_candle)
            if row['price'] > current_candle['high']:
                current_candle['high'] = row['price']
            elif row['price'] < current_candle['low']:
                current_candle['low'] = row['price']
            if current_candle['timestamp'] + timedelta(seconds=(60 * int(sys.argv[1]))) < ts:
                add_candle(dict(current_candle), candlesticks, ts)
                current_candle = {}
    add_candle(current_candle, candlesticks, ts)

# parse input file and save to CSV as candlesticks
def parse_file(file):
    candlesticks = []
    print(f'Loading {file}')
    df = pd.read_csv(f'{os.getcwd()}/data/{file}')
    df = df.loc[df['symbol'] == 'XBTUSD']
    build_candles(df, candlesticks)
    pd.DataFrame(candlesticks).to_csv(f'data/ohlc/{file}', index=False)

# create a pool to process days in parallel
procs = multiprocessing.cpu_count()
print(f'Running {procs} days in parallel')
pool = multiprocessing.Pool(processes=procs)

results = []
for file in input_files:
    if '.csv' in file:
        async_func = pool.apply_async(parse_file, args=(file,))
        results.append(async_func)

# block until all files are finished
for result in results:
    result.get()
