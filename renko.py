import os, sys
import pandas as pd
import multiprocessing
from datetime import datetime, timedelta

# read block size from command line args
if len(sys.argv) < 2:
    print('Error! You must provide block size in ticks as command line arguments.')
    exit(-1)

# create renko dir if doesn't exist
if not os.path.exists('data/renko'):
    os.makedirs('data/renko')

# load input files from disk
input_files = os.listdir(f'{os.getcwd()}/data')
input_files.sort()

# increment the volume of the current block
def inc_vol(row, current_block):
    if row['side'] == 'Sell':
        current_block['sell_volume'] += row['size']
    else:
        current_block['buy_volume'] += row['size']

# save a new block
def add_block(current_block, blocks, ts):
    current_block['timestamp'] = ts
    blocks.append(current_block)
    print(f'Added block for {ts}')

# build renko blocks for dataframe from input file
def build_blocks(df, blocks):
    current_block = {}
    for index, row in df.iterrows():
        if not current_block:
            current_block['open'] = row['price']
            current_block['low'] = row['price']
            current_block['high'] = row['price']
            current_block['buy_volume'] = 0
            current_block['sell_volume'] = 0
            inc_vol(row, current_block)
        else:
            current_block['close'] = row['price']
            inc_vol(row, current_block)
            if row['price'] > current_block['high']:
                current_block['high'] = row['price']
            elif row['price'] < current_block['low']:
                current_block['low'] = row['price']
            if (row['price'] >= (current_block['open'] + (int(sys.argv[1]) * 0.5))) or \
                (row['price'] <= (current_block['open'] - (int(sys.argv[1]) * 0.5))):
                ts = datetime.strptime(row['timestamp'].split('.')[0], '%Y-%m-%dD%H:%M:%S')
                add_block(dict(current_block), blocks, ts)
                current_block = {}
    ts = datetime.strptime(row['timestamp'].split('.')[0], '%Y-%m-%dD%H:%M:%S')
    add_block(dict(current_block), blocks, ts)

# parse input file and save to CSV as renko blocks
def parse_file(file):
    blocks = []
    print(f'Loading {file}')
    df = pd.read_csv(f'{os.getcwd()}/data/{file}')
    df = df.loc[df['symbol'] == 'XBTUSD']
    build_blocks(df, blocks)
    pd.DataFrame(blocks).to_csv(f'data/renko/{file}', index=False)

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
