import requests
import gzip
import os
import sys
import multiprocessing
import shutil
from datetime import datetime, timedelta

# read to and from date from command line args
if len(sys.argv) < 3:
    print('Error! You must provide to_date and from_date as command line arguments.')
    exit(-1)

# create data dir if doesn't exist
if not os.path.exists('data'):
    os.makedirs('data')

# parse dates from command line
from_date = datetime.strptime(sys.argv[1], '%Y%m%d')
to_date = datetime.strptime(sys.argv[2], '%Y%m%d')

def download_data(date):
    # download file from Bitmex
    url = f'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/{date}.csv.gz'
    print(f'Downloading from {url}')
    response = requests.get(url, allow_redirects=True)
    output_file = f'data/{date}.csv.gz'
    open(output_file, 'wb').write(response.content)
    # decompress file
    with gzip.open(output_file, 'rb') as f_in:
        with open(output_file.replace('.gz', ''), 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    # delete gz file
    os.remove(output_file)

results = []

# create a pool to process days in parallel
procs = multiprocessing.cpu_count()
print(f'Running {procs} days in parallel')
pool = multiprocessing.Pool(processes=procs)

# loop until date range is done
while from_date <= to_date:
    date = from_date.strftime('%Y%m%d')
    async_func = pool.apply_async(download_data, args=(date,))
    results.append(async_func)
    from_date = from_date + timedelta(days=1)

# block until all files are finished
for result in results:
    result.get()
