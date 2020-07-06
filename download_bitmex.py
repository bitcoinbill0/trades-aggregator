import requests
import gzip
import os
import sys
import shutil
from datetime import datetime, timedelta
if len(sys.argv) < 3:
    print('Error! You must provide to_date and from_date as command line arguments.')
    exit(-1)
if not os.path.exists('data'):
    os.makedirs('data')
from_date = datetime.strptime(sys.argv[1], '%Y%m%d')
to_date = datetime.strptime(sys.argv[2], '%Y%m%d')
while from_date <= to_date: 
    date = from_date.strftime('%Y%m%d')
    url = f'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/{date}.csv.gz'
    print(f'Downloading from {url}')
    response = requests.get(url, allow_redirects=True)
    output_file = f'data/{date}.csv.gz'
    open(output_file, 'wb').write(response.content) 
    with gzip.open(output_file, 'rb') as f_in:
        with open(output_file.replace('.gz', ''), 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(output_file)
    from_date = from_date + timedelta(days=1)
