import gfdapi
import os
import requests
import pandas as pd
import multiprocessing as mp

tickers = pd.read_csv('uk_stocks_pre_1963.csv')['Symbol'].to_list()
output_path = './data/ratios_quarterly/'
overwrite = False

fixed_parameters = {'startdate': '12/31/1799',
                    'endate': '12/31/1985',
                    'period': 'Quarterly',
                    'group': 'ratios'}


def download_ratios(ticker):
    print(f'{ticker} - download ratios')

    url = 'https://api.globalfinancialdata.com/fundamentals/'
    parameters = {'token': os.environ['GFD_API_TOKEN'],
                  'seriesname': ticker}

    parameters.update(fixed_parameters)
    r = requests.post(url, data=parameters)
    if r.status_code == 200:
        r_dict = r.json()

        try:
            ratios = pd.DataFrame(r_dict['ratios'])
            ratios.to_csv(f'{output_path}/{ticker}.csv')
        except BaseException as e:
            print(f'*** error downloading ratios for {ticker} ***')
            print(e)
    else:
        print(f'*** response error: {r.status_code} ***')


if __name__ == '__main__':

    # authentication
    gfdapi.gfd_auth()

    processed_tickers = [f.replace('.csv', '') for f in os.listdir(output_path)]

    if not overwrite:
        tickers_to_process = list(set(tickers) - set(processed_tickers))

    pool = mp.Pool(processes=mp.cpu_count())
    pool.map(download_ratios, tickers_to_process)
