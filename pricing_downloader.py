import gfdapi
import os
import requests
import pandas as pd
import multiprocessing as mp


tickers = pd.read_csv('uk_stocks_pre_1963.csv')['Symbol'].to_list()
output_path = './data/series/'
overwrite = False

fixed_parameters = {'startdate': '12/31/1799',
                    'endate': '12/31/1985',
                    'periodicity': 'Monthly',
                    'closeonly': False,
                    'splitadjusted': False,
                    'currency': 'GBP',
                    'totalreturn': True,
                    'corporateactions': True,
                    'metadata': True}


def download_series(ticker):
    print(f'{ticker} - series')

    # pricing url
    url = 'https://api.globalfinancialdata.com/series'

    parameters = {'token': os.environ['GFD_API_TOKEN'],
                  'seriesname': ticker}
    parameters.update(fixed_parameters)

    # API call return body
    r = requests.post(url, data=parameters)

    if r.status_code == 200:

        r_dict = r.json()

        # extracts the price data and assigns it to a pandas dataframe and exports file to local directory
        try:
            return_data = pd.DataFrame(r_dict['price_data'])
            return_data.to_csv(f'{output_path}/returns/{ticker}.csv', index=False)
        except BaseException as e:
            print(f'*** error downloading price_data for {ticker} ***')
            print(e)

        try:
            splits_and_divs = pd.DataFrame(r_dict['Splits And Dividends'])
            splits_and_divs.to_csv(f'{output_path}/splits_and_divs/{ticker}.csv', index=False)
        except BaseException as e:
            print(f'*** error downloading splits and dividends for {ticker} ***')
            print(e)

        try:
            ticker_info = pd.DataFrame(r_dict['data_information'])
            ticker_info.to_csv(f'{output_path}/ticker_info/{ticker}.csv', index=False)
        except BaseException as e:
            print(f'*** error downloading data_information for {ticker} ***')
            print(e)

        try:
            dl_status = pd.DataFrame(r_dict['download_status'])
            dl_status.to_csv(f'{output_path}/dl_status/{ticker}.csv', index=False)
        except BaseException as e:
            print(f'*** error downloading download_status for {ticker} ***')
            print(e)
    else:
        print(f'*** response error: {r.status_code} ***')


if __name__ == '__main__':

    # authentication
    gfdapi.gfd_auth()

    processed_tickers = [f.replace('.csv', '') for f in os.listdir(os.path.join(output_path, 'returns'))]

    if not overwrite:
        tickers_to_process = list(set(tickers) - set(processed_tickers))

    pool = mp.Pool(processes=mp.cpu_count())
    pool.map(download_series, tickers_to_process)
