import pandas as pd
from pandas.tseries.offsets import MonthEnd
import os


pd.set_option('max_columns', 100)
pd.set_option('max_rows', 5000)

# ------------------------------------------------------------------
# Data collection, concat, and cleanup
# ------------------------------------------------------------------

def collect_and_concat(data_dir):
    output_list = []
    for f in os.listdir(data_dir):
        output_list.append(pd.read_csv(os.path.join(data_dir, f)))
    output_df = pd.concat(output_list)
    return output_df


# collect ticker info
ticker_info = collect_and_concat(data_dir='./data/series_splitadj/ticker_info/')

# collect series (returns)
series = collect_and_concat(data_dir='./data/series_splitadj/returns/')
series_w_info = series.merge(ticker_info, how='inner', on='series_id')
series_w_info['date'] = pd.to_datetime(series_w_info['date']).dt.strftime('%Y-%m-%d')
totret_matrix = series_w_info.pivot(index='symbol', columns='date', values='total_return')
totret_matrix.to_csv('./matrix_outputs/totret_matrix.csv')

# collect dividend yield
# using not split adjusted because X or Y
divs = collect_and_concat(data_dir='./data/series/splits_and_divs/')
divs[divs['action'].str.lower().str.contains('dividend')].groupby('action').size().sort_values(ascending=False) / \
    sum(divs[divs['action'].str.lower().str.contains('dividend')].groupby('action').size())

divs = divs.loc[divs['action'] == 'Cash Dividend', ['ticker', 'amount', 'ex-dividend date']]

# inconsistent formatting requires this
divs['amount'] = divs['amount'].astype(str).str.replace('..', '.', regex=False)
divs['amount'] = divs['amount'].astype(str).str.replace('\.', '.', regex=False)
divs['amount'] = divs['amount'].astype(float)

# get prices
ticker_info_pr = collect_and_concat(data_dir='./data/series/ticker_info/')
prices = collect_and_concat(data_dir='./data/series/returns/')
prices_w_info = prices.merge(ticker_info_pr, how='inner', on='series_id')
prices_w_info['date'] = pd.to_datetime(prices_w_info['date'])
prices_df = prices_w_info[['symbol', 'date', 'close']].sort_values(by=['symbol', 'date'], ascending=True).set_index(
    'date')


# ------------------------------------------------------------------
# Calculating Div-Yield
# ------------------------------------------------------------------
dy_list = []

# for every id:
ids = prices_df['symbol'].unique()
for i, id in enumerate(ids):
    print(i)

    # subset of prices for a given id
    id_prices = prices_df[prices_df['symbol'] == id]

    #   fill in any missing months, keeping prior price (fill-forward)
    id_prices = id_prices.asfreq('M', method='ffill').reset_index()

    #   get divs for same id, and covert div dates to eom
    id_divs = divs[divs['ticker'] == id]
    id_divs['date'] = pd.to_datetime(id_divs['ex-dividend date']) + MonthEnd(0)

    # sum multiple divs within the same month into one value
    id_divs = id_divs.groupby(['ticker', 'date'])['amount'].sum().reset_index()

    #  merge w/ prices and fill 0s
    id_df = id_prices.merge(id_divs, how='left', left_on=['symbol', 'date'], right_on=['ticker', 'date'], )
    id_df['amount'] = id_df['amount'].fillna(0)

    #  calc DY: rolling sum last 12 mths of divs and divide by current price
    id_df['amount_ann'] = id_df['amount'].rolling(12, min_periods=1).sum()
    id_df['dy'] = 100*(id_df['amount_ann'] / id_df['close'])

    dy_list.append(id_df)

dy_df = pd.concat(dy_list)

# ------------------------------------------------------------------
# QA
# ------------------------------------------------------------------
# mikhail has outputs from their desktop application.
# he sent me those and i compared his outputs to mine
batch_dl = pd.read_csv('ms_batch_1.csv')
batch_dl['date'] = pd.to_datetime(batch_dl['date'])

test = dy_df.merge(batch_dl, how='inner', on=['date', 'symbol'])
test['diff'] = abs(test['dy'] - test['Dividend Yield'])
test.to_csv('dy_qa.csv',index=False)
#test = test.sort_values(by='diff', ascending=False)

# output to matrix
dy_df['date'] = dy_df['date'].dt.strftime('%Y-%m-%d')
# dy_df.groupby(['ticker','date']).size().sort_values(ascending=False).describe()
dy_matrix = dy_df.pivot_table(index='symbol', columns='date', values='dy')
dy_matrix.to_csv('./matrix_outputs/dy_matrix.csv')