# Global Financial Data API via Python
Get Long-History Prices/Fundamentals from a list of tickers, 
managing authentication and (frequent) error-cases

### API HOME
https://api.globalfinancialdata.com/

### KEY SCRIPTS
1. `config.py`: 
    - input with credentials for API
    - requires valid `config.username` and `config.password`
2. `gfdapi.py`:
    - module with login functionality (i.e. call `gfdapi.gfd_auth()` from other scripts)
3. `pricing_downloader.py`: 
    - downloads **prices** to output directory of your choice
    - current output dir: `./data/series/`
    - current input data: `uk_stocks_pre_1963.csv`
4. `ratios_downloader.py`:
    - downloads **fundamentals** to output directory of your choice
    - current output dir: `./data/ratios_quarterly/`
    - current input data: `uk_stocks_pre_1963.csv`
5. `build_output.py`: 
    - collect and concat outputs from other scripts
    - perform calculations like the correct div yield
    - dataframe to matrix transformation for downstream usage