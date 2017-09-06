'''
Prices handler for reporting and updating the database of prices
Database handler using SQLAlchemy and SQLite3 for simplicity. Database holds
price information from primarily yahoo and quandl. Pandas is used to get data
Various information sources format the data differently - format is shown in
Provider table ("source"). All data gathered is written based on date,
symbol and price (close) as no analysis requires bars (OHLC) or volume
'''
import pandas
import pandas_datareader.data as web
import quandl
from sqlalchemy import create_engine
from rnl_util import Logged

# Database
SQLALCHEMY = "SQLAlchemy {}"
SQLALCHEMY_DB = "sqlite:///test_db.db"
SELECT_PRICE = "select * from price"
SELECT_QUANDL = "select symbol from provider where host='quandl'"
SELECT_YAHOO = "select symbol from provider where host='yahoo'"
SELECT_SYMBOL = "select symbol from provider"
SELECT_DATE = "select date from price where symbol='{}'"
SELECT_PROVIDER = "select distinct host from provider"
DEFAULT_TO_APPEND = "append"
DB_PRICE_TABLE = "price"

# Price providers
YAHOO_DATA_PROVIDER = "yahoo"
QUANDL_DATA_PROVIDER = "quandl"

# Pandas DataFrame
# Map takes the various column headers from Quandl or Yahoo and translates them
COLUMN_MAP = {"Date": "date",
              "Value": "price",
              "Percent per annum": "price",
              "Settle": "price",
              "Close": "price"}
COPY_COLUMN = ["date", "symbol", "price"]
DATE_COLUMN = "date"
SYMBOL_COLUMN = "symbol"
HOST_COLUMN = "host"
COLUMN_LOCATION = 0
TODAY = "today"
ONE_DAY_OFFSET = "1 days"
SLICE_DATE = 10
START = 0
OFFSET_ZERO_START = 1


class Database(metaclass=Logged):
    '''Database created with automatic logging from metaclass'''
    def __init__(self):
        self.engine = create_engine(SQLALCHEMY_DB)

    def __str__(self):
        return SQLALCHEMY.format(self.engine)

    def get(self, query):
        '''Get data from the database using Pandas SQL query'''
        return pandas.read_sql(query, self.engine)

    def set(self, table, df_data, update=DEFAULT_TO_APPEND, idx=False):
        '''Update database table with Pandas dataframe. Default append and
           index is not included'''
        return df_data.to_sql(table, self.engine, if_exists=update, index=idx)

    def get_provider(self):
        '''Get distinct host data providers. Only 2 as of Sep-17'''
        return self.get(SELECT_PROVIDER)

    def symbols(self, data_provider):
        '''Get symbols associated with the data provider. Each provider has
           their own symbol definition'''
        if data_provider == QUANDL_DATA_PROVIDER:
            result = self.get(SELECT_QUANDL)
        else:
            result = self.get(SELECT_YAHOO)
        return result

    def get_next_day(self, symbol):
        '''Utility function to determine date in database for any symbol and
           then consequently next date to collect data to avoid duplicates'''
        result = self.get(SELECT_DATE.format(symbol))
        next_day = pandas.to_datetime(result[DATE_COLUMN].max())
        if pandas.isnull(next_day):
            next_day = None
        else:
            # Do not increase date beyond today
            if next_day < pandas.to_datetime(TODAY):
                next_day += pandas.Timedelta(ONE_DAY_OFFSET)
        return next_day


class Host(metaclass=Logged):
    '''Functions to get data using Pandas from Quandl, Yahoo or CSV files'''
    @staticmethod
    def get_quandl(symbol, start_date=None):
        '''Get web hosted data from Quandl'''
        return quandl.get(symbol, start_date=start_date)

    @staticmethod
    def get_yahoo(symbol, start_date=None):
        '''Get web hosted data from Yahoo'''
        return web.DataReader(symbol, YAHOO_DATA_PROVIDER, start_date)

    @staticmethod
    def get_csv(filename):
        '''Read CSV file'''
        return pandas.read_csv(filename)

    @staticmethod
    def copy_columns(dataframe, symbol):
        '''Standardise data from provider to copy it to the database. Data to
           be copied is date, symbol and price'''
        dataframe.reset_index(inplace=True)
        dataframe.rename(columns=COLUMN_MAP, inplace=True)
        dataframe.insert(COLUMN_LOCATION, SYMBOL_COLUMN, symbol)
        return dataframe[COPY_COLUMN].copy()

    def get_latest_data(self, host, symbol, start_date):
        '''Use a start_date to collect data. If None then it gets all available
           data if a date is given it will collect price data from that date'''
        if host == QUANDL_DATA_PROVIDER:
            result = self.get_quandl(symbol, start_date)
        elif host == YAHOO_DATA_PROVIDER:
            result = self.get_yahoo(symbol, start_date)
        else:
            print("Unexpected call to host: {}".format(host))
        return self.copy_columns(result, symbol)


class Prices(metaclass=Logged):
    '''Prices has two main methods - report data in database and update data'''
    def update_data(self):
        '''Get latest data and write it to the database by iterating through
           the data providers (yahoo and quandl) and then the symbols for each
           data provider'''
        data = Database()
        providers = data.get_provider()
        for i, row in providers.iterrows():
            print("Host #{}: get {} symbols".format(i + 1, row[HOST_COLUMN]))
            self._get_latest(data, row[HOST_COLUMN])

    @staticmethod
    def _get_latest(database, data_provider):
        '''Internal method to get latest data from host data providers and
           write it to the database'''
        host = Host()
        symbols = database.symbols(data_provider)
        for index, value in symbols.iterrows():
            symbol = value[SYMBOL_COLUMN]
            next_day = database.get_next_day(symbol)
            # Check if already have the latest data. Ideally data is collected
            # at weekends to avoid any duplication or incomplete data
            if next_day < pandas.to_datetime(TODAY):
                print("Index: {}".format(index),
                      "Host: {}".format(data_provider),
                      "Symbol: {}".format(symbol),
                      "Next day: {}".format(next_day))

                result = host.get_latest_data(data_provider, symbol, next_day)
                if result.empty:
                    print("Latest dataset empty for {}".format(symbol))
                else:
                    database.set(DB_PRICE_TABLE, result)

    @staticmethod
    def report():
        '''Get latest summary data from database. Useful to run it just prior
           to an update to see changes or when the last data update happened
           Changes are appended by default so results of latest changes can be
           seen based on most recent update'''
        data = Database()
        symbols = data.get(SELECT_SYMBOL)
        for index, value in symbols.iterrows():
            latest = data.get(SELECT_DATE.format(value.symbol))
            last_day = latest[DATE_COLUMN].max()[START:SLICE_DATE]
            count = latest.index.max() + OFFSET_ZERO_START
            symbol = value[SYMBOL_COLUMN]

            print("Index: {}".format(index), "Symbol: {}".format(symbol),
                  "Last date: {}".format(last_day), "Count: {}".format(count))
