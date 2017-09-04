'''Database handler using SQLAlchemy and SQLite3 for simplicity'''
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
DEFAULT_TO_APPEND = "append"
DB_PRICE_TABLE = "price"

# Price
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
COLUMN_LOCATION = 0
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
        '''Get data from the database using SQL query'''
        return pandas.read_sql(query, self.engine)

    def set(self, table, df_data, update=DEFAULT_TO_APPEND, idx=False):
        '''Update database table with Pandas dataframe'''
        return df_data.to_sql(table, self.engine, if_exists=update, index=idx)

    def symbols(self, data_provider):
        '''Get symbol'''
        if data_provider == QUANDL_DATA_PROVIDER:
            result = pandas.read_sql(SELECT_QUANDL, self.engine)
        else:
            result = pandas.read_sql(SELECT_YAHOO, self.engine)
        return result


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
        '''Standardise data from provider to copy it to the database'''
        dataframe.reset_index(inplace=True)
        dataframe.rename(columns=COLUMN_MAP, inplace=True)
        dataframe.insert(COLUMN_LOCATION, SYMBOL_COLUMN, symbol)
        return dataframe[COPY_COLUMN].copy()


class Prices(metaclass=Logged):
    '''Prices'''
    @staticmethod
    def get_latest(data_provider):
        '''Get latest data from host data providers'''
        data = Database()
        host = Host()
        symbols = data.symbols(data_provider)
        for index, value in symbols.iterrows():
            latest = data.get(SELECT_DATE.format(value.symbol))
            next_day = pandas.to_datetime(latest[DATE_COLUMN].max())
            next_day += pandas.Timedelta(ONE_DAY_OFFSET)
            symbol = value[SYMBOL_COLUMN]

            print("Index: {}".format(index),
                  "Host: {}".format(data_provider),
                  "Value: {}".format(symbol),
                  "Next day: {}".format(next_day))

            if data_provider == QUANDL_DATA_PROVIDER:
                result = host.get_quandl(symbol, next_day)
            elif data_provider == YAHOO_DATA_PROVIDER:
                result = host.get_yahoo(symbol, next_day)
            else:
                print("Unexpected call to host: {}".format(data_provider))

            copied = host.copy_columns(result, symbol)
            data.set(DB_PRICE_TABLE, copied)

    @staticmethod
    def report():
        '''Get latest data from host data providers'''
        data = Database()
        symbols = data.get(SELECT_SYMBOL)
        for index, value in symbols.iterrows():
            latest = data.get(SELECT_DATE.format(value.symbol))
            last_day = latest[DATE_COLUMN].max()[START:SLICE_DATE]
            count = latest.index.max() + OFFSET_ZERO_START
            symbol = value[SYMBOL_COLUMN]

            print("Index: {}".format(index),
                  "Symbol: {}".format(symbol),
                  "Last date: {}".format(last_day),
                  "Count: {}".format(count))
