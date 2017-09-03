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
DEFAULT_TO_APPEND = "append"

# Price
YAHOO_DATA_PROVIDER = "yahoo"

# Pandas DataFrame
# Map takes the various column headers from Quandl or Yahoo and translates them
COLUMN_MAP = {"Date": "date",
              "Value": "price",
              "Percent per annum": "price",
              "Settle": "price",
              "Close": "price"}
COPY_COLUMN = ["date", "symbol", "price"]
ADD_SYMBOL = "symbol"
COLUMN_LOCATION = 0
ROW_INDEX = 0
COL_INDEX = 0


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


class Prices(metaclass=Logged):
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
        dataframe.insert(COLUMN_LOCATION, ADD_SYMBOL, symbol)
        return dataframe[COPY_COLUMN].copy()
