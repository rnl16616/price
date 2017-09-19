'''
Database handler using SQLAlchemy and SQLite3 for simplicity. Database holds
price information from primarily yahoo and quandl. Pandas is used to get data
Various information sources format the data differently - format is shown in
Provider table ("source"). All data gathered is written based on date,
symbol and price (close) as no analysis requires bars (OHLC) or volume
'''
import pandas
from pandas.tseries.offsets import BDay
import pandas_datareader.data as web


import quandl
from sqlalchemy import create_engine
from rnl_util import Logged

# Database
SQLALCHEMY = "SQLAlchemy {}"
SQLALCHEMY_DB = "sqlite:///prices.db"
SELECT_PRICE = "select * from price"
SELECT_QUANDL = "select symbol from provider where host='quandl'"
SELECT_YAHOO = "select symbol from provider where host='yahoo'"
SELECT_SYMBOL = "select symbol from provider"
SELECT_COMPARATORS = "select distinct comparison from provider"
SELECT_COMPARISON = "select symbol from provider where comparison='{}'"
SELECT_DATE = "select date from price where symbol='{}'"
SELECT_PRICE_WHERE = "select * from price where symbol='{}'"
SELECT_PROVIDER = "select distinct host from provider"
DEFAULT_TO_APPEND = "append"
UPDATE_MODE_REPLACE = "replace"
DB_PRICE_TABLE = "price"
DB_PROVIDER_TABLE = "provider"

# Price providers
PROVIDER_CSV = "provider.csv"
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
COMPARISON_COLUMN = "comparison"
PRICE_COLUMN = "price"
COLUMN_LOCATION = 0
TODAY = "today"
NEXT_BUSINESS_DAY = 1
SLICE_DATE = 10
START = 0
TUPLE_VALUES = 1
OFFSET_ZERO_START = 1
MONTH = "M"
YEAR = "A"

# Chart
SAVE_LOCATION = "c:\\temp\\"

# Comparison using Provider
GOLD = "Gold"
SHARE_INDEX = "ShareIndex"
WORLD_INDEX = "WorldIndex"
COMMODITY_INDEX = "CommodityIndex"
CENTRAL_BANK = "CentralBank"
TEN_YEAR_RATE = "10Year"
DOLLAR_INDEX = "DollarIndex"
CURRENCY = "Currency"
INFLATION = "Inflation"
STRIP_OR_CLAUSE = -4


class Host(metaclass=Logged):
    '''Functions to get data using Pandas from Quandl, Yahoo or CSV files.
       Symbols are held in the provider table and are unique to each host'''
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
        '''Read CSV file, assumed to be for provider to replace existing table
           with updated entries'''
        return pandas.read_csv(filename)


class Database(metaclass=Logged):
    '''Database created with automatic logging from metaclass. Database class
       manages all database operations including getting updated data from the
       host data providers. Public methods:
           replace_provider - redefine table entries, primarily addition
           get_host_data - get data in defined format (date, symbol, price)
           get_data - get all database data for symbol(s) from specified date
           update_all_symbols - update database with latest host data
           update_symbol - update individual symbol in database
           report - list symbols and simple count of price points
           comparators - chart similar (inflation, 10Year...) symbols
           chart - write chart to PNG file'''
    def __init__(self, database=SQLALCHEMY_DB):
        '''Initialise database and prepare to get host data'''
        self._engine = create_engine(database)
        self._host = Host()

    def __str__(self):
        '''String representation of active database connection'''
        return SQLALCHEMY.format(self._engine)

    def _get(self, query):
        '''Get data from the database using Pandas SQL query'''
        return pandas.read_sql(query, self._engine)

    def _set(self, table, df_data, update=DEFAULT_TO_APPEND, idx=False):
        '''Update database table with Pandas dataframe. Default append and
           index is not included'''
        return df_data.to_sql(table, self._engine, if_exists=update, index=idx)

    def _get_next_day(self, symbol):
        '''Utility function to determine date in database for any symbol and
           then consequently next date to collect data to avoid duplicates.
           Duplicates will cause the "pivot" operation in comparator to fail'''
        result = self._get(SELECT_DATE.format(symbol))
        next_day = pandas.to_datetime(result[DATE_COLUMN].max())
        if pandas.isnull(next_day):
            next_day = None
        else:
            # Collect next business day even if some prices (currencies...) are
            # quoted 24x7
            next_day += BDay(NEXT_BUSINESS_DAY)
        return next_day

    def _get_provider(self):
        '''Get distinct host data providers. Only 2 as of Sep-17'''
        return self._get(SELECT_PROVIDER)

    def _get_host_symbols(self, data_provider):
        '''Get symbols associated with the data provider. Each provider has
           their own symbol definition'''
        if data_provider == QUANDL_DATA_PROVIDER:
            result = self._get(SELECT_QUANDL)
        else:
            result = self._get(SELECT_YAHOO)
        return result

    def _where_comparators(self, comparison_set):
        '''Select symbols for comparison'''
        query = SELECT_COMPARISON.format(comparison_set)
        result = self._get(query)
        where_clause = " where "
        for symbol in result[SYMBOL_COLUMN]:
            where_clause = where_clause + "symbol='{}'".format(symbol) + " or "
        where_clause = where_clause[:STRIP_OR_CLAUSE]
        return where_clause

    @staticmethod
    def _copy_columns(dataframe, symbol):
        '''Standardise data from provider to copy it to the database. Data to
           be copied is date, symbol and price. To keep method simple only
           mapping Close to price and not Adj Close as well. This would make
           logic more complex to test for Close and Adj Close in dataframe and
           difference is not very meaningful in current analyses'''
        dataframe.reset_index(inplace=True)
        dataframe.rename(columns=COLUMN_MAP, inplace=True)
        dataframe.insert(COLUMN_LOCATION, SYMBOL_COLUMN, symbol)

        # Copy only required dataframe columns and convert to short date (sd)
        # by slicing off zeroes in the timestamp section (2017-09-01 00:00:00)
        result = dataframe[COPY_COLUMN].copy()
        result[DATE_COLUMN] = pandas.Series([str(sd)[START:SLICE_DATE]
                                             for sd in result[DATE_COLUMN]])
        return result

    def _update_latest(self, data_provider):
        '''Internal method to get latest data from host data providers and
           write it to the database'''
        symbols = self._get_host_symbols(data_provider)
        for index, value in symbols.iterrows():
            symbol = value[SYMBOL_COLUMN]
            next_day = self._get_next_day(symbol)
            # Check if already have the latest data. Ideally data is collected
            # at weekends to avoid any duplication or incomplete data yahoo
            # seems to get previous business date (01-Sep-17) when requesting
            # a weekend date (02-Sep-17) so use next business day (04-Sep-17)
            if next_day < pandas.to_datetime(TODAY):
                print("Index: {}".format(index + OFFSET_ZERO_START),
                      "Host: {}".format(data_provider),
                      "Symbol: {}".format(symbol),
                      "Next day: {}".format(next_day))

                result = self.get_host_data(data_provider, symbol, next_day)
                if result.empty:
                    print("Latest dataset empty for {}".format(symbol))
                else:
                    self._set(DB_PRICE_TABLE, result)

    def replace_provider(self, filename=PROVIDER_CSV):
        '''Replace provider table with an updated version provider by user'''
        result = self._host.get_csv(filename)
        self._set(DB_PROVIDER_TABLE, result, update=UPDATE_MODE_REPLACE)
        print("Replaced provider with content from {}".format(filename))
        return result

    def get_host_data(self, host, symbol, start_date):
        '''Use a start_date to collect data. If None then it gets all available
           data if a date is given it will collect price data from that date'''
        if host == QUANDL_DATA_PROVIDER:
            result = self._host.get_quandl(symbol, start_date)
        elif host == YAHOO_DATA_PROVIDER:
            result = self._host.get_yahoo(symbol, start_date)
        else:
            print("Unexpected call to host: {}".format(host))
        # Data format will be different for each source and symbol so
        # standardise output based on three columns (date, symbol, price)
        return self._copy_columns(result, symbol)

    def get_data(self, symbols, start_date=None):
        '''Get symbol data from database and return a pandas dataframe with a
           simple concatenation of the results
             symbols - list of symbols to extract from database
             start_date - filter out earlier dates i.e. 2016 or 2013-05'''
        result = pandas.DataFrame
        for symbol in symbols:
            query = SELECT_PRICE_WHERE.format(symbol)
            dataframe = self._get(query)
            if result.empty:
                result = dataframe
            else:
                result = pandas.concat([result, dataframe])
        if start_date is not None:
            result = result[result[DATE_COLUMN] > start_date]

        return result

    def update_all_symbols(self):
        '''Get latest data and write it to the database by iterating through
           the data providers (yahoo and quandl) and then the symbols for each
           data provider'''
        providers = self._get_provider()
        for i, row in providers.iterrows():
            print("Host #{}: get {} symbols".format(i + OFFSET_ZERO_START,
                                                    row[HOST_COLUMN]))
            self._update_latest(row[HOST_COLUMN])

    def update_symbol(self, provider, symbol):
        '''Update symbol'''
        next_day = self._get_next_day(symbol)
        if next_day < pandas.to_datetime(TODAY):
            result = self.get_host_data(provider, symbol, next_day)
            self._set(DB_PRICE_TABLE, result)
            print("Updated {} from {}".format(symbol, next_day))
        else:
            print("Not updated {} - next date was {}".format(symbol, next_day))
        return result

    def report(self):
        '''Get latest summary data from database. Useful to run it just prior
           to an update to see changes or when the last data update happened
           Changes are appended by default so results of latest changes can be
           seen based on most recent update'''
        symbols = self._get(SELECT_SYMBOL)
        for index, value in symbols.iterrows():
            latest = self._get(SELECT_DATE.format(value.symbol))
            last_day = latest[DATE_COLUMN].max()[START:SLICE_DATE]
            count = latest.index.max() + OFFSET_ZERO_START
            symbol = value[SYMBOL_COLUMN]

            print("Index: {}".format(index), "Symbol: {}".format(symbol),
                  "Last date: {}".format(last_day), "Count: {}".format(count))

    def comparators(self):
        '''Comparators'''
        result = self._get(SELECT_COMPARATORS)
        for i in result.iterrows():
            # self.price_set(i[TUPLE_VALUES][START], i[TUPLE_VALUES][START])
            query = SELECT_PRICE + \
                    self._where_comparators(i[TUPLE_VALUES][START])
            prices = self._get(query)
            pivot_data = prices.pivot(index=DATE_COLUMN, columns=SYMBOL_COLUMN,
                                      values=PRICE_COLUMN)
            pivot_data = pivot_data.dropna()
            self.chart(i[TUPLE_VALUES][START], pivot_data)

    @staticmethod
    def chart(title, chart_data):
        '''Chart'''
        charts = chart_data.plot(title=title, figsize=(16, 10)).get_figure()
        # today = str(pandas.to_datetime(TODAY))[START:SLICE_DATE]
        today = str(pandas.to_datetime(TODAY).date())
        filename = SAVE_LOCATION + title + "_" + today + ".png"
        charts.savefig(filename)
        print("Saved chart to {}".format(filename))

    def aggregate_mean(self, symbol, start_date=None, period=MONTH):
        '''Aggregate'''
        result = self.get_data(symbol, start_date)
        result[DATE_COLUMN] = pandas.to_datetime(result[DATE_COLUMN])
        result.set_index([DATE_COLUMN], inplace=True)

        aggregated = pandas.DataFrame()
        aggregated[PRICE_COLUMN] = result[PRICE_COLUMN].resample(period).mean()
        return aggregated