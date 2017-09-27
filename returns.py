'''
Price handler for reporting and updating the database of prices
'''
import pandas
from database import Database, DATE, REAL_RETURN, COLUMN_LOCATION, TOTAL_RETURN
from rnl_util import Logged

START_DATE = "2015"
GOLD = "WGC/GOLD_DAILY_USD"
SPDR_GLD_ETF = "GLD"
COUNTRY = "country"
# Central bank rates
FED = "FRED/DFF"
BOE = "BOE/IUDBEDR"
ECB = "BUNDESBANK/BBK01_SU0202"
# Real Return
COMPARE_REAL_RETURNS = "Comparison of Real Returns on 10 Year Bonds"
US_REAL_RETURN = "US Real Return"
UK_REAL_RETURN = "UK Real Return"
EUR_REAL_RETURN = "EUR Real Return"
JPN_REAL_RETURN = "Japan Real Return"
# 10 Year bonds
TEN_YEAR_US = "FRED/DGS10"
TEN_YEAR_UK = "BOE/IUDMNPY"
TEN_YEAR_EUR = "ECB/FM_M_U2_EUR_4F_BB_U2_10Y_YLD"
TEN_YEAR_JPN = "MOFJ/INTEREST_RATE_JAPAN_10Y"
# Inflation based on consumer price index
CPI_US = "RATEINF/CPI_USA"
CPI_UK = "RATEINF/CPI_GBR"
CPI_EUR = "RATEINF/CPI_EUR"
CPI_JPN = "RATEINF/CPI_JPN"
# Major share indices and All Country World Index (ACWI)
INDEX_UK = "^FTAS"
INDEX_JAPAN = "^N225"
INDEX_GERMANY = "^GDAXI"
INDEX_US = "^GSPC"
ACWI = "ACWI"
# List of symbols to resample and chart
INDICES_TITLE = "Total return for Gold compared to Major Indices"
INDICES = [INDEX_GERMANY, INDEX_JAPAN, INDEX_UK, INDEX_US, ACWI, GOLD]
TEN_YEAR_TITLE = "Total Return for 10 Year Bonds"
TEN_YEAR = [TEN_YEAR_UK, TEN_YEAR_US]
UK_RETURN_TITLE = "Total Return for UK Share, Bond and Gold"
UK_RETURN = [INDEX_UK, CPI_UK, GOLD]
CPI_TITLE = "Total Return for Inflation in US, UK, Japan and EU"
CPI = [CPI_US, CPI_UK, CPI_JPN, CPI_EUR]


class Returns(metaclass=Logged):
    '''Price'''
    def __init__(self):
        '''Init'''
        self._data = Database()
        self._log = Logged.logger(__name__)

    def real(self, country=US_REAL_RETURN, bond=TEN_YEAR_US, cpi=CPI_US,
             start_date=START_DATE):
        '''Real returns'''
        result = self._data.real_return(bond, cpi, start_date)
        result = result[[DATE, REAL_RETURN]]
        result.insert(COLUMN_LOCATION, COUNTRY, country)
        self._log.info(self._data.chart(country, result))
        return result

    def compare_real_returns(self, start_date=START_DATE):
        '''Compare'''
        # US real return
        us_rtn = self.real(US_REAL_RETURN, TEN_YEAR_US, CPI_US, start_date)
        # UK real return
        uk_rtn = self.real(UK_REAL_RETURN, TEN_YEAR_UK, CPI_UK, start_date)
        # EUR real return
        eur_rtn = self.real(EUR_REAL_RETURN, TEN_YEAR_EUR, CPI_EUR, start_date)
        # Japan real return
        jpn_rtn = self.real(JPN_REAL_RETURN, TEN_YEAR_JPN, CPI_JPN, start_date)

        compared = pandas.concat([us_rtn, uk_rtn, eur_rtn, jpn_rtn])
        pivot = compared.pivot(index=DATE, columns=COUNTRY,
                               values=REAL_RETURN)
        pivot.reset_index(inplace=True)
        self._log.info(self._data.chart(COMPARE_REAL_RETURNS, pivot))
        return pivot

    def country_assets(self):
        '''Country'''
        us_real_return = self.real(US_REAL_RETURN, TEN_YEAR_US, CPI_US,
                                   START_DATE)
        us_share_return = self._data.resample([INDEX_US], START_DATE)
        real_rate = us_real_return[[DATE, REAL_RETURN]]
        share = us_share_return[[DATE, TOTAL_RETURN]]
        result = self._data.concatenate(real_rate, share)
        self._log.info(self._data.chart("Country Assets", result))
        return result
