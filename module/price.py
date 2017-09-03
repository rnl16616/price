'''hello world'''
from database import Database
from database import Prices

TICKER = "RATEINF/CPI_GBR"
DB_PRICE_TABLE = "price"
DB_PROVIDER_TABLE = "provider"


def main():
    '''Doc'''
    prc = Prices()
    result = prc.get_quandl(TICKER)
#    result = prc.get_yahoo(TICKER)
    copied = prc.copy_columns(result, TICKER)
    mydb = Database()
    mydb.set('price', copied)

if __name__ == '__main__':
    main()
