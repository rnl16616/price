'''hello world'''
from database import Prices

YAHOO_DATA_PROVIDER = "yahoo"
QUANDL_DATA_PROVIDER = "quandl"


def main():
    '''Doc'''
    price = Prices()
    price.report()
    price.get_latest(YAHOO_DATA_PROVIDER)
    price.get_latest(QUANDL_DATA_PROVIDER)


if __name__ == '__main__':
    main()
