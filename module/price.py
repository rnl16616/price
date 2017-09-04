'''hello world'''
from database import Prices

YAHOO_DATA_PROVIDER = "yahoo"
QUANDL_DATA_PROVIDER = "quandl"


def main():
    '''Doc'''
    print("Hello World")
    price = Prices()
    price.report()


if __name__ == '__main__':
    main()
