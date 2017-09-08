'''hello world'''
from prices import Prices

PROVIDER_CSV = "provider.csv"
YAHOO_DATA_PROVIDER = "yahoo"


def main():
    '''Doc'''
    price = Prices()
    # price.report()
    price.update_symbol(YAHOO_DATA_PROVIDER, "^FTAS")


if __name__ == '__main__':
    main()
