'''
Price handler for reporting and updating the database of prices
'''
from database import Database
from rnl_util import Logged


class Price(metaclass=Logged):
    '''Price'''
    def __init__(self):
        '''Init'''
        self._data = Database()

    def symbol(self, symbol, start_date=None, period="M"):
        '''Symbol'''
        result = self._data.aggregate_mean(symbol, start_date, period)
        print(result)

    def symbols(self):
        '''Symbols'''
        print("Symbols - {}".format(self._data))


def main():
    '''Main'''
    print("Price Main")
    prc = Price()
    prc.symbol(["GLD"], "2016")
    prc.symbols()


if __name__ == '__main__':
    main()
