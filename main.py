'''Main'''
from database import Database


GOLD = "WGC/GOLD_DAILY_USD"
SPDR_GLD_ETF = "GLD"


def main():
    '''Main'''

    # Collect data (last = 16-Sep-17)

    # Report
    data = Database()
    # data.report()
    # data.update_symbol("yahoo", "^FTAS")
    # data.update_all_symbols()
    # data.comparators()
    data.get_data([GOLD, SPDR_GLD_ETF])

if __name__ == '__main__':
    main()
