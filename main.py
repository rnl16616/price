'''Main'''
# from database import Database
from returns import Returns


def main():
    '''Main'''

    # Collect data (last = 24-Sep-17)
    # data = Database()
    # data.report()
    # data.update_all_symbols()
    # print(data.comparators())

    # Collect real return data
    rtn = Returns()
    result = rtn.compare_real_returns(start_date="2010")
    # print(rtn.country_assets())
    print(result)

if __name__ == '__main__':
    main()
