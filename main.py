'''hello world'''
from database import Prices


def main():
    '''Doc'''
    price = Prices()
    price.report()
    price.update_data()


if __name__ == '__main__':
    main()
