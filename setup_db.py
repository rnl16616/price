'''
Setup database using SQLite3 which is deprecated by pandas, which is
using SQLAlchemy. Since database creation is one-off seems unimportant
'''
import sqlite3
from prices import Host, Database


DATABASE_NAME = "test_db.db"
SQLALCHEMY_DB = "sqlite:///"
CREATE_PRICE = '''CREATE TABLE price (symbol text, date text, price real)'''
CREATE_PROVIDER = '''CREATE TABLE provider
                        (symbol text, description text, source text,
                        host text)'''
PROVIDER_TABLE = "provider"
PROVIDER_CSV = "provider.csv"


def execute(sql_query):
    '''Connect to a database to execute SQL'''
    conn = sqlite3.connect(DATABASE_NAME)
    cur = conn.cursor()
    result = cur.execute(sql_query)
    conn.commit()
    conn.close()
    return result


# Drive database creation and print result. If fail, use python generated error
def create_db():
    ''' Initial creation of empty tables'''
    print('Creating database tables (price and provider): ' + DATABASE_NAME)
    execute(CREATE_PRICE)
    execute(CREATE_PROVIDER)
    print('Successfully created database tables in: ' + DATABASE_NAME)


def add_provider(filename=PROVIDER_CSV):
    '''populate provider table with content of CSV file'''
    print("Updating Provider with CSV data from {}".format(filename))
    data = Database(SQLALCHEMY_DB + DATABASE_NAME)
    host = Host()
    result = host.get_csv(filename)
    data.set(PROVIDER_TABLE, result)
    print("Successfully updated Provider with content of {}".format(filename))


if __name__ == '__main__':
    create_db()
    add_provider()
