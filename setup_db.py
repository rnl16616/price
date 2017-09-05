'''
Setup database using SQLite3 which is deprecated by pandas, which is
using SQLAlchemy. Since database creation is one-off seems unimportant
'''
import sqlite3


DATABASE_NAME = "test_db.db"
CREATE_PRICE = '''CREATE TABLE price (symbol text, date text, price real)'''
CREATE_PROVIDER = '''CREATE TABLE provider
                        (symbol text, description text, source text,
                        host text)'''


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
    execute(CREATE_PRICE)
    execute(CREATE_PROVIDER)
    print('Created database tables (price and provider): ' + DATABASE_NAME)


if __name__ == '__main__':
    create_db()
