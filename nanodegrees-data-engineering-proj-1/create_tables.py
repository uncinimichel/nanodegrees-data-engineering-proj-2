import psycopg2

from sql_queries import create_table_queries, drop_table_queries


def create_database():
    '''
    create a Postgres database called sparkifydb and return the connection and the cursor
    :return:
    con: postgress connection
    cur: postgress cursor
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    conn.close()

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    '''
    Drop all the tables of the sparkifydb postgress database
    :param cur: postgress connection
    :param conn: postgress cursor
    :return: None
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Create all the tables of the sparkifydb postgress database
    :param cur: postgress connection
    :param conn: postgress cursor
    :return: None
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
