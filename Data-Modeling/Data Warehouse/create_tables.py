import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    print('Dropping tables..')
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print('Error executing DROP query')
            print(e)
            print(query)


def create_tables(cur, conn):
    print('Creating tables..')
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print('Error executing CREATE query')
            print(e)
            print(query)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()