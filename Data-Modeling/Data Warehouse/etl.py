import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    print('Running COPY queries..')
    print(f'COPY queries: {len(copy_table_queries)}')
    for query in copy_table_queries:
        print('-- COPY query:')
        print(f'{query}')
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print('Error executing COPY query')
            print(e)
            print(query)
            print('Exiting..')
            break


def insert_tables(cur, conn):
    print('Running INSERT queries..')
    print(f'INSERT queries: {len(insert_table_queries)}')
    for query in insert_table_queries:
        print('-- INSERT query:')
        print(f'{query}')
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print('Error executing INSERT query')
            print(e)
            print(query)
            print('Exiting..')
            break


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)

    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()