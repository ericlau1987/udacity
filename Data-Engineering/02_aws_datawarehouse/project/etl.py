import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            # print(f'{query} is finished')
        except Exception as e:
            print(f"{query} got errors")
            print(e)


def insert_tables(cur, conn):
    for query in insert_table_queries:
        try:
            print(f'{query} is starting')
            cur.execute(query)
            conn.commit()
            # print(f'{query} is finished')
        except Exception as e:
            print(f"{query} got errors")
            print(e)


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