import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, table_success_queries, load_staging_success_queries
import pandas as pd

def load_staging_tables(cur, conn):
    for query, success_query in zip(copy_table_queries, load_staging_success_queries):
        try:
            cur.execute(query)
            conn.commit()

        except Exception as e:
            print(f"{query} got errors")
            print(e)

        try:            
            print(success_query)
            cur.execute(success_query)
            print(cur.fetchone()[0])

        except Exception as e:
            print(f"{success_query} got errors")
            print(e)


def insert_tables(cur, conn):
    for query, success_query in zip(insert_table_queries, table_success_queries):
        try:
            cur.execute(query)
            conn.commit()

        except Exception as e:
            print(f"{query} got errors")
            print(e)

        try:
            print(success_query)
            cur.execute(success_query)
            print(cur.fetchone()[0])

        except Exception as e:
            print(f"{success_query} got errors")
            print(e)

def main(cur, conn):
    print("ETL process are being performed")
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    print("ETL process are completed")

if __name__ == "__main__":
    main()