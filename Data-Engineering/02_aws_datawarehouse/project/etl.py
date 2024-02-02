import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, table_success_queries, load_staging_success_queries
import pandas as pd

def load_staging_tables(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection) -> None:
    """loading s3 jason data into staging table in postgres

    Args:
        cur (psycopg2.extensions.cursor): a database object stored in temp memory and used to work with datasets
        conn (psycopg2.extensions.connection): Connection of postgres database
    """    
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


def insert_tables(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection) -> None:
    """loading s3 jason data into staging table in postgres

    Args:
        cur (psycopg2.extensions.cursor): a database object stored in temp memory and used to work with datasets
        conn (psycopg2.extensions.connection): Connection of postgres database
    """   
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

def main(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection) -> None:
    """Run the ETL process

    Args:
        cur (psycopg2.extensions.cursor): a database object stored in temp memory and used to work with datasets
        conn (psycopg2.extensions.connection): Connection of postgres database
    """   
    print("ETL process are being performed")
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    print("ETL process are completed")

if __name__ == "__main__":
    main()