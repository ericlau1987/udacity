import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection) -> None:
    """drop table if exists in database

    Args:
        cur (psycopg2.extensions.cursor): a database object stored in temp memory and used to work with datasets
        conn (psycopg2.extensions.connection): Connection of postgres database
    """    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection) -> None:
    """create table if exists in database

    Args:
        cur (psycopg2.extensions.cursor): a database object stored in temp memory and used to work with datasets
        conn (psycopg2.extensions.connection): Connection of postgres database
    """    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection) -> None:
    """drop and create table in database

    Args:
        cur (psycopg2.extensions.cursor): a database object stored in temp memory and used to work with datasets
        conn (psycopg2.extensions.connection): Connection of postgres database
    """   
    try:
        print("tables are being dropped")
        drop_tables(cur, conn)
        print("tables have been dropped")
        
    except Exception as e:
        print(e)

    try:
        print("tables are being created")
        create_tables(cur, conn)
        print("tables have been created")
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()