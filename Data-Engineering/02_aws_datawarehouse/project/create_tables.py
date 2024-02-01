import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """The function is to drop table if exist

    Args:
        cur (str): cursor from connection
        conn (str): connection to database
    """    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create table if not exist

    Args:
        cur (str): cursor from connection
        conn (str): connection to database
    """  
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main(cur, conn):
   
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