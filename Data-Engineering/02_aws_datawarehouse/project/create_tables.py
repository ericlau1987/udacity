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


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    try:
        drop_tables(cur, conn)
    except Exception as e:
        print(e)
    try:
        create_tables(cur, conn)
    except Exception as e:
        print(e)

    conn.close()


if __name__ == "__main__":
    main()