import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, schemas_drop_queries, schemas_create_queries


def drop_schemas(cur, conn):
    """The function is to drop table if exist

    Args:
        cur (str): cursor from connection
        conn (str): connection to database
    """    
    for query in schemas_drop_queries:
        cur.execute(query)
        conn.commit()


def create_schemas(cur, conn):
    """Create table if not exist

    Args:
        cur (str): cursor from connection
        conn (str): connection to database
    """  
    for query in schemas_create_queries:
        cur.execute(query)
        conn.commit()


def main(cur, conn):
    # config = configparser.ConfigParser()
    # config.read('dwh.cfg')

    # conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    # cur = conn.cursor()
    cur, conn = cur, conn
    try:
        print("schemas are being dropped")
        drop_schemas(cur, conn)
        print("schemas have been dropped")
    except Exception as e:
        print(e)
    try:
        print("schemas are being created")
        create_schemas(cur, conn)
        print("schemas have been created")
    except Exception as e:
        print(e)

    # conn.close()


if __name__ == "__main__":
    main()