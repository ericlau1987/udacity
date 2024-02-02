import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, schemas_drop_queries, schemas_create_queries


def drop_schemas(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection) -> None:
    """drop schemas if exists in database

    Args:
        cur (psycopg2.extensions.cursor): a database object stored in temp memory and used to work with datasets
        conn (psycopg2.extensions.connection): Connection of postgres database
    """    
    for query in schemas_drop_queries:
        cur.execute(query)
        conn.commit()


def create_schemas(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection) -> None:
    """create schemas if exists in database

    Args:
        cur (psycopg2.extensions.cursor): a database object stored in temp memory and used to work with datasets
        conn (psycopg2.extensions.connection): Connection of postgres database
    """    
    for query in schemas_create_queries:
        cur.execute(query)
        conn.commit()


def main(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection) -> None:
    """Run drop schema and create schema

    Args:
        cur (psycopg2.extensions.cursor): a database object stored in temp memory and used to work with datasets
        conn (psycopg2.extensions.connection): Connection of postgres database
    """   
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

if __name__ == "__main__":
    main()