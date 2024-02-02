import cluster
import configparser
import pandas as pd
import psycopg2


config = configparser.ConfigParser()
config.read('dwh.cfg')

# HOST = config['CLUSTER']['HOST']
DB_NAME = config['CLUSTER']['DB_NAME']
DB_USER = config['CLUSTER']['DB_USER']
DB_PASSWORD = config['CLUSTER']['DB_PASSWORD']
DB_PORT  = config['CLUSTER']['DB_PORT']

cluster.create_IAM_role()
arn = cluster.get_IAM_role()

cluster.create_cluster(arn)
ENDPOINT = cluster.get_cluster_name()

connection = f"host={ENDPOINT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} port={DB_PORT}"

print(f'roleArn is {arn}')
print(f'endpoint is {ENDPOINT}')
print(connection)
conn = psycopg2.connect(connection)
cur = conn.cursor()

def create_redshift_schemas(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection) -> None:
    """create schema if exists in database

    Args:
        cur (psycopg2.extensions.cursor): a database object stored in temp memory and used to work with datasets
        conn (psycopg2.extensions.connection): Connection of postgres database
    """   
    import create_schemas 
    create_schemas.main(cur, conn)

def create_redshift_tables(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection) -> None:
    """create table if exists in database

    Args:
        cur (psycopg2.extensions.cursor): a database object stored in temp memory and used to work with datasets
        conn (psycopg2.extensions.connection): Connection of postgres database
    """   
    import create_tables
    create_tables.main(cur, conn)

def perform_etl(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection) -> None:
    """perform etl if exists in database

    Args:
        cur (psycopg2.extensions.cursor): a database object stored in temp memory and used to work with datasets
        conn (psycopg2.extensions.connection): Connection of postgres database
    """   
    import etl
    etl.main(cur, conn)

create_redshift_schemas(cur, conn)
create_redshift_tables(cur, conn)
perform_etl(cur, conn)

conn.close()

print('IAM role is being removed')
cluster.delete_IAM_role()
print('IAM role is removed')
print('Cluster is being removed')
cluster.delete_cluster()
print('Cluster is removed')