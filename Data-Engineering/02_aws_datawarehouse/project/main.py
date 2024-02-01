import create_tables 
import etl
import configparser
import pandas as pd
import psycopg2

config = configparser.ConfigParser()
config.read('dwh.cfg')

arn = config['IAM_ROLE']['ARN']
endpoint = config['CLUSTER']['HOST']
connection = "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())

print(f'roleArn is {arn}')
print(f'roleArn is {endpoint}')
print(connection)


print("tables are being created")
create_tables.main()
print("tables have been created")

print("ETL process are being performed")
etl.main()
print("ETL process is completed")

# conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
# cur = conn.cursor()
# success_query = 'select count(*) from staging_events'
# cur.execute(success_query)
# print(cur.fetchone()[0])