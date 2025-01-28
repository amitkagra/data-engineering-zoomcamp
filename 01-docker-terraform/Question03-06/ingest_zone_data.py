#import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    #url = params.url
    
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    #if url.endswith('.csv.gz'):
     #   csv_name = 'output.csv.gz'
    #else:
     #   csv_name = 'output.csv'

    #os.system(f"wget {url} -O {csv_name}")

    # Read the file
    df = pd.read_csv('taxi_zone_lookup.csv')
    #df = pd.read_csv(csv_name)
    
    # Connect to PostgreSQL database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Insert the data into the database
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    #parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args) 