#!/usr/bin/env python
# coding: utf-8
import os
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
    table_name =params.table_name #'yellow_taxi_data'
    url = params.url
    
    # the backup files are gzipped, and its important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    # download the csv
    os.system(f"wget {url} -O {csv_name}")
    
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}')
    #print(pd.io.sql.get_schema(df, name ='yellow_taxi_data',con =engine))

    df_iter = pd.read_csv(csv_name, iterator =True,chunksize =100000, on_bad_lines='warn',low_memory=False) # ,compression='gzip' - for gzipped file only
    df = next(df_iter)
    
    ## yellow taxi columns :
    #df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
    ## green taxi columns :
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name =table_name,con =engine, if_exists = 'replace')
    df.to_sql(name =table_name,con =engine, if_exists = 'append')


    for chunk in df_iter : 
        t_start = time()
        
        chunk.lpep_pickup_datetime = pd.to_datetime(chunk.lpep_pickup_datetime)
        chunk.lpep_dropoff_datetime = pd.to_datetime(chunk.lpep_dropoff_datetime)
        chunk.to_sql(name =table_name,con =engine, if_exists = 'append')
        t_end = time()
        
        print('Time taken to insert the next chuck %.3f second' %(t_end - t_start))

if __name__ =='__main__' :
    parser = argparse.ArgumentParser(description = 'Ingest CSV data to Postgres')

    parser.add_argument('--user',required =True,help = 'user name for postgres')           # positional argument
    parser.add_argument('--password',required =True, help = 'password for postgres')      
    parser.add_argument('--host',required =True, help = 'host for postgres' ) 
    parser.add_argument('--port',type = int,required =True, help = 'Port for postgres' )  
    parser.add_argument('--db',required =True, help = 'database name for postgres' )
    parser.add_argument('--table_name',required =True, help = 'Name of the table where we will write results to' )
    parser.add_argument('--url',required =True, help = 'url of the csv file' )

    args = parser.parse_args()
    main(args)



