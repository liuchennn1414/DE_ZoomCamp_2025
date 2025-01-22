#!/usr/bin/env python
# coding: utf-8

import os 
import argparse 
import pandas as pd 
from sqlalchemy import create_engine
from time import time 
import gzip
import shutil

def main(params):
    user = params.user
    pwd = params.pwd 
    host = params.host 
    port = params.port 
    db = params.db 
    table_name = params.table_name 
    url = params.url
    csv_name = 'output.csv'

    # Download the .gz file from github 
    os.system(f"wget {url} -O {csv_name}.gz")
    print(f"Downloaded {csv_name}.gz")

    # Decompress the .gz file
    try:
        with gzip.open(f"{csv_name}.gz", 'rb') as f_in:
            with open(csv_name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"Decompressed {csv_name}")
    except Exception as e:
        print(f"Error decompressing file: {e}")
        return

    # Verify the decompressed file
    if os.path.exists(csv_name) and os.path.getsize(csv_name) > 0:
        print(f"{csv_name} exists with size: {os.path.getsize(csv_name)} bytes")
    else:
        print(f"{csv_name} does not exist or is empty.")
        return

    engine = create_engine(f'postgresql://{user}:{pwd}@{host}:{port}/{db}')
    engine.connect()


    df_iter = pd.read_csv(csv_name,iterator=True,chunksize=100000)
    df = next(df_iter)

    #df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    #df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])

    # ingest table header 
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # ingest first chuck 
    print('Started Ingestion')
    df.to_sql(name=table_name, con=engine, if_exists='append')
    print('First chunk done')

    # ingest 
    flag = True 
    while flag: 
        t_start = time() 

        try: 
            df = next(df_iter)
        except StopIteration:
            print("Data ingestion is done.")
            flag = False
            break 
            

        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time() 

        print('inserted another chunk..., took %.3f second' % (t_end - t_start))



if __name__ == '__main__': 
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # users, pwd, host, port, database name, table name, url of the csv 
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--pwd', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table-name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)


 