#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import pandas as pd
from time import time
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password= params.password
    host= params.host
    port= params.port
    db= params.db
    table_name= params.table_name
    url = params.url

    parquet_name = '~/data/ny_taxi_data/yellow_tripdata_2021-01.parquet'

    csv_name = parquet_name.replace('parquet', 'csv')
    # download the csv

    os.system(f"wget {url} -O {parquet_name}")
    df = pd.read_parquet(parquet_name)
    df.to_csv(csv_name, encoding='utf8', sep=';')
    df = pd.read_csv(csv_name, encoding='utf8', sep=';')
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    df_iter = pd.read_csv(csv_name, sep=';', iterator=True, chunksize=100000)


    chunk_number = 0
    while True:
        try:
            chunk_number += 1
            t_start = time()

            df = next(df_iter)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()
            print(f'inserted chunk {chunk_number}, took {round((t_end - t_start), ndigits=2)}')
        except StopIteration:
            print('All chunks are inserted')
            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                        description='Ingest parquet data to Postgres and write csv')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)
