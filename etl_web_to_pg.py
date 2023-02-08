#!/usr/bin/env python

from pathlib import Path
from prefect import task, flow
from prefect_sqlalchemy import SqlAlchemyConnector

import pandas as pd

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    print(f'fetch {dataset_url}')
    df = pd.read_csv(dataset_url, compression='gzip')
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix dtype issues"""
    print(f'clean for {color}')
    if color=="yellow":
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    else:
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True)
def write_postgres(table_name, df):
    with SqlAlchemyConnector.load("block-pg") as connection_block:
        with connection_block.get_connection(begin=False) as engine:
            df.head(n=0).to_sql(name = table_name, con = engine, if_exists = 'replace')
            df.to_sql(name=table_name, con=engine, if_exists = 'append')

@flow(log_prints=True)
def etl_web_to_pq(year: int, month: int, color: str) -> int:
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df = fetch(dataset_url)
    df_clean = clean(df,color)
    #path = write_local(df_clean, color, dataset_file)
    #write_postgres(f'{color}_{year}', df_clean)
    return len(df_clean)


@flow(log_prints=True)
def main_ETL(months: list[int] = [2,3], year: int = 2019, color: str = "yellow"):
    total_rows = 0    
    for month in months:
        total_rows += etl_web_to_pq(year, month, color)
    print(f'Total rows in Main ETL written {total_rows}')
    
if __name__ == "__main__":
    year = 2019
    months = [2,3]
    color = "yellow"
    main_ETL(months, year, color)