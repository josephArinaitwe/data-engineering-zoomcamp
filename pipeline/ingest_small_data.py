#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine

dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string"
}


def ingest_small_data(
        url: str,
        engine,
        target_table: str,
) -> pd.DataFrame:
    df = pd.read_csv(url, dtype=dtype)
    
    df.to_sql(
        name=target_table,
        con=engine,
        if_exists='replace'
    )
    
    print(f"Inserted {len(df)} rows into {target_table}")
    return df


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--url', default='https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv', help='URL of the CSV file')
@click.option('--target-table', default='zones', help='Target table name')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, url, target_table):
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    ingest_small_data(
        url=url,
        engine=engine,
        target_table=target_table
    )


if __name__ == '__main__':
    main()
