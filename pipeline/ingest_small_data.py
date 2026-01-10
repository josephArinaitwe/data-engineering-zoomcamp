#!/usr/bin/env python
# coding: utf-8
"""
Ingest Small Data Script
========================

A lightweight data ingestion script optimized for small CSV datasets.
Designed for loading reference/lookup tables (e.g., taxi zones) into PostgreSQL.

Unlike the chunked ingestion approach used for large datasets, this script
loads the entire CSV into memory at once, making it simpler and faster
for small files.

Usage:
    python ingest_small_data.py [OPTIONS]

Example:
    python ingest_small_data.py --pg-host pgdatabase --target-table zones
"""

import click
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# -----------------------------------------------------------------------------
# Data Type Configuration
# -----------------------------------------------------------------------------
# Define explicit dtypes for the taxi zones lookup table.
# Using nullable Int64 and string types ensures proper handling of missing values
# and consistent database schema generation.

DTYPE_ZONES = {
    "LocationID": "Int64",      # Primary key - taxi zone identifier
    "Borough": "string",        # NYC borough name (Manhattan, Brooklyn, etc.)
    "Zone": "string",           # Specific zone name within the borough
    "service_zone": "string"    # Service classification (Yellow Zone, Boro Zone, etc.)
}


# -----------------------------------------------------------------------------
# Core Ingestion Function
# -----------------------------------------------------------------------------

def ingest_small_data(
        url: str,
        engine: Engine,
        target_table: str,
) -> pd.DataFrame:
    """
    Ingest a small CSV dataset directly into a PostgreSQL table.

    This function is optimized for small reference datasets that can be
    loaded entirely into memory. It replaces the target table if it exists.

    Args:
        url: URL or file path to the CSV file.
        engine: SQLAlchemy engine connected to the target database.
        target_table: Name of the destination table in PostgreSQL.

    Returns:
        pd.DataFrame: The ingested data as a DataFrame.

    Raises:
        pd.errors.ParserError: If the CSV file cannot be parsed.
        sqlalchemy.exc.SQLAlchemyError: If database insertion fails.
    """
    # Load entire CSV into memory (suitable for small datasets only)
    df = pd.read_csv(url, dtype=DTYPE_ZONES)

    # Write to PostgreSQL, replacing existing table if present
    df.to_sql(
        name=target_table,
        con=engine,
        if_exists='replace',
        index=False  # Don't write DataFrame index as a column
    )

    print(f"✓ Successfully inserted {len(df)} rows into '{target_table}'")
    return df


# -----------------------------------------------------------------------------
# CLI Entry Point
# -----------------------------------------------------------------------------

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option(
    '--url',
    default='https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv',
    help='URL of the CSV file to ingest'
)
@click.option('--target-table', default='zones', help='Target table name')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, url, target_table):
    """
    CLI entry point for small data ingestion.

    Creates a database connection and triggers the ingestion process
    with the provided configuration options.
    """
    # Build PostgreSQL connection string and create engine
    connection_string = f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    engine = create_engine(connection_string)

    print(f"→ Connecting to PostgreSQL at {pg_host}:{pg_port}/{pg_db}")
    print(f"→ Ingesting data from: {url}")

    # Execute ingestion
    ingest_small_data(
        url=url,
        engine=engine,
        target_table=target_table
    )

    print("✓ Ingestion complete!")


if __name__ == '__main__':
    main()
