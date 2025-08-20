# ingestion/main.py
import os
import pandas as pd
from dotenv import load_dotenv
from utils.api_client import (
    get_races_data, get_driver_standings, get_constructor_standings
)
from utils.snowflake_loader import load_df_to_snowflake
import logging

load_dotenv()
SEASON = 2025

def process_and_load_nested_data(races_data, table_name, record_path, meta_fields):
    """
    Processes nested JSON data from the 'Races' object and loads it to Snowflake.
    `record_path` specifies the list to unpack (e.g., ['Results']).
    `meta_fields` specifies the parent-level fields to include.
    """
    if not races_data:
        logging.warning(f"No race data received for {table_name}. Skipping.")
        return
    
    # json_normalize flattens the nested structure
    df = pd.json_normalize(
        races_data,
        record_path=record_path,
        meta=meta_fields,
        errors='ignore' # Ignore records where the record_path might be missing
    )

    # Clean up column names that result from nesting (e.g., 'Circuit.circuitName' -> 'CIRCUIT.CIRCUITNAME')
    df.columns = df.columns.str.upper().str.replace('.', '_', regex=False)
    
    load_df_to_snowflake(df, table_name)


def process_and_load_simple_data(data, table_name):
    """Processes simple, non-nested data like standings."""
    if not data:
        logging.warning(f"No data received for {table_name}. Skipping.")
        return
    
    df = pd.json_normalize(data)
    df['SEASON'] = SEASON
    df.columns = df.columns.str.upper().str.replace('.', '_', regex=False)
    load_df_to_snowflake(df, table_name)


def ingest_season_data(season: int):
    """Ingests all F1 data for a given season."""
    logging.info(f"Starting ingestion for the {season} F1 season...")

    # Define the parent-level fields we want to keep for each record
    meta = ['season', 'round', 'raceName', 'date', ['Circuit', 'circuitId'], ['Circuit', 'circuitName']]
    
    # Fetch, process, and load each nested data type
    results_data = get_races_data(season, 'results')
    process_and_load_nested_data(results_data, "RAW_RESULTS", ['Results'], meta)

    qualifying_data = get_races_data(season, 'qualifying')
    process_and_load_nested_data(qualifying_data, "RAW_QUALIFYING", ['QualifyingResults'], meta)
    
    
    # pitstops_data = get_races_data(season, 'pitstops')
    # Pitstops doesn't have a record path, the list is the data itself
    # We still need to join it to get constructor, but the raw load is simpler.
    # For now, let's keep the pitstop ingestion simple as it doesn't contain circuit info anyway.
    # Note: A full implementation might handle pitstops differently.
    
    # Ingest standings data (this part remains the same)
    driver_standings = get_driver_standings(season)
    process_and_load_simple_data(driver_standings, "RAW_DRIVER_STANDINGS")

    constructor_standings = get_constructor_standings(season)
    process_and_load_simple_data(constructor_standings, "RAW_CONSTRUCTOR_STANDINGS")
    
    logging.info(f"Finished ingestion for the {season} F1 season.")

if __name__ == "__main__":
    ingest_season_data(SEASON)