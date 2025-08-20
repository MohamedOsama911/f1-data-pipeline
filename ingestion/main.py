# ingestion/main.py
import pandas as pd
from dotenv import load_dotenv
from utils.api_client import (
    get_all_season_results, get_all_season_qualifying, get_all_season_pit_stops,
    get_driver_standings, get_constructor_standings
)
from utils.snowflake_loader import load_df_to_snowflake
import logging

load_dotenv()
SEASON = 2025

def process_and_load_nested(data, table_name, record_path, meta_fields):
    """Processes and loads data with a nested structure."""
    if not data:
        logging.warning(f"No data for {table_name}, skipping.")
        return
    df = pd.json_normalize(data, record_path=record_path, meta=meta_fields, errors='ignore')
    df.columns = df.columns.str.upper().str.replace('.', '_', regex=False)
    load_df_to_snowflake(df, table_name)

def process_and_load_simple(data, table_name):
    """Processes and loads data with a simple (flat) structure."""
    if not data:
        logging.warning(f"No data for {table_name}, skipping.")
        return
    df = pd.json_normalize(data)
    df['SEASON'] = SEASON
    df.columns = df.columns.str.upper().str.replace('.', '_', regex=False)
    load_df_to_snowflake(df, table_name)


def ingest_results(season: int):
    """Ingests race results for a given season."""
    results_data = get_all_season_results(season)
    meta = ['season', 'round', 'raceName', 'date', ['Circuit', 'circuitId'], ['Circuit', 'circuitName']]
    process_and_load_nested(results_data, "RAW_RESULTS", ['Results'], meta)

def ingest_qualifying(season: int):
    """Ingests qualifying results for a given season."""
    qualifying_data = get_all_season_qualifying(season)
    meta = ['season', 'round', 'raceName', 'date', ['Circuit', 'circuitId'], ['Circuit', 'circuitName']]
    process_and_load_nested(qualifying_data, "RAW_QUALIFYING", ['QualifyingResults'], meta)

def ingest_pit_stops(season: int):
    """Ingests pit stop data for a given season."""
    pit_stop_data = get_all_season_pit_stops(season)
    process_and_load_simple(pit_stop_data, "RAW_PIT_STOPS")

def ingest_driver_standings(season: int):
    """Ingests driver standings for a given season."""
    driver_standings_data = get_driver_standings(season)
    process_and_load_simple(driver_standings_data, "RAW_DRIVER_STANDINGS")

def ingest_constructor_standings(season: int):
    """Ingests constructor standings for a given season."""
    constructor_standings_data = get_constructor_standings(season)
    process_and_load_simple(constructor_standings_data, "RAW_CONSTRUCTOR_STANDINGS")


def ingest_all_data_for_season(season: int):
    """
    Main orchestration function to ingest all F1 data for a given season.
    """
    logging.info(f"STARTING FULL INGESTION FOR THE {season} F1 SEASON...")
    
    ingest_results(season)
    ingest_qualifying(season)
    ingest_pit_stops(season)
    ingest_driver_standings(season)
    ingest_constructor_standings(season)
    
    logging.info(f"COMPLETED FULL INGESTION FOR THE {season} F1 SEASON.")

if __name__ == "__main__":
    ingest_all_data_for_season(SEASON)