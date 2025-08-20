import os
import pandas as pd
from dotenv import load_dotenv
from utils.api_client import (
    get_total_rounds, get_race_results, get_qualifying_results,
    get_pit_stops, get_driver_standings, get_constructor_standings
)
from utils.snowflake_loader import load_df_to_snowflake
import logging

# Load environment variables from .env file
load_dotenv()

SEASON = 2025 

def process_and_load(data, table_name, extra_cols={}):
    """Processes raw JSON data into a DataFrame and loads it."""
    if not data:
        logging.warning(f"No data received for {table_name}. Skipping.")
        return
    
    df = pd.json_normalize(data)
    
    # Add extra columns like season and round if provided
    for col, val in extra_cols.items():
        df[col] = val
        
    # Convert column names to uppercase to match Snowflake's default behavior
    df.columns = [col.upper() for col in df.columns]

    load_df_to_snowflake(df, table_name)

def ingest_season_data(season: int):
    """Ingests all F1 data for a given season."""
    logging.info(f"Starting ingestion for the {season} F1 season...")
    total_rounds = get_total_rounds(season)
    
    if total_rounds == 0:
        logging.error("Could not determine the number of rounds. Aborting.")
        return

    logging.info(f"Found {total_rounds} rounds for the {season} season.")

    all_results, all_qualifying, all_pit_stops = [], [], []

    for r in range(1, total_rounds + 1):
        round_info = {'SEASON': season, 'ROUND': r}
        
        # Append data from each round to a list
        # extend means you’re appending a whole list of dicts at once.
        all_results.extend(get_race_results(season, r))
        all_qualifying.extend(get_qualifying_results(season, r))
        all_pit_stops.extend(get_pit_stops(season, r))
    
    # Process and load data for each data type
    process_and_load(all_results, "RAW_RESULTS", {'SEASON': season})
    process_and_load(all_qualifying, "RAW_QUALIFYING", {'SEASON': season})
    process_and_load(all_pit_stops, "RAW_PIT_STOPS", {'SEASON': season})

    # Ingest standings data
    driver_standings = get_driver_standings(season)
    process_and_load(driver_standings, "RAW_DRIVER_STANDINGS", {'SEASON': season})

    constructor_standings = get_constructor_standings(season)
    process_and_load(constructor_standings, "RAW_CONSTRUCTOR_STANDINGS", {'SEASON': season})
    
    logging.info(f"Finished ingestion for the {season} F1 season.")

if __name__ == "__main__":
    ingest_season_data(SEASON)