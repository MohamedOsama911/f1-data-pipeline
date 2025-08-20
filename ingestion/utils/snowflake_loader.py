import os
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector import connect
import logging

def get_snowflake_connection():
    """Establishes a connection to Snowflake using environment variables."""
    return connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA_RAW")
    )

def load_df_to_snowflake(df: pd.DataFrame, table_name: str):
    """Loads a pandas DataFrame into a specified Snowflake table."""
    if df.empty:
        logging.warning(f"DataFrame for {table_name} is empty. Skipping load.")
        return

    table_name = table_name.upper()
    logging.info(f"Loading {len(df)} rows into {table_name}...")

    try:
        with get_snowflake_connection() as conn:
            # Using write_pandas for efficient bulk loading
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name=table_name,
                auto_create_table=True, # Creates table if it doesn't exist
                overwrite=True # Overwrites table content on each run
            )
            logging.info(f"Successfully loaded {nrows} rows into {table_name}.")
    except Exception as e:
        logging.error(f"Failed to load data into {table_name}: {e}")