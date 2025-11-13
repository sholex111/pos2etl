# app/etl.py

import os
import glob
import pandas as pd
from sqlalchemy import text
from utils import get_db_engine, setup_logging

# Setup logger for this module
logger = setup_logging()

# Get data folder path from environment variable
DATA_FOLDER = os.getenv('DATA_FOLDER', './data/')

# --- SQL Definitions ---
# We use 'ecomm_sales' as the theme for our new tables.

CREATE_STAGING_TABLE = """
CREATE TABLE IF NOT EXISTS staging_ecomm_sales (
    invoice VARCHAR(50),
    stockcode VARCHAR(50),
    description TEXT,
    quantity INT,
    invoicedate TIMESTAMP WITH TIME ZONE,
    price DECIMAL(10, 2),
    customer_id VARCHAR(50),
    country VARCHAR(100),
    desc_low TEXT,
    category VARCHAR(100),
    margin DECIMAL(10, 4),
    profit DECIMAL(10, 2),
    source_file VARCHAR(255)
);
"""

CREATE_CORE_TABLE = """
CREATE TABLE IF NOT EXISTS core_ecomm_sales (
    -- We create a composite primary key for deduplication
    line_item_id VARCHAR(100) PRIMARY KEY, 
    invoice VARCHAR(50),
    stockcode VARCHAR(50),
    description TEXT,
    quantity INT,
    invoicedate TIMESTAMP WITH TIME ZONE,
    price DECIMAL(10, 2),
    customer_id VARCHAR(50),
    country VARCHAR(100),
    desc_low TEXT,
    category VARCHAR(100),
    margin DECIMAL(10, 4),
    profit DECIMAL(10, 2)
);
"""

# This query inserts data from staging to core.
# We create the line_item_id on the fly using CONCAT.
# ON CONFLICT (line_item_id) DO NOTHING handles deduplication.
INSERT_FROM_STAGING = """
INSERT INTO core_ecomm_sales (
    line_item_id, invoice, stockcode, description, quantity, 
    invoicedate, price, customer_id, country, desc_low, category, margin, profit
)
SELECT
    CONCAT(invoice, '_', stockcode) AS line_item_id,
    invoice,
    stockcode,
    description,
    quantity,
    invoicedate,
    price,
    customer_id,
    country,
    desc_low,
    category,
    margin,
    profit
FROM staging_ecomm_sales
ON CONFLICT (line_item_id) DO NOTHING;
"""

TRUNCATE_STAGING = "TRUNCATE TABLE staging_ecomm_sales;"

# --- ETL Functions ---

def create_tables(engine):
    """Creates the staging and core tables if they don't exist."""
    try:
        with engine.connect() as conn:
            conn.execute(text(CREATE_STAGING_TABLE))
            conn.execute(text(CREATE_CORE_TABLE))
            conn.commit()
        logger.info("Staging and Core e-commerce tables verified/created.")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise

def find_csv_files(folder_path):
    """Finds all .csv files in the specified folder."""
    search_path = os.path.join(folder_path, "*.csv")
    files = glob.glob(search_path)
    logger.info(f"Found {len(files)} CSV files in {folder_path}.")
    return files

def process_file_to_staging(filepath, engine):
    """Reads a single CSV, transforms it, and loads it to the staging table."""
    try:
        logger.info(f"Processing file: {filepath}")
        df = pd.read_csv(filepath)
        
        # --- Transformation ---
        # 1. Standardize column names (lowercase, replace spaces with _)
        #    e.g., "Customer ID" becomes "customer_id"
        df.columns = df.columns.str.lower().str.replace(' ', '_')

        # 2. Ensure correct data types
        #    This is crucial for SQL loading
        df['invoicedate'] = pd.to_datetime(df['invoicedate'])
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0).astype(int)
        df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
        df['margin'] = pd.to_numeric(df['margin'], errors='coerce').fillna(0)
        df['profit'] = pd.to_numeric(df['profit'], errors='coerce').fillna(0)
        
        # 3. Handle potential nulls in string columns
        str_cols = ['invoice', 'stockcode', 'description', 'customer_id', 'country', 'desc_low', 'category']
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).fillna('Unknown')

        # 4. Add source file for tracking
        df['source_file'] = os.path.basename(filepath)
        
        # --- Load to Staging ---
        # Ensure columns in DF match the staging table order
        staging_cols = [
            'invoice', 'stockcode', 'description', 'quantity', 'invoicedate', 
            'price', 'customer_id', 'country', 'desc_low', 'category', 
            'margin', 'profit', 'source_file'
        ]
        
        # Filter DF to only include columns that exist in the staging table
        df_to_load = df[[col for col in staging_cols if col in df.columns]]

        df_to_load.to_sql(
            'staging_ecomm_sales',
            con=engine,
            if_exists='append', # Add data to the table
            index=False,
            method='multi' # Efficiently insert rows in batches
        )
        logger.info(f"Loaded {len(df_to_load)} rows from {filepath} to staging_ecomm_sales.")
        
    except Exception as e:
        logger.error(f"Failed to process file {filepath}: {e}")
        # Log which columns were expected vs. found
        logger.debug(f"File columns: {list(df.columns)}")

def move_data_to_core(engine):
    """Moves unique data from staging to core and truncates staging."""
    try:
        with engine.connect() as conn:
            # 1. Insert unique records into core table
            result = conn.execute(text(INSERT_FROM_STAGING))
            conn.commit()
            logger.info(f"Moved {result.rowcount} new rows from staging to core_ecomm_sales.")
            
            # 2. Clear staging table for next run
            conn.execute(text(TRUNCATE_STAGING))
            conn.commit()
            logger.info("Staging table truncated.")
    except Exception as e:
        logger.error(f"Error moving data to core: {e}")
        raise

# --- Main Execution ---
def main():
    logger.info("=== Starting E-Commerce ETL Process ===")
    try:
        engine = get_db_engine()
        
        # 1. Setup database tables
        create_tables(engine)
        
        # 2. Find CSV files
        csv_files = find_csv_files(DATA_FOLDER)
        if not csv_files:
            logger.warning(f"No CSV files found in {DATA_FOLDER}. Exiting.")
            return

        # 3. Process each file into staging table
        for f in csv_files:
            process_file_to_staging(f, engine)
        
        # 4. Move data from staging to core (deduplication)
        move_data_to_core(engine)
        
        logger.info("=== ETL Process Completed Successfully ===")
        
    except Exception as e:
        logger.critical(f"ETL process failed: {e}")
    finally:
        logger.info("ETL script finished.")

if __name__ == "__main__":
    main()