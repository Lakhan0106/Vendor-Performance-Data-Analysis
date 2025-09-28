import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# --- Ensure required directories exist ---
os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)  # Optional: ensure 'data' dir exists

# --- Set up logging ---
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# --- Create SQLite engine ---
engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    '''This function ingests the DataFrame into the specified database table.'''
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"Successfully ingested table '{table_name}' into the database.")
    except Exception as e:
        logging.error(f"Failed to ingest table '{table_name}': {e}")

def load_raw_data():
    '''This function loads CSV files from the data folder and ingests them into the DB.'''
    logging.info("Starting data ingestion process...")
    
    start = time.time()

    data_files = [f for f in os.listdir('data') if f.endswith('.csv')]
    if not data_files:
        logging.warning("No CSV files found in the 'data' directory.")
        return

    for file in data_files:
        try:
            file_path = os.path.join('data', file)
            df = pd.read_csv(file_path)
            logging.info(f"Ingesting {file} into the database...")
            ingest_db(df, file[:-4], engine)  # table name without .csv
        except Exception as e:
            logging.error(f"Error processing file {file}: {e}")

    end = time.time()
    total_time = (end - start) / 60
    logging.info('-------------Ingestion Complete------------')
    logging.info(f'Total Time Taken: {total_time:.2f} minutes')

# --- Entry point ---
if __name__ == '__main__':
    load_raw_data()
