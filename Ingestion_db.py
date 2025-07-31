import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# ✅ Ensure logs folder exists
os.makedirs('logs', exist_ok=True)

# ✅ Setup logging
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# ✅ Create database engine
engine = create_engine('sqlite:///inventory.db')

# ✅ Define the ingest function first
def ingest_db(df, table_name, engine):
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

# ✅ Properly defined and indented load function
def load_raw_data():
    '''This function will load the CSVs as DataFrames and ingest them into the DB'''
    start = time.time()
    folder_path = r"C:\Users\Shraddha.Shukla\Downloads\data\data"

    for file in os.listdir(folder_path):
        if file.endswith('.csv'):
            df = pd.read_csv(os.path.join(folder_path, file))
            logging.info(f'Ingesting {file} in DB')
            ingest_db(df, file[:-4], engine)

    end = time.time()
    total_time = (end - start) / 60
    logging.info('Ingestion Complete')
    logging.info(f'Total Time Taken: {total_time:.2f} minutes')

# ✅ Run it
load_raw_data()
