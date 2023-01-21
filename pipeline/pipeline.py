import os
import pandas as pd

from sqlalchemy import create_engine

def env_vars_loaded():
    if os.getenv("POSTGRES_USER") and \
        os.getenv("POSTGRES_PASSWORD") and \
        os.getenv("POSTGRES_DB") and \
        os.getenv("POSTGRES_TABLE") and \
        os.getenv("POSTGRES_HOST") and \
        os.getenv("POSTGRES_PORT") and \
        os.getenv("DATASET_URL"):
        return True
    
    return False
    
def clean_and_insert_data(data, engine):
    POSTGRES_TABLE = os.getenv("POSTGRES_TABLE")

    data.tpep_pickup_datetime = pd.to_datetime(data.tpep_pickup_datetime)
    data.tpep_dropoff_datetime = pd.to_datetime(data.tpep_dropoff_datetime)
    data.store_and_fwd_flag = data.store_and_fwd_flag.map({"Y": True, "N": False})
    data.to_sql(name=POSTGRES_TABLE, con=engine, if_exists='append')

def load_nyc_taxi_data():
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_TABLE = os.getenv("POSTGRES_TABLE")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT")
    DATASET_URL = os.getenv("DATASET_URL")

    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
    iterator = pd.read_csv(DATASET_URL, iterator=True, chunksize=100000)
    data = next(iterator)
    data.head(n=0).to_sql(name=POSTGRES_TABLE, con=engine, if_exists='replace')
    clean_and_insert_data(data, engine)
    
    while True:
        try:        
            data = next(iterator)
            clean_and_insert_data(data, engine)
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break
    
if __name__ == "__main__":
    if env_vars_loaded():
        load_nyc_taxi_data()
    else:
        print("Please add all required env vars to .env file")