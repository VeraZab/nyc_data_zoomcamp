import os
import pandas as pd

from sqlalchemy import create_engine

def env_vars_loaded():
    if os.getenv("POSTGRES_USER") and \
        os.getenv("POSTGRES_PASSWORD") and \
        os.getenv("POSTGRES_DB") and \
        os.getenv("POSTGRES_HOST") and \
        os.getenv("POSTGRES_PORT"):
        return True
    
    return False
    
def load_green_taxi_data(engine):
    POSTGRES_TABLE = "green_taxi"
    DATASET_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"

    def clean_data(data):
        data.lpep_pickup_datetime = pd.to_datetime(data.lpep_pickup_datetime)
        data.lpep_dropoff_datetime = pd.to_datetime(data.lpep_dropoff_datetime)
        data.store_and_fwd_flag = data.store_and_fwd_flag.map({"Y": True, "N": False})
        

    iterator = pd.read_csv(DATASET_URL, iterator=True, chunksize=100000)
    data = next(iterator)
    clean_data(data)
    data.head(n=0).to_sql(name=POSTGRES_TABLE, con=engine, if_exists='replace')
    data.to_sql(name=POSTGRES_TABLE, con=engine, if_exists='append')
    
    while True:
        try:        
            data = next(iterator)
            clean_data(data)
            data.to_sql(name=POSTGRES_TABLE, con=engine, if_exists='append')
        except StopIteration:
            print("Finished ingesting green taxi data into the postgres database")
            break

def load_zones_data(engine):
    POSTGRES_TABLE = "zones"
    DATASET_URL = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

    iterator = pd.read_csv(DATASET_URL, iterator=True, chunksize=100000)
    data = next(iterator)
    data.head(n=0).to_sql(name=POSTGRES_TABLE, con=engine, if_exists='replace')
    data.to_sql(name=POSTGRES_TABLE, con=engine, if_exists='append')

    while True:
        try:        
            data = next(iterator)
            data.to_sql(name=POSTGRES_TABLE, con=engine, if_exists='append')
        except StopIteration:
            print("Finished ingesting zones data into the postgres database")
            break

def load_db():
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT")

    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
    load_green_taxi_data(engine)
    load_zones_data(engine)
    
    
if __name__ == "__main__":
    if env_vars_loaded():
        load_db()
    else:
        print("Please add all required env vars to .env file")