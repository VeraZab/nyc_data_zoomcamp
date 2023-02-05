import os
import pandas as pd

from datetime import timedelta
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector


@task(
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def extract_data(data_url):
    """Getting data from url"""

    return pd.read_csv(data_url)


@task()
def transform_data(data):
    """Cleaning columns"""

    data.lpep_pickup_datetime = pd.to_datetime(data.lpep_pickup_datetime)
    data.lpep_dropoff_datetime = pd.to_datetime(data.lpep_dropoff_datetime)
    data.store_and_fwd_flag = data.store_and_fwd_flag.map({"Y": True, "N": False})
    return data


@task()
def load_data(data, table_name):
    """Loading up to database"""

    with SqlAlchemyConnector.load("psql-db") as database_block:
        engine = database_block.get_connection(begin=False)
        data.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
        data.to_sql(name=table_name, con=engine, if_exists="append")


@flow(name="ETL Taxi Data")
def etl(month="01", year="2020", color="green"):
    """This is the main flow"""

    table_name = f"{color}_taxi_trips"
    data_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month}.csv.gz"
    extracted_data = extract_data(data_url)
    transformed_data = transform_data(extracted_data)
    load_data(transformed_data, table_name)


if __name__ == "__main__":
    etl(month="01", year="2020", color="green")
