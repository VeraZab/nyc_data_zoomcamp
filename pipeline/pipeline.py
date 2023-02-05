import os
import pandas as pd

from datetime import timedelta
from pathlib import Path
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket


@task(
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def extract_data(data_url):
    """Getting data from url"""

    return pd.read_csv(data_url)


@task(
    log_prints=True,
)
def transform_data(data, color):
    """Cleaning columns"""

    if color == "green":
        data.lpep_pickup_datetime = pd.to_datetime(data.lpep_pickup_datetime)
        data.lpep_dropoff_datetime = pd.to_datetime(data.lpep_dropoff_datetime)
    elif color == "yellow":
        data.tpep_pickup_datetime = pd.to_datetime(data.tpep_pickup_datetime)
        data.tpep_dropoff_datetime = pd.to_datetime(data.tpep_dropoff_datetime)

    data.store_and_fwd_flag = data.store_and_fwd_flag.map({"Y": True, "N": False})
    print(f"rows: {len(data)}")
    return data


@task()
def load_locally(data, color, file_name):
    """Loading up to local storage as parquet file"""

    path = Path(f"data/{color}/{file_name}.parquet")
    data.to_parquet(path, compression="gzip")
    return path


@task()
def load_gcs(path):
    """Uploading to GCS"""

    gcp_block = GcsBucket.load("taxi-data")
    gcp_block.upload_from_path(from_path=path, to_path=path)
    return


@flow(name="ETL Taxi Data")
def etl(month="01", year="2020", color="green"):
    """This is the main flow"""

    file_name = f"{color}_tripdata_{year}-{month}"
    data_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{file_name}.csv.gz"
    extracted_data = extract_data(data_url)
    transformed_data = transform_data(extracted_data, color)
    path = load_locally(transformed_data, color, file_name)
    load_gcs(path)


if __name__ == "__main__":
    etl(month="01", year="2020", color="green")
