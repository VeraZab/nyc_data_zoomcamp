import pandas as pd

from datetime import timedelta
from pathlib import Path
from prefect import flow, task, Flow
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3, cache_key_fn=task_input_hash, log_prints=True)
def extract(month, year, color):
    """Extract from GCS and save parquet file locally"""

    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month}.parquet"
    gcs_block = GcsBucket.load("taxi-data")
    gcs_block.get_directory(
        from_path=gcs_path,
        local_path=".",
    )
    return Path(gcs_path)


@task(log_prints=True)
def transform(path):
    """Clean Data"""

    data = pd.read_parquet(path)
    print(f"pre rows: {len(data)}")
    data.passenger_count.fillna(0, inplace=True)
    print(f"post rows: {len(data)}")
    return data


@task()
def load(data):
    """Write Data to Big Query"""
    gcp_credentials = GcpCredentials.load("zoom-gcp-creds")

    data.to_gbq(
        destination_table="trips_data_all.rides",
        project_id="liquid-agility-375815",
        credentials=gcp_credentials.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(name="GCS to BQ: Month Subflow")
def etl(month, year, color):
    path = extract(month, year, color)
    cleaned_data = transform(path)
    load(cleaned_data)


@flow(name="GCS to BQ")
def main(months=["01"], year="2020", color="green"):
    """Main flow from GCS to Big Query"""

    for month in months:
        etl(month, year, color)


if __name__ == "__main__":
    main(months=["11"], year="2020", color="green")
