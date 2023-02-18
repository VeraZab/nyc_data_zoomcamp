import pandas as pd

from datetime import timedelta
from pathlib import Path
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_dbt import DbtCoreOperation
from prefect_gcp import GcpCredentials

@task()
def transform():
    dbt_op = DbtCoreOperation.load("ny-taxi")
    dbt_op.run()


@task()
def load(blob, color):
    """Load Data to BigQuery Warehouse"""

    gcp_credentials = GcpCredentials.load("zoom-gcp-creds")
    
    blob.to_gbq(
        destination_table=f"trips_data_all.{color}_taxi_rides",
        project_id="liquid-agility-375815",
        credentials=gcp_credentials.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@task(    
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1)
)
def extract(month, year, color):
    """Extract csv data from source"""

    if len(f"{month}") == 1:
        month = f"0{month}"

    file_name = f"{color}_tripdata_{year}-{month}"
    data_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{file_name}.csv.gz"
    return pd.read_csv(data_url, nrows=100)


@flow()
def extract_and_load(month, year, color):
    """Subflow Extracting and Loading a particular file"""
    blob = extract(month, year, color)
    load(blob, color)


def el_main(month_range, year_range, colors):
    """Run all parametrized extraction and loading flows"""
    for color in colors:
        for year in range(year_range[0], year_range[1] + 1):
            for month in range(month_range[0], month_range[1] + 1):
                extract_and_load(month, year, color)


@flow(log_prints=True)
def main(month_range, year_range, colors):
    """Main function kicking off our subflows"""
    el_main(month_range, year_range, colors)
    transform()
    

if __name__ == "__main__":
    main(month_range=[1,2], year_range=[2019, 2020], colors=["yellow", "green"])