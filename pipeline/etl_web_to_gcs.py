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


@task()
def load_locally(data, file_name):
    """Loading up to local storage as parquet file"""

    path = Path(f"data/fhv/{file_name}.csv.gz")
    data.to_csv(path, compression="gzip")
    return path


@task()
def load_gcs(path):
    """Uploading to GCS"""

    gcp_block = GcsBucket.load("taxi-data")
    gcp_block.upload_from_path(from_path=path, to_path=path)
    return


@flow(name="Web to GCP: Monthly Subflow")
def etl(month, year):
    """This is the main subflow for a given month"""

    file_name = f"fhv_tripdata_{year}-{month}"
    data_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month}.csv.gz"
    extracted_data = extract_data(data_url)
    path = load_locally(extracted_data, file_name)
    load_gcs(path)


@flow(name="Web to GCP")
def main(months=["01"], year="2020"):
    """Main flow from GCS to Big Query"""

    for month in months:
        etl(month, year)


if __name__ == "__main__":
    main(
        months=["02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"],
        year="2019",
    )
