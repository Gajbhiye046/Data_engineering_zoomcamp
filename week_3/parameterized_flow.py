from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect.filesystems import GitHub

@task(retries=3)#, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df = df.convert_dtypes()
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
    df.rename(columns = {"dropOff_datetime":"dropOff_datetime"}, inplace = True)
    
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    
    path = Path(f"data/{color}/{dataset_file}.csv.gz").as_posix()
    df.to_csv(path, compression="gzip",index=False)
    return path


@task()
def write_gcs(path_from: Path,path_to : Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path_from, to_path=path_to)
    return


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    #dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    ## Fetch, clean and write_local was done via jupyter notebook
    #df = fetch(dataset_url)
    #df_clean = clean(df)
    path_from = Path(f"C:/Users/Lenovo/data_engineering_zoomcamp/data/{color}/{dataset_file}.csv.gz").as_posix()
    path_to = Path(f"data/{color}/{dataset_file}.csv.gz").as_posix()
    write_gcs(path_from,path_to)


@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == "__main__":
    color = "fhv"
    months = [1, 2]
    year = 2019
    etl_parent_flow(months, year, color)