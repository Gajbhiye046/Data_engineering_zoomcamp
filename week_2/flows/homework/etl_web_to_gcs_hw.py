# running the cmd in "zoomcamp" python =3.9 environment and data_engineering_zoomcamp directory
# (zoomcamp) C:\Users\Lenovo\data_engineering_zoomcamp>
from pathlib import Path
import pandas as pd
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=1)
def fetch(dataset_url:str) -> pd.DataFrame:
    """Read taxi data from the web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints =True)
def clean(df : pd.DataFrame)-> pd.DataFrame:
    "Fix the dtype issue"
    ##use this for green taxi:
    #df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    #df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    ##use this for yellow taxi:
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f'columns:{df.dtypes}')
    print(f'rows :{len(df)}')
    return df

@task 
def write_local(df: pd.DataFrame,color : str, dataset_file : str) -> Path :
    """ Write DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet").as_posix()
    df.to_parquet(path,engine='pyarrow',compression="gzip")
    return path

@task()
def write_gcs(path: Path)-> None :
    "Upload local parquet to GCS"
    gcs_block = GcsBucket.load("zoom-gcs")
    #https://prefecthq.github.io/prefect-gcp/cloud_storage/#prefect_gcp.cloud_storage.GcsBucket.upload_from_path
    gcs_block.upload_from_path(from_path = path,to_path = path)
    return 

@flow()
def etl_web_to_gcs() -> None:
    """The Main ETL function"""
    color ="green"#"yellow"
    year = 2020 #2021
    month = 11 #January
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean,color,dataset_file)
    write_gcs(path)

if __name__ == "__main__":
    etl_web_to_gcs()
