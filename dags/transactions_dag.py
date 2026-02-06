from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import zipfile

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor

import pandas as pd

BASE_DIR = Path("/opt/airflow/data")
INCOMING_DIR = BASE_DIR / "incoming"
STAGING_DIR = BASE_DIR / "staging"
BAD_DIR = BASE_DIR / "bad_records"

ZIP_FILE_PATH = "incoming/transactions_{{ ds_nodash }}.zip"


@task
def unzip_file(ds: str, ds_nodash: str) -> dict:
    zip_path = INCOMING_DIR / f"transactions_{ds_nodash}.zip"
    if not zip_path.exists():
        raise AirflowSkipException(f"Zip file {zip_path} does not exist")

    run_staging_dir = STAGING_DIR / ds
    run_staging_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(run_staging_dir)

    csv_files = list(run_staging_dir.glob("*.csv"))
    if not csv_files:
        raise ValueError(f"No CSV files found in {run_staging_dir}")

    csv_path = csv_files[0]
    if csv_path.stat().st_size == 0:
        raise ValueError(f"CSV file {csv_path} is empty")

    return {
        "zip_path": str(zip_path),
        "staging_dir": str(run_staging_dir),
        "csv_path": str(csv_path),
    }


@task
def transform_data(csv_path: str,ds: str,ds_nodash: str) -> dict:
    df =pd.read_csv(csv_path)
    required_columns=["transaction_id","customer_id","transaction_date","amount"]
    missing=[c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    df["transaction_id"]=df["transaction_id"].astype(str).str.strip()
    df["customer_id"]=df["customer_id"].astype(str).str.strip()
    df["transaction_date"]=pd.to_datetime(df["transaction_date"],errors="coerce")
    df["amount"]=pd.to_numeric(df["amount"],errors="coerce")

    df=df.dropduplicates(subset=["transaction_id"])
    valid_mask=(
        df["transaction_id"].ne("") &
        df["customer_id"].ne("") &
        df["transaction_date"].notna() &
        df["amount"].notna() &
        (df["amount"]>0)
    )

    valid_df=df[valid_mask].copy()
    bad_df=df[~valid_mask].copy()

    valid_df["load_date"]=ds

    BAD_DIR.mkdir(parents=True,exist_ok=True)
    bad_path=BAD_DIR/f"bad_{ds_nodash}.csv"
    if len(bad_df)>0:
        bad_df.to_csv(bad_path,index=False)

    staging_dir=Path(csv_path).parent
    clean_path=staging_dir/f"clean_{ds_nodash}.csv"
    valid_df.to_csv(clean_path,index=False)
    return {
        "clean_csv_path": str(clean_path),
        "records_total": int(len(df)),
        "records_valid": int(len(valid_df)),
        "records_bad": int(len(bad_df)),
        "bad_path": str(bad_path) if len(bad_df) else None,
    }





default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="transactions_dag",
    start_date=datetime(2026, 2, 5),
    schedule="@daily",
    catchup=True,  
    default_args=default_args,
    tags=["capstone", "batch", "zip"],
) as dag:

    start = EmptyOperator(task_id="start")

    sense_zip = FileSensor(
        task_id="sense_zip",
        fs_conn_id="fs_capstone",     
        filepath=ZIP_FILE_PATH,      
        poke_interval=10,
        timeout=60 * 30,
        mode="poke",
        retries=0,  
    )

    unzipped = unzip_file(ds="{{ ds }}", ds_nodash="{{ ds_nodash }}")
    
    xform = transform_validate(
    csv_path=unzipped["csv_path"],
    ds="{{ ds }}",
    ds_nodash="{{ ds_nodash }}",
)


    end = EmptyOperator(task_id="end")

    start >> sense_zip >> unzipped >> xform >> end
