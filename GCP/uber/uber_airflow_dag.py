from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowPythonOperator
from datetime import datetime

with DAG(
    "uber_beam_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    run_etl = DataflowPythonOperator(
        task_id="run_uber_etl",
        py_file="gs://<your-dag-bucket>/beam_uber.py",  # points to same script
        options={
            "input": "gs://<raw-bucket>/ncr_ride_bookings.csv",
            "output-prefix": "gs://<processed-bucket>/uber",
            "project-id": "<your-project-id>",
            "region": "europe-west2",
            "runner": "DataflowRunner"
        }
    )
