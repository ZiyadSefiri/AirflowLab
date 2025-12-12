from airflow import DAG
from airflow.providers.docker import DockerOperator

from datetime import datetime, timedelta
from pathlib import Path

repo_root = Path(__file__).parent.parent
raw_data= repo_root/"RawData"
processed_data = repo_root/"ProcessedData"


with DAG(
    dag_id="youtube_ETA_pipeline",        # Unique DAG ID
    start_date=datetime(2025, 12, 12),  # Start date
    schedule_interval="@daily",    # Run daily
    catchup=False,                 # Do not backfill
    tags=["example"]
) as dag:
    
    # Task 1: Extract
    extract_task = DockerOperator(
        task_id="extract",
        image="extract:latest",          # Docker image named 'extract'
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",  # Use host Docker
        network_mode="bridge",
        mount = [f"{raw_data}:/app/output_data"]
    )

    # Task 2: Transform
    transform_task = DockerOperator(
        task_id="transform",
        image="transform:latest",       # Docker image named 'transform'
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount = [f"{raw_data}:/app/intput_data" ,
                 f"{processed_data}:/app/output_data"]
    )

    # Set dependency: transform runs after extract
    extract_task >> transform_task