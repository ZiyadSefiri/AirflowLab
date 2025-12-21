from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime
from pathlib import Path

# Define paths relative to repo root
repo_root = Path(__file__).parent.parent
raw_data = repo_root/"Jobs"/"RawData"
processed_data = repo_root/"Jobs"/"ProcessedData"

with DAG(
    dag_id="youtube_ETA_pipeline",        # Unique DAG ID
    start_date=datetime(2025, 12, 12),    # Start date
    schedule="@daily",                     # Run daily
    catchup=False,                         # Do not backfill
    tags=["example"]
) as dag:
    
    # Task 1: Extract
    extract_task = DockerOperator(
        task_id="extract",
        image="extract:latest",          # Docker image
        auto_remove="success",           # Remove container if successful
        docker_url="unix://var/run/docker.sock",  # Use host Docker
        network_mode="bridge",
        mounts=[Mount(source="/home/ziyad/Dev/AirflowLab/Jobs/RawData", target="/app/output_data", type="bind")],
        do_xcom_push=False
    )

    # Task 2: Transform
    transform_task = DockerOperator(
        task_id="transform",
        image="transform:latest",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            Mount(source="/home/ziyad/Dev/AirflowLab/Jobs/RawData", target="/app/input_data", type="bind"),
            Mount(source="/home/ziyad/Dev/AirflowLab/Jobs/ProcessedData", target="/app/output_data", type="bind")
        ],
        mount_tmp_dir=False,
        do_xcom_push=False
        
    )

    analyse_task = DockerOperator(
        task_id="analyse",
        image="analyse:latest",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
         mounts=[
            Mount(source="/home/ziyad/Dev/AirflowLab/Jobs/ProcessedData", target="/app/input_data", type="bind"),
            Mount(source="/home/ziyad/Dev/AirflowLab/Jobs/AnalysisOutput", target="/app/output_data", type="bind")
        ],
        mount_tmp_dir=False,
        do_xcom_push=False
    )

    garbage_collector1 = DockerOperator(
        task_id="garbage_collector1",
        image="garbage_collector1:latest",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[ Mount(source="/home/ziyad/Dev/AirflowLab/Jobs/RawData", target="/app/raw_data", type="bind")],
        mount_tmp_dir=False,
        do_xcom_push=False

    )

    garbage_collector2 = DockerOperator(
        task_id="garbage_collector2",
        image="garbage_collector2:latest",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[ Mount(source="/home/ziyad/Dev/AirflowLab/Jobs/ProcessedData", target="/app/processed_data", type="bind")],
        mount_tmp_dir=False,
        do_xcom_push=False


    )

    

    # Set dependency: transform runs after extract
    extract_task >> transform_task >> [analyse_task,garbage_collector1]
    analyse_task >> garbage_collector2
