FROM apache/airflow:3.1.3

# Install the Docker provider for Airflow
RUN pip install apache-airflow-providers-docker

