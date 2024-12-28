# airflow-pipelines

We use Apache Airflow to demonstrate container orchestration for a bioinformatics pipeline using the **KubernetesPodOperator** model.

**NOTE**: This code is unfinished

## Process

1. Data is pulled from the **rarecompute/airflow-data** repository and placed into the **workspace** persistent volume
2. Data is processed using established variables in Apache Airflow
3. Artifacts are generated and stored in the workspace persistent volume and can be downloaded via SFTP
