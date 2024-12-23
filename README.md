# airflow-pipelines

We use Apache Airflow to demonstrate container orchestration for bioinformatics pipelines using the **KubernetesPodOperator** model.

**NOTE**
This code is untested and needs work! Please contribute; this is just a tiny Proof of Concept.

## Process

![Workflow](https://i.ibb.co/7bYVc5w/image.png)

## Boltz-1

The workflow for Boltz-1 is split into three parts:

1. Data is pulled from the **rarecompute/airflow-data** repository and placed into the **workspace** persistent volume
2. Data is processed using established variables in Apache Airflow
3. Artifacts are generated and stored in the workspace persistent volume and can be downloaded via SFTP
