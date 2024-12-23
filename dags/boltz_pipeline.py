"""
Proof of Concept Boltz-1 container orchestration workflow
"""

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import (
    V1Affinity,
    V1NodeAffinity,
    V1NodeSelector,
    V1NodeSelectorRequirement,
    V1NodeSelectorTerm,
)
from kubernetes.client import models as k8s

ENV_VARS = {
    "USE_MSA_SERVER": Variable.get("USE_MSA_SERVER", default_var="True"),
    "DEVICES_COUNT": Variable.get("DEVICES_COUNT", default_var="1"),
    "OVERRIDE": Variable.get("OVERRIDE", default_var="False"),
    "DATA_PATH": Variable.get("DATA_PATH", default_var="/workspace/boltz-1/output"),
    "MSA_SERVER_URL": Variable.get(
        "https://api.colabfold.com", default_var="https://api.colabfold.com"
    ),
}


GIT_REPO_URL = Variable.get(
    "INPUT_GIT_REPO", "https://github.com/rarecompute/airflow-data.git"
)

GIT_TOKEN = Variable.get("PRIVATE_GIT_TOKEN", default_var="")

default_args = {
    "depends_on_past": False,
    "retries": 1,
}

pvc_volume = k8s.V1Volume(name="workspace", persistent_volume_claim="qlora-workspace")

pvc_volume_mount = k8s.V1VolumeMount(name="workspace", mount_path="/workspace")

job_requests = k8s.V1ResourceRequirements(
    requests={"memory": "16G", "cpu": "16", "nvidia.com/gpu": "1"},
    limits={"memory": "32G", "cpu": "32"},
)

affinity = V1Affinity(
    node_affinity=V1NodeAffinity(
        required_during_scheduling_ignored_during_execution=V1NodeSelector(
            node_selector_terms=[
                V1NodeSelectorTerm(
                    match_expressions=[
                        V1NodeSelectorRequirement(
                            key="pool", operator="In", values="application"
                        )
                    ]
                )
            ]
        )
    )
)

with DAG(
    dag_id="boltz_pipeline",
    default_args=default_args,
    description="Proof of concept Boltz-1 container orchestration workflow",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    pull_data = KubernetesPodOperator(
        task_id="pull_data",
        name="pull-data",
        image="alpine/git",
        cmds=["sh", "-c"],
        arguments=[
            # If the repo exists already, pull and don't clone
            'if [ ! -d "/workspace/boltz-1" ]; then '
            f'  mkdir "/workspace/boltz-1; '
            "fi; "
            'if [ ! -d "/workspace/boltz-1/data" ]; then '
            f"  git clone {GIT_REPO_URL} /workspace/boltz-1/data; "
            "else "
            "  cd /workspace/boltz-1/data && git pull; "
            "fi; "
            "ls -lah /workspace/boltz-1/data"
        ],
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    process_data = KubernetesPodOperator(
        task_id="process_data",
        name="boltz-1-task",
        image="ghcr.io/rarecompute/boltz:rolling",
        cmds=["/workspace/boltz-1/data"],
        env_vars=ENV_VARS,
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    finalize = KubernetesPodOperator(
        task_id="finalize",
        name="finalize",
        image="ubuntu:latest",
        cmds=["bash", "-c"],
        arguments=[
            'echo "Finished. See below for output.',
            "ls -lah /workspace/boltz-1/output",
        ],
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    pull_data >> process_data >> finalize
