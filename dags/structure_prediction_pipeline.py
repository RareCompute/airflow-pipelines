"""
Proof of Concept
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

PATHS_ENV = {
    "TARGET": Variable.get(
        "TARGET_PDB", default_var="/workspace/airflow/data/github/target.pdb"
    ),
    "QUERY_FASTA": Variable.get(
        "QUERY_FASTA",
        default_var="/workspace/airflow/data/pipeline/sequence/query.fasta",
    ),
    "RESULT_FASTA": Variable.get(
        "RESULT_FASTA",
        default_var="/workspace/airflow/data/pipeline/sequence/result.fasta",
    ),
    "SEARCH_DB": Variable.get(
        "SEARCH_DB", default_var="/workspace/mmseqs/database/gpu/uniref100_gpu"
    ),
    "TMP": Variable.get("TMP", default_var="/workspace/airflow/tmp"),
    "TARGET_GPU": Variable.get("TARGET_GPU", default_var="0"),
}

LIGANDMPNN_ENV = {
    "USE_MSA_SERVER": Variable.get("USE_MSA_SERVER", default_var="True"),
    "DEVICES_COUNT": Variable.get("DEVICES_COUNT", default_var="1"),
    "OVERRIDE": Variable.get("OVERRIDE", default_var="False"),
    "DATA_PATH": Variable.get("DATA_PATH", default_var="/workspace/boltz-1/output"),
    "MSA_SERVER_URL": Variable.get(
        "https://api.colabfold.com", default_var="https://api.colabfold.com"
    ),
}

# TODO: Pipeline environment
RFDIFFUSION_ENV = {}
MMSEQS2_ENV = {}
BOLTZ_ENV = {}
CHAI_ENV = {}

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

# TODO: Add resources
mmseqs_requests = k8s.V1ResourceRequirements(
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
            'if [ ! -d "/workspace/airflow/data" ]; then '
            f'  mkdir "/workspace/airflow/data; '
            "fi; "
            'if [ ! -d "/workspace/airflow/data/pipeline" ]; then '
            f'  mkdir -p "/workspace/airflow/data/pipeline/sequence; '
            "fi; "
            'if [ ! -d "/workspace/airflow/data/github" ]; then '
            f"  git clone {GIT_REPO_URL} /workspace/airflow/data/github; "
            "else "
            "  cd /workspace/airflow/data/github && git pull; "
            "fi; "
        ],
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    build_sequence = KubernetesPodOperator(
        task_id="build_sequence",
        name="build-sequence-ligandmpnn",
        namespace="machine-learning",
        image="ghcr.io/rarecompute/ligandmpnn:rolling",
        cmds=[PATHS_ENV["TARGET"], PATHS_ENV["QUERY_FASTA"]],
        env_vars=LIGANDMPNN_ENV,
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    query_sequence = KubernetesPodOperator(
        task_id="query_sequence",
        name="query-sequence-mmseqs2",
        namespace="machine-learning",
        image="ghcr.io/soedinglab/mmseqs2:latest",
        cmds=[
            "/usr/local/bin/mmseqs_sse41",
            "search",
            PATHS_ENV["QUERY_FASTA"],
            PATHS_ENV["SEARCH_DB"],
            PATHS_ENV["RESULT_FASTA"],
            PATHS_ENV["TMP"],
            PATHS_ENV["TARGET_GPU"],
        ],
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # TODO: Sequence alignment
    # align_sequence = KubernetesPodOperator()

    # TODO: Structure prediction
    # predict_structures = KubernetesPodOperator()
    finalize = KubernetesPodOperator(
        task_id="finalize",
        name="finalize",
        image="ubuntu:latest",
        cmds=["bash", "-c"],
        arguments=[
            'echo "Finished. See below for output.',
            "ls -lah",
            PATHS_ENV["RESULT_FASTA"],
        ],
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    pull_data >> build_sequence >> query_sequence >> finalize
