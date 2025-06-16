"""DAG to run our DBT project as a DAG."""

import logging
import pathlib
import shutil
from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import get_airflow_home
from airflow.models import Variable
from cosmos import (ExecutionConfig, ExecutionMode, ProfileConfig,
                    ProjectConfig, RenderConfig)
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import TestBehavior
from cosmos.operators import DbtDocsOperator
from cosmos.profiles import PostgresUserPasswordProfileMapping
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


POSTGRES_CONN = "postgres"
DBT_PROJECT_NAME = "dbt_tp"
DBT_ROOT_PATH = pathlib.Path(get_airflow_home()) / "dbt_tp"

DEFAULT_ARGS = {
    "owner": "utdt-td7",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 3),
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}


def copy_docs(project_dir: str):
    # copy from project_dir/target/index.html to DBT_ROOT_PATH/target/index.htmlif it exists
    target_path = DBT_ROOT_PATH / "target"
    target_path.mkdir(exist_ok=True)
    for file in ["index.html", "manifest.json", "graph.gpickle", "catalog.json"]:
        docs_path = pathlib.Path(project_dir) / "target" / file
        if docs_path.exists():
            shutil.move(docs_path, target_path / file)
        else:
            logger.info("%s was not found", docs_path)


with DAG(
    "run_dbt_voting",
    default_args=DEFAULT_ARGS,
    description='Ejecuta transformaciones dbt para el sistema de votación',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['voting', 'dbt'],
) as dag:

    # Tarea para ejecutar dbt deps
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='cd /opt/airflow/dbt_tp && dbt deps',
    )

    # Tarea para ejecutar dbt seed
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command='cd /opt/airflow/dbt_tp && dbt seed',
    )

    # Tarea para ejecutar dbt run para modelos de staging
    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command='cd /opt/airflow/dbt_tp && dbt run --select staging',
    )

    # Tarea para ejecutar dbt run para modelos intermedios
    dbt_run_intermediate = BashOperator(
        task_id='dbt_run_intermediate',
        bash_command='cd /opt/airflow/dbt_tp && dbt run --select intermediate',
    )

    # Tarea para ejecutar dbt run para modelos marts
    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command='cd /opt/airflow/dbt_tp && dbt run --select marts',
    )

    # Tarea para ejecutar dbt test
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt_tp && dbt test',
    )

    # Tarea para ejecutar dbt docs generate
    dbt_docs_generate = BashOperator(
        task_id='dbt_docs_generate',
        bash_command='cd /opt/airflow/dbt_tp && dbt docs generate',
    )

    # Tarea para ejecutar dbt docs serve (opcional, solo si quieres servir la documentación)
    dbt_docs_serve = BashOperator(
        task_id='dbt_docs_serve',
        bash_command='cd /opt/airflow/dbt_tp && dbt docs serve --port 8080',
    )

    # Definir el orden de las tareas
    dbt_deps >> dbt_seed >> dbt_run_staging >> dbt_run_intermediate >> dbt_run_marts >> dbt_test >> dbt_docs_generate >> dbt_docs_serve
