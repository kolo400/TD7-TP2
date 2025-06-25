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
    "run_dbt",
    default_args=DEFAULT_ARGS,
    schedule=None,  # TODO: complete aquí con lo que considere
    catchup=False,
    max_active_runs=1,
    tags=["dbt"],
):
    project_config = ProjectConfig(
        dbt_project_path=DBT_ROOT_PATH,
        project_name=DBT_PROJECT_NAME,
    )

    profile_config = ProfileConfig(
        profile_name="dbt_tp",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id=POSTGRES_CONN,
            profile_args={"dbname": "postgres", "schema": "public"},
        ),
    )

    dbt_task_group = DbtTaskGroup(
        group_id="dbt_task_group",
        profile_config=profile_config,
        project_config=project_config,
        execution_config=ExecutionConfig(execution_mode=ExecutionMode.LOCAL),
        render_config=RenderConfig(
            emit_datasets=False, test_behavior=TestBehavior.AFTER_EACH, dbt_deps=True
        ),
    )

    generate_dbt_docs = DbtDocsOperator(
        task_id="generate_dbt_docs",
        project_dir=project_config.dbt_project_path,
        profile_config=profile_config,
        callback=copy_docs,
    )

    dbt_task_group >> generate_dbt_docs