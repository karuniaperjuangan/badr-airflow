"""
## Astronaut ETL example DAG

This DAG queries the list of astronauts currently in space from the
Open Notify API and prints each astronaut's name and flying craft.

There are two tasks, one to get the data from the API and save the results,
and another to print the results. Both tasks are written in Python using
Airflow's TaskFlow API, which allows you to easily turn Python functions into
Airflow tasks, and automatically infer dependencies and pass data.

The second task uses dynamic task mapping to create a copy of the task for
each Astronaut in the list retrieved from the API. This list will change
depending on how many Astronauts are in space, and the DAG will adjust
accordingly each time it runs.

For more explanation and getting started instructions, see our Write your
first DAG tutorial: https://www.astronomer.io/docs/learn/get-started-with-airflow

![Picture of the ISS](https://www.esa.int/var/esa/storage/images/esa_multimedia/images/2010/02/space_station_over_earth/10293696-3-eng-GB/Space_Station_over_Earth_card_full.jpg)
"""

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests
from modules.load_db import mysql_to_clickhouse
from cosmos.profiles.clickhouse import ClickhouseUserPasswordProfileMapping
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
import os
from airflow.utils.task_group import TaskGroup

DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/modules/dbt/badr_interactive"
# The path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=ClickhouseUserPasswordProfileMapping(
        conn_id="clickhouse_default",
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"retries": 3},
    tags=["sql","clickhouse","badr-interactive"],
)
def elt_dag():
    mysql_config = {
        "host": os.getenv("MYSQL_HOST"),
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "database": os.getenv("MYSQL_DB"),
    }

    clickhouse_config = {
        "host": os.getenv("CLICKHOUSE_HOST"),
        "port": os.getenv("CLICKHOUSE_HTTP_PORT"),
        "user": os.getenv("CLICKHOUSE_USER"),
        "password": os.getenv("CLICKHOUSE_PASSWORD"), 
        "database": os.getenv("CLICKHOUSE_DB"),
    }

    tables_to_transfer = [
        "entity_has_master_materials",
        "master_materials",
        "stocks",
        "master_activities",
        "entity_entity_tags",
        "provinces",
        "regencies",
        "entities",
        "entity_tags",
    ]

    with TaskGroup("ingest_data_task") as ingest_data_task:
        task(mysql_to_clickhouse)(mysql_config, clickhouse_config, tables_to_transfer)

    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        default_args={"retries": 2},
    )

    ingest_data_task >> transform_data

# Instantiate the DAG
elt_dag()
