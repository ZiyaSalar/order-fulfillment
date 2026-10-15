from datetime import timedelta

import pendulum
from airflow.sdk import dag
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


# Existing Databricks jobs
BRONZE_JOB_ID = 752292528739831
SILVER_JOB_ID = 539746400368695
GOLD_JOB_ID = 119334250861014

DATABRICKS_CONN_ID = "cencora_databricks"


@dag(
    dag_id="cencora_team_3_order_fulfilment",
    description="Orchestrates the Bronze, Silver, and Gold Databricks jobs.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "team_3",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
    },
    tags=["team3", "databricks", "order-fulfilment"],
)
def cencora_team_3_order_fulfilment():

    bronze_ingestion = DatabricksRunNowOperator(
        task_id="bronze_ingestion",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=BRONZE_JOB_ID,
    )

    silver_validation = DatabricksRunNowOperator(
        task_id="silver_validation",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=SILVER_JOB_ID,
    )

    gold_processing = DatabricksRunNowOperator(
        task_id="gold_processing",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=GOLD_JOB_ID,
    )

    bronze_ingestion >> silver_validation >> gold_processing


cencora_team_3_order_fulfilment()
