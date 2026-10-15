
from airflow.sdk import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.databricks.hooks.databricks import DatabricksHook
import pendulum


@dag(
    dag_id="test_cencora_databricks_connection",
    description="Temporary, read-only test of the Databricks Airflow connection.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["team3", "connection-test"],
)
def test_cencora_databricks_connection():

    @task
    def check_connection():
        connection = BaseHook.get_connection("cencora_databricks")

        print(f"Connection found: {connection.conn_id}")
        print(f"Host configured: {bool(connection.host)}")
        print(f"Login configured: {bool(connection.login)}")
        print(f"Password configured: {bool(connection.password)}")

        hook = DatabricksHook(
            databricks_conn_id="cencora_databricks"
        )

        # Authenticate by making a read-only Databricks API request.
        workspace_client = hook.get_conn()
        workspace_client.jobs.list(limit=1)

        print("Databricks API request succeeded.")

    check_connection()


test_cencora_databricks_connection()
