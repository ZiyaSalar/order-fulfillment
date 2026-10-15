from airflow.sdk import dag, task
from airflow.providers.databricks.hooks.databricks import DatabricksHook
import pendulum


@dag(
    dag_id="test_cencora_databricks_connection",
    description="Read-only Databricks connection test.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["team3", "connection-test"],
)
def test_cencora_databricks_connection():

    @task
    def check_connection():
        hook = DatabricksHook(
            databricks_conn_id="cencora_databricks"
        )

        success, message = hook.test_connection()
        print(f"Databricks connection test: {message}")

        if not success:
            raise RuntimeError(
                f"Databricks authentication test failed: {message}"
            )

        print("Databricks connection verified successfully.")

    check_connection()


test_cencora_databricks_connection()
