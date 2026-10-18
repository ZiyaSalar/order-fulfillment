import pendulum

from airflow.sdk import dag, task
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


ADLS_CONN_ID = "cencora_adls"
CONTAINER_NAME = "team3"
TEST_PREFIX = "landing/batch_008/"


@dag(
    dag_id="test_team3_adls_connection",
    description="Tests Astro workload identity access to Team 3 ADLS.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["team3", "adls", "connection-test"],
)
def test_team3_adls_connection():

    @task
    def list_batch_008_files():
        hook = WasbHook(
            wasb_conn_id=ADLS_CONN_ID
        )

        blobs = hook.get_blobs_list(
            container_name=CONTAINER_NAME,
            prefix=TEST_PREFIX,
        )

        print("=" * 70)
        print("TEAM 3 ADLS CONNECTION TEST")
        print("=" * 70)
        print(f"Container : {CONTAINER_NAME}")
        print(f"Prefix    : {TEST_PREFIX}")
        print()
        print("Files found:")

        for blob in blobs:
            print(blob)

        if not blobs:
            raise ValueError(
                f"No files found under "
                f"{CONTAINER_NAME}/{TEST_PREFIX}"
            )

        expected_files = {
            f"{TEST_PREFIX}orders.csv",
            f"{TEST_PREFIX}order_lines.csv",
            f"{TEST_PREFIX}shipment_lines.csv",
            f"{TEST_PREFIX}delivery_events.csv",
        }

        actual_files = set(blobs)

        missing_files = expected_files - actual_files

        if missing_files:
            raise ValueError(
                "ADLS connection worked, but required files "
                f"are missing: {sorted(missing_files)}"
            )

        print()
        print("All 4 required files were found successfully.")
        print("ASTRO -> ADLS CONNECTION TEST PASSED.")
        print("=" * 70)

    list_batch_008_files()


test_team3_adls_connection()