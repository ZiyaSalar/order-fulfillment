from datetime import timedelta
import re

import pendulum

from airflow.sdk import (
    dag,
    task,
    Variable,
)
from airflow.sdk.exceptions import AirflowSkipException

from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)
from include.new_relic_telemetry import (
    new_relic_task_retry,
    new_relic_task_failure,
    new_relic_dag_failure,
    new_relic_pipeline_success,
)


# ============================================================
# TEAM 3 - ORDER FULFILMENT AIRFLOW ORCHESTRATION
#
# Flow:
#
# ADLS landing/
#       ↓
# Discover highest batch_NNN
#       ↓
# Verify all 4 required CSV files
#       ↓
# Compare with team3_last_successful_batch
#       ↓
# New batch?
#       ↓
# Bronze
#       ↓
# Silver
#       ↓
# Gold
#       ↓
# Mark batch successful
#
# ============================================================


# ------------------------------------------------------------
# 1. CONNECTIONS
# ------------------------------------------------------------

ADLS_CONN_ID = "cencora_adls"

DATABRICKS_CONN_ID = "cencora_databricks"


# ------------------------------------------------------------
# 2. ADLS CONFIGURATION
# ------------------------------------------------------------

CONTAINER_NAME = "team3"

LANDING_PREFIX = "landing/"


# Every NEW batch from batch_009 onwards must contain
# all 4 transactional CSV files.

REQUIRED_FILES = {
    "orders.csv",
    "order_lines.csv",
    "shipment_lines.csv",
    "delivery_events.csv",
}


# Only folders exactly matching batch_NNN are considered.

BATCH_PATTERN = re.compile(
    r"^landing/(batch_(\d{3}))/$"
)


# ------------------------------------------------------------
# 3. AIRFLOW STATE
# ------------------------------------------------------------

LAST_SUCCESSFUL_BATCH_VARIABLE = (
    "team3_last_successful_batch"
)


# ------------------------------------------------------------
# 4. DATABRICKS JOB IDs
# ------------------------------------------------------------

BRONZE_JOB_ID = 752292528739831

SILVER_JOB_ID = 539746400368695

GOLD_JOB_ID = 119334250861014


# ------------------------------------------------------------
# 5. DAG
# ------------------------------------------------------------

@dag(
    dag_id="cencora_team_3_order_fulfilment",
    description=(
        "Automatically discovers a new Team 3 ADLS batch "
        "and orchestrates Bronze, Silver, and Gold "
        "Databricks jobs."
    ),

    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),

    # --------------------------------------------------------
    # IMPORTANT:
    #
    # Keep manual during the first integrated test.
    #
    # After batch_009 end-to-end validation succeeds,
    # change to:
    #
    # schedule="*/5 * * * *"
    #
    # --------------------------------------------------------

    schedule=None,

    catchup=False,

    max_active_runs=1,

    default_args={
        "owner": "team_3",
        "retries": 2,
        "retry_delay": timedelta(
            minutes=5
        ),
        "execution_timeout": timedelta(
            hours=2
        ),
        "on_retry_callback": new_relic_task_retry,
        "on_failure_callback": new_relic_task_failure,
    },

    on_failure_callback=new_relic_dag_failure,
    on_success_callback=new_relic_pipeline_success,
    tags=[
        "team3",
        "adls",
        "databricks",
        "order-fulfilment",
        "incremental",
    ],
)
def cencora_team_3_order_fulfilment():

    # ========================================================
    # TASK 1
    #
    # Discover the latest NEW complete batch.
    # ========================================================

    @task
    def discover_new_batch():

        hook = WasbHook(
            wasb_conn_id=ADLS_CONN_ID
        )

        print("=" * 70)
        print("TEAM 3 - ADLS BATCH DISCOVERY")
        print("=" * 70)

        print(
            f"Container : {CONTAINER_NAME}"
        )

        print(
            f"Landing   : {LANDING_PREFIX}"
        )


        # ----------------------------------------------------
        # A. List top-level entries under landing/
        # ----------------------------------------------------

        landing_entries = (
            hook.get_blobs_list(
                container_name=CONTAINER_NAME,
                prefix=LANDING_PREFIX,
            )
        )


        if not landing_entries:

            raise AirflowSkipException(
                "No entries found under "
                f"{CONTAINER_NAME}/"
                f"{LANDING_PREFIX}"
            )


        print()
        print(
            "Entries found under landing/:"
        )


        for entry in landing_entries:

            print(
                f" - {entry}"
            )


        # ----------------------------------------------------
        # B. Extract batch_NNN directories
        # ----------------------------------------------------

        discovered_batches = []


        for entry in landing_entries:

            match = BATCH_PATTERN.fullmatch(
                entry
            )


            if not match:

                print(
                    "Ignoring non-batch entry:",
                    entry,
                )

                continue


            batch_id = (
                match.group(1)
            )

            batch_number = int(
                match.group(2)
            )


            discovered_batches.append(
                {
                    "batch_id": batch_id,
                    "batch_number": batch_number,
                }
            )


        if not discovered_batches:

            raise AirflowSkipException(
                "No batch_NNN folders "
                "were found in ADLS."
            )


        # Highest numbered directory first.

        discovered_batches.sort(
            key=lambda batch: (
                batch["batch_number"]
            ),
            reverse=True,
        )


        print()
        print("Detected batches:")


        for batch in discovered_batches:

            print(
                f" - {batch['batch_id']}"
            )


        # ----------------------------------------------------
        # C. Read Airflow processing state
        # ----------------------------------------------------

        last_successful_batch = (
            Variable.get(
                LAST_SUCCESSFUL_BATCH_VARIABLE,
                default=None,
            )
        )


        if not last_successful_batch:

            raise ValueError(
                "Airflow Variable "
                "'team3_last_successful_batch' "
                "has not been configured."
            )


        last_match = re.fullmatch(
            r"batch_(\d{3})",
            last_successful_batch,
        )


        if not last_match:

            raise ValueError(
                "Invalid value in "
                "'team3_last_successful_batch': "
                f"{last_successful_batch!r}. "
                "Expected batch_NNN."
            )


        last_successful_number = int(
            last_match.group(1)
        )


        print()
        print(
            "Last successful batch:",
            last_successful_batch,
        )


        # ----------------------------------------------------
        # D. Find NEW batches only
        #
        # batch_008 is our current processing boundary.
        #
        # Therefore batch_001 -> batch_008 are ignored.
        # ----------------------------------------------------

        new_batches = [

            batch

            for batch
            in discovered_batches

            if (
                batch["batch_number"]
                > last_successful_number
            )
        ]


        if not new_batches:

            print()
            print(
                "No new ADLS batch available."
            )

            print(
                "Latest successfully processed batch:",
                last_successful_batch,
            )

            raise AirflowSkipException(
                "No new ADLS batch available."
            )


        # ----------------------------------------------------
        # E. Select newest candidate
        # ----------------------------------------------------

        candidate = new_batches[0]

        batch_id = (
            candidate["batch_id"]
        )

        batch_number = (
            candidate["batch_number"]
        )

        batch_prefix = (
            f"{LANDING_PREFIX}"
            f"{batch_id}/"
        )


        print()
        print("=" * 70)
        print("NEW BATCH CANDIDATE")
        print("=" * 70)

        print(
            "Batch ID     :",
            batch_id,
        )

        print(
            "Batch number :",
            batch_number,
        )

        print(
            "Batch prefix :",
            batch_prefix,
        )


        # ----------------------------------------------------
        # F. List files inside candidate batch
        # ----------------------------------------------------

        batch_entries = (
            hook.get_blobs_list(
                container_name=CONTAINER_NAME,
                prefix=batch_prefix,
            )
        )


        actual_files = set()


        for entry in batch_entries:

            # WASB may return the directory marker.
            if entry == batch_prefix:

                continue


            if not entry.startswith(
                batch_prefix
            ):

                continue


            relative_name = entry[
                len(batch_prefix):
            ]


            # Only direct files are part of this batch.
            if (
                relative_name
                and "/"
                not in relative_name
            ):

                actual_files.add(
                    relative_name
                )


        print()
        print(
            "Files discovered:"
        )


        for file_name in sorted(
            actual_files
        ):

            print(
                f" - {file_name}"
            )


        # ----------------------------------------------------
        # G. Validate the 4-file batch contract
        # ----------------------------------------------------

        missing_files = (
            REQUIRED_FILES
            - actual_files
        )


        if missing_files:

            # IMPORTANT:
            #
            # Do NOT mark the batch successful.
            # Do NOT start Databricks.
            #
            # A later DAG run can check again once
            # remaining files have landed.

            print()
            print(
                f"{batch_id} is not ready."
            )

            print(
                "Missing required files:",
                sorted(
                    missing_files
                ),
            )

            raise AirflowSkipException(
                f"{batch_id} is not ready. "
                "Missing: "
                f"{sorted(missing_files)}"
            )


        print()
        print(
            "All required files are present."
        )

        print(
            f"NEW PROCESSABLE BATCH: "
            f"{batch_id}"
        )

        print("=" * 70)


        # ----------------------------------------------------
        # H. Return small metadata through XCom
        # ----------------------------------------------------

        return {
            "batch_id": batch_id,
            "batch_number": batch_number,
            "batch_prefix": batch_prefix,
            "files": sorted(
                actual_files
            ),
        }


    # ========================================================
    # CREATE DISCOVERY TASK
    # ========================================================

    batch = discover_new_batch()


    # ========================================================
    # TASK 2
    #
    # BRONZE
    #
    # Airflow explicitly tells Bronze which batch to process.
    # ========================================================

    bronze_ingestion = (
        DatabricksRunNowOperator(
            task_id="bronze_ingestion",

            databricks_conn_id=(
                DATABRICKS_CONN_ID
            ),

            job_id=BRONZE_JOB_ID,
            do_xcom_push=True,

            notebook_params={
                "input_batch": (
                    "{{ ti.xcom_pull("
                    "task_ids='discover_new_batch'"
                    ")['batch_id'] }}"
                ),
            },
        )
    )


    # ========================================================
    # TASK 3
    #
    # SILVER
    #
    # Receives exactly the SAME batch as Bronze.
    # ========================================================

    silver_validation = (
        DatabricksRunNowOperator(
            task_id="silver_validation",

            databricks_conn_id=(
                DATABRICKS_CONN_ID
            ),

            job_id=SILVER_JOB_ID,
            do_xcom_push=True,

            notebook_params={
                "catalog": (
                    "dbwteam3"
                ),

                "schema": (
                    "order_fulfillment"
                ),

                "input_batch": (
                    "{{ ti.xcom_pull("
                    "task_ids='discover_new_batch'"
                    ")['batch_id'] }}"
                ),
            },
        )
    )


    # ========================================================
    # TASK 4
    #
    # GOLD
    #
    # Receives:
    #
    # - same input_batch
    # - Airflow-controlled dynamic as_of_time
    #
    # The old hardcoded:
    #
    # 2026-10-01T00:00:00
    #
    # is no longer used.
    # ========================================================

    gold_processing = (
        DatabricksRunNowOperator(
            task_id="gold_processing",

            databricks_conn_id=(
                DATABRICKS_CONN_ID
            ),

            job_id=GOLD_JOB_ID,
            do_xcom_push=True,

            notebook_params={
                "catalog": (
                    "dbwteam3"
                ),

                "schema": (
                    "order_fulfillment"
                ),

                "input_batch": (
                    "{{ ti.xcom_pull("
                    "task_ids='discover_new_batch'"
                    ")['batch_id'] }}"
                ),

                "as_of_time": (
                    "{{ data_interval_end.isoformat() }}"
                ),
            },
        )
    )


    # ========================================================
    # TASK 5
    #
    # MARK BATCH SUCCESSFUL
    #
    # This task runs ONLY after Gold succeeds.
    #
    # Therefore:
    #
    # Bronze fails
    #     → variable NOT updated
    #
    # Silver fails
    #     → variable NOT updated
    #
    # Gold fails
    #     → variable NOT updated
    #
    # Gold succeeds
    #     → variable updated
    #
    # ========================================================

    @task
    def mark_batch_successful(
        batch_info
    ):

        batch_id = (
            batch_info[
                "batch_id"
            ]
        )


        print("=" * 70)
        print("MARKING BATCH SUCCESSFUL")
        print("=" * 70)


        print(
            "Completed batch:",
            batch_id,
        )


        previous_batch = (
            Variable.get(
                LAST_SUCCESSFUL_BATCH_VARIABLE,
                default=None,
            )
        )


        print(
            "Previous successful batch:",
            previous_batch,
        )


        Variable.set(
            LAST_SUCCESSFUL_BATCH_VARIABLE,
            batch_id,
        )


        print(
            "New successful batch:",
            batch_id,
        )

        print()
        print(
            "Airflow Variable updated:"
        )

        print(
            f"{LAST_SUCCESSFUL_BATCH_VARIABLE}"
            f" = {batch_id}"
        )

        print("=" * 70)


        return batch_id


    batch_completed = (
        mark_batch_successful(
            batch
        )
    )


    # ========================================================
    # TASK DEPENDENCIES
    # ========================================================

    (
        batch
        >> bronze_ingestion
        >> silver_validation
        >> gold_processing
        >> batch_completed
    )


cencora_team_3_order_fulfilment()