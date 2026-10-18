import re
import pendulum

from airflow.sdk import dag, task
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


ADLS_CONN_ID = "cencora_adls"

CONTAINER_NAME = "team3"

LANDING_PREFIX = "landing/"

EXPECTED_FILES = {
    "orders.csv",
    "order_lines.csv",
    "shipment_lines.csv",
    "delivery_events.csv",
}

BATCH_PATTERN = re.compile(
    r"^landing/(batch_(\d{3}))/"
)


@dag(
    dag_id="test_team3_dynamic_batch_discovery",
    description="Detects the latest complete batch_NNN folder in Team 3 ADLS.",
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),
    schedule=None,
    catchup=False,
    tags=[
        "team3",
        "adls",
        "batch-discovery",
    ],
)
def test_team3_dynamic_batch_discovery():

    @task
    def discover_latest_complete_batch():

        hook = WasbHook(
            wasb_conn_id=ADLS_CONN_ID
        )

        print("=" * 70)
        print("TEAM 3 - DYNAMIC BATCH DISCOVERY")
        print("=" * 70)

        print(f"Container : {CONTAINER_NAME}")
        print(f"Prefix    : {LANDING_PREFIX}")

        # --------------------------------------------------------
        # 1. List everything under landing/
        # --------------------------------------------------------

        blobs = hook.get_blobs_list(
            container_name=CONTAINER_NAME,
            prefix=LANDING_PREFIX,
        )

        if not blobs:
            raise ValueError(
                f"No files found under "
                f"{CONTAINER_NAME}/{LANDING_PREFIX}"
            )

        print()
        print(f"Total blob entries found: {len(blobs)}")

        print()
        print("RAW BLOB NAMES RETURNED BY WASB:")
        print("=" * 70)

        for blob_name in blobs:
            print(repr(blob_name))

        print("=" * 70)

        # --------------------------------------------------------
        # 2. Group files by batch_NNN
        # --------------------------------------------------------

        batches = {}

        for blob_name in blobs:

            match = BATCH_PATTERN.match(blob_name)

            if not match:
                continue

            batch_id = match.group(1)
            batch_number = int(match.group(2))

            relative_file = blob_name[
                len(f"{LANDING_PREFIX}{batch_id}/"):
            ]

            # Ignore fake directory entries if any
            if not relative_file:
                continue

            batches.setdefault(
                batch_id,
                {
                    "number": batch_number,
                    "files": set(),
                },
            )

            batches[batch_id]["files"].add(
                relative_file
            )

        if not batches:
            raise ValueError(
                "No valid batch_NNN folders were found."
            )

        print()
        print("Detected batches:")

        for batch_id in sorted(
            batches,
            key=lambda b: batches[b]["number"],
        ):
            print(
                f"{batch_id}: "
                f"{sorted(batches[batch_id]['files'])}"
            )

        # --------------------------------------------------------
        # 3. Determine which batches are COMPLETE
        # --------------------------------------------------------

        complete_batches = []

        print()
        print("=" * 70)
        print("BATCH COMPLETENESS CHECK")
        print("=" * 70)

        for batch_id, info in batches.items():

            actual_files = {
                file_name.split("/")[-1]
                for file_name in info["files"]
            }

            missing_files = (
                EXPECTED_FILES - actual_files
            )

            if not missing_files:

                print(
                    f"{batch_id}: COMPLETE"
                )

                complete_batches.append(
                    (
                        info["number"],
                        batch_id,
                    )
                )

            else:

                print(
                    f"{batch_id}: INCOMPLETE"
                )

                print(
                    f"  Missing: "
                    f"{sorted(missing_files)}"
                )

        if not complete_batches:
            raise ValueError(
                "No complete batch_NNN folders were found."
            )

        # --------------------------------------------------------
        # 4. Select numerically latest COMPLETE batch
        # --------------------------------------------------------

        complete_batches.sort(
            key=lambda item: item[0]
        )

        latest_number, latest_batch = (
            complete_batches[-1]
        )

        batch_prefix = (
            f"{LANDING_PREFIX}"
            f"{latest_batch}/"
        )

        print()
        print("=" * 70)
        print("LATEST COMPLETE BATCH")
        print("=" * 70)

        print(
            f"Batch ID     : {latest_batch}"
        )

        print(
            f"Batch number : {latest_number}"
        )

        print(
            f"Batch prefix : {batch_prefix}"
        )

        print()
        print(
            "DYNAMIC BATCH DISCOVERY PASSED."
        )

        print("=" * 70)

        # Small metadata is safe for XCom.
        return {
            "batch_id": latest_batch,
            "batch_number": latest_number,
            "batch_prefix": batch_prefix,
        }

    discover_latest_complete_batch()


test_team3_dynamic_batch_discovery()