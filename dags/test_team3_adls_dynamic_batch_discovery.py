import re
import pendulum

from airflow.sdk import dag, task
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


ADLS_CONN_ID = "cencora_adls"

CONTAINER_NAME = "team3"

LANDING_PREFIX = "landing/"

SUPPORTED_FILES = {
    "orders.csv",
    "order_lines.csv",
    "shipment_lines.csv",
    "delivery_events.csv",
}

FULL_BATCH_FILES = {
    "orders.csv",
    "order_lines.csv",
    "shipment_lines.csv",
    "delivery_events.csv",
}

DELIVERY_ONLY_FILES = {
    "delivery_events.csv",
}

BATCH_PATTERN = re.compile(
    r"^landing/(batch_(\d{3}))/$"
)


@dag(
    dag_id="test_team3_dynamic_batch_discovery",
    description=(
        "Detects the latest valid batch_NNN folder "
        "in Team 3 ADLS."
    ),
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
    def discover_latest_valid_batch():

        hook = WasbHook(
            wasb_conn_id=ADLS_CONN_ID
        )

        print("=" * 70)
        print("TEAM 3 - DYNAMIC BATCH DISCOVERY")
        print("=" * 70)

        # --------------------------------------------------------
        # 1. Discover folders under landing/
        # --------------------------------------------------------

        landing_entries = hook.get_blobs_list(
            container_name=CONTAINER_NAME,
            prefix=LANDING_PREFIX,
        )

        if not landing_entries:
            raise ValueError(
                "No entries found under landing/."
            )

        print()
        print("Entries under landing/:")

        for entry in landing_entries:
            print(entry)

        # --------------------------------------------------------
        # 2. Identify only batch_NNN folders
        # --------------------------------------------------------

        batches = []

        for entry in landing_entries:

            match = BATCH_PATTERN.match(entry)

            if not match:
                print(
                    f"Ignoring non-batch entry: {entry}"
                )
                continue

            batch_id = match.group(1)
            batch_number = int(match.group(2))

            batches.append(
                (
                    batch_number,
                    batch_id,
                )
            )

        if not batches:
            raise ValueError(
                "No valid batch_NNN directories found."
            )

        batches.sort(
            key=lambda x: x[0]
        )

        print()
        print("Detected batch folders:")

        for batch_number, batch_id in batches:
            print(
                f"{batch_id} "
                f"(number={batch_number})"
            )

        # --------------------------------------------------------
        # 3. Inspect files INSIDE every batch
        # --------------------------------------------------------

        valid_batches = []

        print()
        print("=" * 70)
        print("BATCH VALIDATION")
        print("=" * 70)

        for batch_number, batch_id in batches:

            batch_prefix = (
                f"{LANDING_PREFIX}"
                f"{batch_id}/"
            )

            batch_entries = hook.get_blobs_list(
                container_name=CONTAINER_NAME,
                prefix=batch_prefix,
            )

            actual_files = set()

            for entry in batch_entries:

                # Ignore the directory marker itself
                if entry == batch_prefix:
                    continue

                relative_name = entry[
                    len(batch_prefix):
                ]

                # Only consider direct CSV files
                if (
                    relative_name
                    and "/" not in relative_name
                ):
                    actual_files.add(
                        relative_name
                    )

            print()
            print(f"{batch_id}:")
            print(
                f"  Files: "
                f"{sorted(actual_files)}"
            )

            # ----------------------------------------------------
            # Classify the batch
            # ----------------------------------------------------

            if FULL_BATCH_FILES.issubset(
                actual_files
            ):

                batch_type = "FULL"

                print(
                    "  Type : FULL"
                )

            elif DELIVERY_ONLY_FILES.issubset(
                actual_files
            ):

                batch_type = "DELIVERY_ONLY"

                print(
                    "  Type : DELIVERY_ONLY"
                )

            else:

                print(
                    "  Type : INVALID"
                )

                continue

            unsupported_files = (
                actual_files
                - SUPPORTED_FILES
            )

            if unsupported_files:

                print(
                    "  WARNING - additional files: "
                    f"{sorted(unsupported_files)}"
                )

            valid_batches.append(
                {
                    "batch_number": batch_number,
                    "batch_id": batch_id,
                    "batch_type": batch_type,
                    "batch_prefix": batch_prefix,
                    "files": sorted(
                        actual_files
                    ),
                }
            )

        # --------------------------------------------------------
        # 4. Ensure at least one valid batch exists
        # --------------------------------------------------------

        if not valid_batches:

            raise ValueError(
                "No valid processable batch_NNN "
                "folders were found."
            )

        # --------------------------------------------------------
        # 5. Select highest numbered valid batch
        # --------------------------------------------------------

        latest_batch = max(
            valid_batches,
            key=lambda x: x["batch_number"],
        )

        print()
        print("=" * 70)
        print("LATEST VALID BATCH")
        print("=" * 70)

        print(
            f"Batch ID     : "
            f"{latest_batch['batch_id']}"
        )

        print(
            f"Batch number : "
            f"{latest_batch['batch_number']}"
        )

        print(
            f"Batch type   : "
            f"{latest_batch['batch_type']}"
        )

        print(
            f"Batch prefix : "
            f"{latest_batch['batch_prefix']}"
        )

        print(
            f"Files        : "
            f"{latest_batch['files']}"
        )

        print()
        print(
            "DYNAMIC BATCH DISCOVERY PASSED."
        )

        print("=" * 70)

        # Small metadata only through XCom
        return latest_batch

    discover_latest_valid_batch()


test_team3_dynamic_batch_discovery()