"""
Reusable New Relic callbacks for the Team 3 Airflow DAG.

No credentials are stored in source.

Required environment variables:
    NEW_RELIC_LICENSE_KEY
        Secret New Relic INGEST-LICENSE key

    NEW_RELIC_EVENT_URL
        Full New Relic Event API endpoint

Optional environment variables:
    NEW_RELIC_EVENT_TYPE
        Default: Team3AirflowOrchestration

    NEW_RELIC_TEAM
        Default: Team 3

    NEW_RELIC_PIPELINE
        Default: order_fulfilment

    NEW_RELIC_ENVIRONMENT
        Default: training
"""

from __future__ import annotations

import json
import logging
import os
import re

from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


# ============================================================
# LOGGING
# ============================================================

LOG = logging.getLogger(__name__)


# ============================================================
# NEW RELIC CONFIGURATION
# ============================================================

EVENT_TYPE = os.getenv(
    "NEW_RELIC_EVENT_TYPE",
    "Team3AirflowOrchestration",
)

TEAM = os.getenv(
    "NEW_RELIC_TEAM",
    "Team 3",
)

PIPELINE = os.getenv(
    "NEW_RELIC_PIPELINE",
    "order_fulfilment",
)

ENVIRONMENT = os.getenv(
    "NEW_RELIC_ENVIRONMENT",
    "training",
)

DEPLOYMENT = "team3-order-fulfilment"

CONNECT_AND_READ_TIMEOUT_SECONDS = 10

MAX_ERROR_LENGTH = 500


# ============================================================
# SECRET / ERROR SANITISATION
# ============================================================

_SECRET_PATTERN = re.compile(
    r"(?i)"
    r"(api[-_ ]?key|license[-_ ]?key|authorization|"
    r"bearer|token|secret|password)"
    r"\s*[:=]\s*[^\s,;]+"
)


def _safe_text(
    value: Any,
    limit: int = MAX_ERROR_LENGTH,
) -> str:
    """
    Return bounded, single-line text and redact obvious
    credential-like values.
    """

    if value is None:
        return ""

    text = (
        str(value)
        .replace("\n", " ")
        .replace("\r", " ")
    )

    text = _SECRET_PATTERN.sub(
        r"\1=[REDACTED]",
        text,
    )

    return text[:limit]


# ============================================================
# TIME UTILITIES
# ============================================================

def _iso(
    value: Any,
) -> Optional[str]:

    if value is None:
        return None

    if hasattr(
        value,
        "isoformat",
    ):
        return value.isoformat()

    return str(value)


def _duration_seconds(
    start: Any,
    end: Any = None,
) -> Optional[float]:

    if start is None:
        return None

    effective_end = (
        end
        or datetime.now(
            timezone.utc
        )
    )

    try:

        seconds = (
            effective_end
            - start
        ).total_seconds()

        return round(
            max(
                0.0,
                float(seconds),
            ),
            3,
        )

    except Exception:

        return None


# ============================================================
# XCOM UTILITIES
# ============================================================

def _xcom_pull(
    ti: Any,
    task_id: str,
    key: Optional[str] = None,
) -> Any:
    """
    Safely retrieve a small XCom value.

    Telemetry failure must never affect the actual pipeline.
    """

    if ti is None:
        return None

    try:

        kwargs: Dict[str, Any] = {
            "task_ids": task_id
        }

        if key is not None:
            kwargs["key"] = key

        return ti.xcom_pull(
            **kwargs
        )

    except Exception as exc:

        LOG.warning(
            "Telemetry could not read XCom "
            "for task %s: %s",
            task_id,
            _safe_text(exc),
        )

        return None


# ============================================================
# BATCH CORRELATION
# ============================================================

def _batch_id(
    ti: Any,
) -> Optional[str]:
    """
    Retrieve batch_NNN returned by discover_new_batch.

    A no-new-batch run legitimately returns no batch ID.
    """

    value = _xcom_pull(
        ti,
        "discover_new_batch",
    )

    if isinstance(
        value,
        dict,
    ):

        batch = value.get(
            "batch_id"
        )

        return (
            str(batch)
            if batch
            else None
        )

    return None


# ============================================================
# DATABRICKS RUN CORRELATION
# ============================================================

def _databricks_run_id(
    ti: Any,
    task_id: Optional[str],
) -> Optional[str]:
    """
    Retrieve a Databricks run ID from the operator's XCom values
    when the provider exposes one.

    Missing values are omitted rather than fabricated.
    """

    if (
        ti is None
        or not task_id
    ):
        return None

    # --------------------------------------------------------
    # Try known explicit XCom keys first.
    # --------------------------------------------------------

    for key in (
        "run_id",
        "databricks_run_id",
    ):

        value = _xcom_pull(
            ti,
            task_id,
            key=key,
        )

        if value not in (
            None,
            "",
        ):
            return str(value)

    # --------------------------------------------------------
    # Fall back to task return value.
    # --------------------------------------------------------

    value = _xcom_pull(
        ti,
        task_id,
    )

    if isinstance(
        value,
        dict,
    ):

        for key in (
            "run_id",
            "databricks_run_id",
        ):

            if value.get(
                key
            ) not in (
                None,
                "",
            ):

                return str(
                    value[key]
                )

    elif (
        isinstance(
            value,
            (
                str,
                int,
            ),
        )
        and str(value).isdigit()
    ):

        return str(value)

    return None


# ============================================================
# BASE EVENT BUILDER
# ============================================================

def _base_event(
    context: Dict[str, Any],
    event_name: str,
    status: str,
    scope: str,
) -> Dict[str, Any]:
    """
    Build common Airflow → New Relic correlation attributes.
    """

    ti = (
        context.get(
            "task_instance"
        )
        or context.get(
            "ti"
        )
    )

    dag_run = context.get(
        "dag_run"
    )

    task = context.get(
        "task"
    )

    dag_id = (
        getattr(
            dag_run,
            "dag_id",
            None,
        )
        or getattr(
            task,
            "dag_id",
            None,
        )
    )

    run_id = getattr(
        dag_run,
        "run_id",
        None,
    )

    task_id = (
        getattr(
            ti,
            "task_id",
            None,
        )
        or getattr(
            task,
            "task_id",
            None,
        )
    )

    batch_id = _batch_id(
        ti
    )

    event: Dict[str, Any] = {

        "eventType":
            EVENT_TYPE,

        "eventName":
            event_name,

        "scope":
            scope,

        "team":
            TEAM,

        "pipeline":
            PIPELINE,

        "environment":
            ENVIRONMENT,

        "deployment":
            DEPLOYMENT,

        "dagId":
            dag_id,

        "taskId":
            task_id,

        "airflowRunId":
            run_id,

        "batchId":
            batch_id,

        "inputBatch":
            batch_id,

        "status":
            status,

        "observedAt":
            datetime.now(
                timezone.utc
            ).isoformat(),
    }

    # --------------------------------------------------------
    # Retry / try information
    # --------------------------------------------------------

    try_number = getattr(
        ti,
        "try_number",
        None,
    )

    max_tries = getattr(
        ti,
        "max_tries",
        None,
    )

    if try_number is not None:

        event[
            "tryNumber"
        ] = int(
            try_number
        )

    if max_tries is not None:

        event[
            "maxTries"
        ] = int(
            max_tries
        )

    # --------------------------------------------------------
    # Duration
    # --------------------------------------------------------

    start_date = (
        getattr(
            ti,
            "start_date",
            None,
        )
        or getattr(
            dag_run,
            "start_date",
            None,
        )
    )

    end_date = (
        getattr(
            ti,
            "end_date",
            None,
        )
        or getattr(
            dag_run,
            "end_date",
            None,
        )
    )

    duration = _duration_seconds(
        start_date,
        end_date,
    )

    if duration is not None:

        event[
            "durationSeconds"
        ] = duration

    if start_date is not None:

        event[
            "startTime"
        ] = _iso(
            start_date
        )

    if end_date is not None:

        event[
            "endTime"
        ] = _iso(
            end_date
        )

    # --------------------------------------------------------
    # Databricks correlation if this callback originated
    # from a Databricks operator task.
    # --------------------------------------------------------

    db_run_id = (
        _databricks_run_id(
            ti,
            task_id,
        )
    )

    if db_run_id:

        event[
            "databricksRunId"
        ] = db_run_id

    # --------------------------------------------------------
    # Exception details
    # --------------------------------------------------------

    exception = context.get(
        "exception"
    )

    if exception is not None:

        event[
            "exceptionType"
        ] = type(
            exception
        ).__name__

        event[
            "errorMessage"
        ] = _safe_text(
            exception
        )

    # --------------------------------------------------------
    # Airflow log correlation
    # --------------------------------------------------------

    log_url = getattr(
        ti,
        "log_url",
        None,
    )

    if log_url:

        event[
            "airflowLogUrl"
        ] = str(
            log_url
        )

    # --------------------------------------------------------
    # New Relic does not need empty attributes.
    # --------------------------------------------------------

    return {
        key: value
        for key, value
        in event.items()
        if (
            value is not None
            and value != ""
        )
    }


# ============================================================
# NEW RELIC EVENT PUBLISHER
# ============================================================

def publish_new_relic_event(
    event: Dict[str, Any],
) -> bool:
    """
    Publish exactly one custom New Relic event.

    Telemetry errors are logged and returned as False.

    They are deliberately NOT raised because monitoring
    failures must not change the original Airflow pipeline
    outcome.
    """

    endpoint = os.getenv(
        "NEW_RELIC_EVENT_URL"
    )

    license_key = os.getenv(
        "NEW_RELIC_LICENSE_KEY"
    )

    if (
        not endpoint
        or not license_key
    ):

        LOG.error(
            "New Relic telemetry not sent: "
            "NEW_RELIC_EVENT_URL or "
            "NEW_RELIC_LICENSE_KEY is not configured. "
            "eventName=%s airflowRunId=%s",
            event.get(
                "eventName"
            ),
            event.get(
                "airflowRunId"
            ),
        )

        return False

    payload = json.dumps(
        [event],
        separators=(
            ",",
            ":",
        ),
    ).encode(
        "utf-8"
    )

    request = Request(
        endpoint,
        data=payload,
        method="POST",
        headers={
            "Api-Key":
                license_key,

            "Content-Type":
                "application/json",

            "User-Agent":
                "team3-airflow-newrelic/1.0",
        },
    )

    try:

        with urlopen(
            request,
            timeout=(
                CONNECT_AND_READ_TIMEOUT_SECONDS
            ),
        ) as response:

            status_code = getattr(
                response,
                "status",
                response.getcode(),
            )

            if (
                200
                <= status_code
                < 300
            ):

                LOG.info(
                    "New Relic telemetry sent: "
                    "eventName=%s "
                    "status=%s "
                    "airflowRunId=%s "
                    "taskId=%s "
                    "batchId=%s "
                    "databricksRunId=%s "
                    "httpStatus=%s",
                    event.get(
                        "eventName"
                    ),
                    event.get(
                        "status"
                    ),
                    event.get(
                        "airflowRunId"
                    ),
                    event.get(
                        "taskId"
                    ),
                    event.get(
                        "batchId"
                    ),
                    event.get(
                        "databricksRunId"
                    ),
                    status_code,
                )

                return True

            LOG.error(
                "New Relic telemetry returned "
                "HTTP %s for eventName=%s",
                status_code,
                event.get(
                    "eventName"
                ),
            )

    except HTTPError as exc:

        LOG.error(
            "New Relic telemetry HTTP failure: "
            "status=%s eventName=%s",
            exc.code,
            event.get(
                "eventName"
            ),
        )

    except (
        URLError,
        TimeoutError,
        OSError,
    ) as exc:

        LOG.error(
            "New Relic telemetry delivery failure: "
            "eventName=%s "
            "errorType=%s "
            "error=%s",
            event.get(
                "eventName"
            ),
            type(
                exc
            ).__name__,
            _safe_text(
                exc
            ),
        )

    except Exception as exc:

        LOG.exception(
            "Unexpected New Relic telemetry "
            "failure for eventName=%s: %s",
            event.get(
                "eventName"
            ),
            _safe_text(
                exc
            ),
        )

    return False


# ============================================================
# TASK RETRY CALLBACK
# ============================================================

def new_relic_task_retry(
    context: Dict[str, Any],
) -> None:

    try:

        publish_new_relic_event(
            _base_event(
                context,
                "task_retry",
                "RETRYING",
                "task",
            )
        )

    except Exception as exc:

        LOG.exception(
            "Task retry callback failed "
            "without changing task outcome: %s",
            _safe_text(
                exc
            ),
        )


# ============================================================
# TASK FAILURE CALLBACK
# ============================================================

def new_relic_task_failure(
    context: Dict[str, Any],
) -> None:

    try:

        publish_new_relic_event(
            _base_event(
                context,
                "task_failure",
                "FAILED",
                "task",
            )
        )

    except Exception as exc:

        LOG.exception(
            "Task failure callback failed "
            "without masking original failure: %s",
            _safe_text(
                exc
            ),
        )


# ============================================================
# HEARTBEAT
# ============================================================

def _publish_heartbeat(
    context: Dict[str, Any],
    dag_status: str,
) -> None:
    """
    Publish a lightweight heartbeat whenever the DAG reaches
    a terminal callback.

    The heartbeat represents health of the Airflow →
    New Relic telemetry path.

    An ALIVE heartbeat does NOT mean the business pipeline
    succeeded.

    dagStatus separately communicates whether the DAG itself
    succeeded or failed.
    """

    try:

        event = _base_event(
            context,
            "heartbeat",
            "ALIVE",
            "dag",
        )

        event[
            "dagStatus"
        ] = str(
            dag_status
        ).upper()

        # Heartbeat semantics are clearer with a DAG-level
        # task description rather than whichever TaskInstance
        # Airflow supplied to the callback context.
        event[
            "taskId"
        ] = "dag_heartbeat"

        sent = (
            publish_new_relic_event(
                event
            )
        )

        if sent:

            LOG.info(
                "New Relic heartbeat sent: "
                "airflowRunId=%s "
                "dagStatus=%s "
                "batchId=%s",
                event.get(
                    "airflowRunId"
                ),
                event.get(
                    "dagStatus"
                ),
                event.get(
                    "batchId"
                ),
            )

        else:

            LOG.error(
                "New Relic heartbeat could not "
                "be delivered: "
                "airflowRunId=%s "
                "dagStatus=%s",
                event.get(
                    "airflowRunId"
                ),
                event.get(
                    "dagStatus"
                ),
            )

    except Exception as exc:

        LOG.exception(
            "Heartbeat telemetry failed "
            "without changing DAG outcome: %s",
            _safe_text(
                exc
            ),
        )


# ============================================================
# DAG FAILURE CALLBACK
# ============================================================

def new_relic_dag_failure(
    context: Dict[str, Any],
) -> None:
    """
    Publish the DAG failure event and independently publish
    a heartbeat proving that Airflow telemetry itself remains
    alive.
    """

    try:

        publish_new_relic_event(
            _base_event(
                context,
                "dag_failure",
                "FAILED",
                "dag",
            )
        )

    except Exception as exc:

        LOG.exception(
            "DAG failure callback failed "
            "without masking original failure: %s",
            _safe_text(
                exc
            ),
        )

    # Even though the business DAG failed, the telemetry path
    # clearly executed successfully enough to reach this callback.
    _publish_heartbeat(
        context,
        "FAILED",
    )


# ============================================================
# DAG SUCCESS / PIPELINE COMPLETION CALLBACK
# ============================================================

def new_relic_pipeline_success(
    context: Dict[str, Any],
) -> None:
    """
    Every successful DAG terminal callback publishes a
    heartbeat.

    A pipeline_completion event is published separately only
    when mark_batch_successful returned a real batch ID.

    Therefore:

        no-new-batch scheduled run
            → heartbeat only

        real successful batch
            → heartbeat
            → pipeline_completion

    This prevents healthy no-op runs from being falsely
    reported as processed business batches.
    """

    # --------------------------------------------------------
    # HEARTBEAT FIRST
    #
    # This must happen even when there is no new business
    # batch and the downstream data tasks were skipped.
    # --------------------------------------------------------

    _publish_heartbeat(
        context,
        "SUCCESS",
    )

    try:

        ti = (
            context.get(
                "task_instance"
            )
            or context.get(
                "ti"
            )
        )

        marked_batch = (
            _xcom_pull(
                ti,
                "mark_batch_successful",
            )
        )

        # ----------------------------------------------------
        # No-new-batch run
        #
        # Airflow may finish successfully even though
        # discover_new_batch intentionally skipped the
        # Databricks tasks.
        #
        # Heartbeat has already been emitted above.
        # Do not emit pipeline_completion.
        # ----------------------------------------------------

        if not marked_batch:

            LOG.info(
                "No pipeline_completion event: "
                "mark_batch_successful returned no batch. "
                "Heartbeat was emitted successfully for "
                "this no-op/skip DAG run."
            )

            return

        # ----------------------------------------------------
        # Actual successfully processed batch
        # ----------------------------------------------------

        event = _base_event(
            context,
            "pipeline_completion",
            "SUCCESS",
            "dag",
        )

        event[
            "batchId"
        ] = str(
            marked_batch
        )

        event[
            "inputBatch"
        ] = str(
            marked_batch
        )

        event[
            "taskId"
        ] = (
            "mark_batch_successful"
        )

        # ----------------------------------------------------
        # Add all three Databricks run IDs when available.
        # ----------------------------------------------------

        for (
            task_id,
            field,
        ) in (

            (
                "bronze_ingestion",
                "bronzeDatabricksRunId",
            ),

            (
                "silver_validation",
                "silverDatabricksRunId",
            ),

            (
                "gold_processing",
                "goldDatabricksRunId",
            ),
        ):

            value = (
                _databricks_run_id(
                    ti,
                    task_id,
                )
            )

            if value:

                event[
                    field
                ] = value

        # Keep the general databricksRunId correlated with
        # Gold for the final completion event.
        if event.get(
            "goldDatabricksRunId"
        ):

            event[
                "databricksRunId"
            ] = event[
                "goldDatabricksRunId"
            ]

        LOG.info(
            "Publishing successful "
            "pipeline telemetry: "
            "airflowRunId=%s "
            "batchId=%s "
            "bronzeDatabricksRunId=%s "
            "silverDatabricksRunId=%s "
            "goldDatabricksRunId=%s",
            event.get(
                "airflowRunId"
            ),
            event.get(
                "batchId"
            ),
            event.get(
                "bronzeDatabricksRunId"
            ),
            event.get(
                "silverDatabricksRunId"
            ),
            event.get(
                "goldDatabricksRunId"
            ),
        )

        sent = (
            publish_new_relic_event(
                event
            )
        )

        if not sent:

            LOG.error(
                "pipeline_completion telemetry "
                "could not be delivered for "
                "airflowRunId=%s batchId=%s",
                event.get(
                    "airflowRunId"
                ),
                event.get(
                    "batchId"
                ),
            )

    except Exception as exc:

        LOG.exception(
            "Pipeline success telemetry "
            "callback failed without changing "
            "DAG outcome: %s",
            _safe_text(
                exc
            ),
        )