"""Reusable New Relic callbacks for the Team 3 Airflow DAG.

No credentials are stored in source. Required environment variables:
  NEW_RELIC_LICENSE_KEY   (secret)
  NEW_RELIC_EVENT_URL     (non-secret full Event API endpoint)
Optional:
  NEW_RELIC_EVENT_TYPE    default: Team3AirflowOrchestration
  NEW_RELIC_TEAM          default: Team 3
  NEW_RELIC_PIPELINE      default: order_fulfilment
  NEW_RELIC_ENVIRONMENT   default: training
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

LOG = logging.getLogger(__name__)

EVENT_TYPE = os.getenv("NEW_RELIC_EVENT_TYPE", "Team3AirflowOrchestration")
TEAM = os.getenv("NEW_RELIC_TEAM", "Team 3")
PIPELINE = os.getenv("NEW_RELIC_PIPELINE", "order_fulfilment")
ENVIRONMENT = os.getenv("NEW_RELIC_ENVIRONMENT", "training")
CONNECT_AND_READ_TIMEOUT_SECONDS = 10
MAX_ERROR_LENGTH = 500

_SECRET_PATTERN = re.compile(
    r"(?i)(api[-_ ]?key|license[-_ ]?key|authorization|bearer|token|secret|password)"
    r"\s*[:=]\s*[^\s,;]+"
)


def _safe_text(value: Any, limit: int = MAX_ERROR_LENGTH) -> str:
    """Return bounded, single-line text with obvious credential material removed."""
    if value is None:
        return ""
    text = str(value).replace("\n", " ").replace("\r", " ")
    text = _SECRET_PATTERN.sub(r"\1=[REDACTED]", text)
    return text[:limit]


def _iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _duration_seconds(start: Any, end: Any = None) -> Optional[float]:
    if start is None:
        return None
    effective_end = end or datetime.now(timezone.utc)
    try:
        seconds = (effective_end - start).total_seconds()
        return round(max(0.0, float(seconds)), 3)
    except Exception:
        return None


def _xcom_pull(ti: Any, task_id: str, key: Optional[str] = None) -> Any:
    if ti is None:
        return None
    try:
        kwargs: Dict[str, Any] = {"task_ids": task_id}
        if key is not None:
            kwargs["key"] = key
        return ti.xcom_pull(**kwargs)
    except Exception as exc:
        LOG.warning("Telemetry could not read XCom for task %s: %s", task_id, _safe_text(exc))
        return None


def _batch_id(ti: Any) -> Optional[str]:
    value = _xcom_pull(ti, "discover_new_batch")
    if isinstance(value, dict):
        batch = value.get("batch_id")
        return str(batch) if batch else None
    return None


def _databricks_run_id(ti: Any, task_id: Optional[str]) -> Optional[str]:
    if ti is None or not task_id:
        return None

    # Provider versions commonly expose the submitted run using one of these keys,
    # or in the operator's return value. Missing values are omitted, never invented.
    for key in ("run_id", "databricks_run_id"):
        value = _xcom_pull(ti, task_id, key=key)
        if value not in (None, ""):
            return str(value)

    value = _xcom_pull(ti, task_id)
    if isinstance(value, dict):
        for key in ("run_id", "databricks_run_id"):
            if value.get(key) not in (None, ""):
                return str(value[key])
    elif isinstance(value, (str, int)) and str(value).isdigit():
        return str(value)
    return None


def _base_event(context: Dict[str, Any], event_name: str, status: str, scope: str) -> Dict[str, Any]:
    ti = context.get("task_instance") or context.get("ti")
    dag_run = context.get("dag_run")
    task = context.get("task")

    dag_id = getattr(dag_run, "dag_id", None) or getattr(task, "dag_id", None)
    run_id = getattr(dag_run, "run_id", None)
    task_id = getattr(ti, "task_id", None) or getattr(task, "task_id", None)

    event: Dict[str, Any] = {
        "eventType": EVENT_TYPE,
        "eventName": event_name,
        "scope": scope,
        "team": TEAM,
        "pipeline": PIPELINE,
        "environment": ENVIRONMENT,
        "deployment": "team3-order-fulfilment",
        "dagId": dag_id,
        "taskId": task_id,
        "airflowRunId": run_id,
        "batchId": _batch_id(ti),
        "inputBatch": _batch_id(ti),
        "status": status,
        "observedAt": datetime.now(timezone.utc).isoformat(),
    }

    try_number = getattr(ti, "try_number", None)
    max_tries = getattr(ti, "max_tries", None)
    if try_number is not None:
        event["tryNumber"] = int(try_number)
    if max_tries is not None:
        event["maxTries"] = int(max_tries)

    start_date = getattr(ti, "start_date", None) or getattr(dag_run, "start_date", None)
    end_date = getattr(ti, "end_date", None) or getattr(dag_run, "end_date", None)
    duration = _duration_seconds(start_date, end_date)
    if duration is not None:
        event["durationSeconds"] = duration

    if start_date is not None:
        event["startTime"] = _iso(start_date)
    if end_date is not None:
        event["endTime"] = _iso(end_date)

    db_run_id = _databricks_run_id(ti, task_id)
    if db_run_id:
        event["databricksRunId"] = db_run_id

    exception = context.get("exception")
    if exception is not None:
        event["exceptionType"] = type(exception).__name__
        event["errorMessage"] = _safe_text(exception)

    log_url = getattr(ti, "log_url", None)
    if log_url:
        event["airflowLogUrl"] = str(log_url)

    return {key: value for key, value in event.items() if value is not None and value != ""}


def publish_new_relic_event(event: Dict[str, Any]) -> bool:
    """Send one event. Never raise, so telemetry cannot mask pipeline state."""
    endpoint = os.getenv("NEW_RELIC_EVENT_URL")
    license_key = os.getenv("NEW_RELIC_LICENSE_KEY")

    if not endpoint or not license_key:
        LOG.error(
            "New Relic telemetry not sent: NEW_RELIC_EVENT_URL or "
            "NEW_RELIC_LICENSE_KEY is not configured. eventName=%s airflowRunId=%s",
            event.get("eventName"),
            event.get("airflowRunId"),
        )
        return False

    payload = json.dumps([event], separators=(",", ":")).encode("utf-8")
    request = Request(
        endpoint,
        data=payload,
        method="POST",
        headers={
            "Api-Key": license_key,
            "Content-Type": "application/json",
            "User-Agent": "team3-airflow-newrelic/1.0",
        },
    )

    try:
        with urlopen(request, timeout=CONNECT_AND_READ_TIMEOUT_SECONDS) as response:
            status_code = getattr(response, "status", response.getcode())
            if 200 <= status_code < 300:
                LOG.info(
                    "New Relic telemetry sent: eventName=%s status=%s airflowRunId=%s "
                    "taskId=%s batchId=%s databricksRunId=%s httpStatus=%s",
                    event.get("eventName"), event.get("status"), event.get("airflowRunId"),
                    event.get("taskId"), event.get("batchId"),
                    event.get("databricksRunId"), status_code,
                )
                return True
            LOG.error("New Relic telemetry returned HTTP %s for eventName=%s", status_code, event.get("eventName"))
    except HTTPError as exc:
        LOG.error("New Relic telemetry HTTP failure: status=%s eventName=%s", exc.code, event.get("eventName"))
    except (URLError, TimeoutError, OSError) as exc:
        LOG.error("New Relic telemetry delivery failure: eventName=%s errorType=%s error=%s", event.get("eventName"), type(exc).__name__, _safe_text(exc))
    except Exception as exc:
        LOG.exception("Unexpected New Relic telemetry failure for eventName=%s: %s", event.get("eventName"), _safe_text(exc))
    return False


def new_relic_task_retry(context: Dict[str, Any]) -> None:
    try:
        publish_new_relic_event(_base_event(context, "task_retry", "RETRYING", "task"))
    except Exception as exc:
        LOG.exception("Task retry callback failed without changing task outcome: %s", _safe_text(exc))


def new_relic_task_failure(context: Dict[str, Any]) -> None:
    try:
        publish_new_relic_event(_base_event(context, "task_failure", "FAILED", "task"))
    except Exception as exc:
        LOG.exception("Task failure callback failed without masking original failure: %s", _safe_text(exc))


def new_relic_dag_failure(context: Dict[str, Any]) -> None:
    try:
        publish_new_relic_event(_base_event(context, "dag_failure", "FAILED", "dag"))
    except Exception as exc:
        LOG.exception("DAG failure callback failed without masking original failure: %s", _safe_text(exc))


def new_relic_pipeline_success(context: Dict[str, Any]) -> None:
    """Emit completion only when mark_batch_successful actually succeeded.

    A no-new-batch run intentionally skips downstream tasks. Such a no-op DAG must
    remain successful, but it is not reported as a completed data-processing run.
    """
    try:
        dag_run = context.get("dag_run")
        ti = context.get("task_instance") or context.get("ti")
        mark_ti = None
        if dag_run is not None:
            try:
                mark_ti = dag_run.get_task_instance("mark_batch_successful")
            except Exception as exc:
                LOG.warning("Could not inspect mark_batch_successful state: %s", _safe_text(exc))

        if mark_ti is None or str(getattr(mark_ti, "state", "")).lower() != "success":
            LOG.info("No pipeline_completion event: mark_batch_successful did not succeed (no-op/skip run).")
            return

        event = _base_event(context, "pipeline_completion", "SUCCESS", "dag")
        if not event.get("batchId"):
            marked_batch = _xcom_pull(ti, "mark_batch_successful")
            if marked_batch:
                event["batchId"] = str(marked_batch)
                event["inputBatch"] = str(marked_batch)
        event["taskId"] = "mark_batch_successful"

        # Include all three Databricks run IDs when their operator XCom exposes them.
        for task_id, field in (
            ("bronze_ingestion", "bronzeDatabricksRunId"),
            ("silver_validation", "silverDatabricksRunId"),
            ("gold_processing", "goldDatabricksRunId"),
        ):
            value = _databricks_run_id(ti, task_id)
            if value:
                event[field] = value
        event["databricksRunId"] = event.get("goldDatabricksRunId")
        publish_new_relic_event(event)
    except Exception as exc:
        LOG.exception("Pipeline success callback failed without changing DAG outcome: %s", _safe_text(exc))
