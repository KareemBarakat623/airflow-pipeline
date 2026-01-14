from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import subprocess
import json
import datetime
import hashlib
import time
import os

app = FastAPI()

recent_triggers = {}


class SheetEvent(BaseModel):
    sheet: str
    row: int
    data: List
    timestamp: str


class FullLoadEvent(BaseModel):
    sheets: dict
    timestamp: str


DAG_ID = "event_based_load"
FULL_LOAD_DAG_ID = "full_excel_load"


def trigger_dag(payload: dict, dag_id: str = DAG_ID):
    """Trigger DAG using airflow CLI inside the Docker container."""

    dag_run_id = f"manual__{datetime.datetime.utcnow().isoformat()}"

    try:
        conf_json = json.dumps(payload)

        result = subprocess.run(
            [
                "docker",
                "exec",
                "workspace-airflow-1",
                "airflow",
                "dags",
                "trigger",
                dag_id,
                "--run-id",
                dag_run_id,
                "--conf",
                conf_json,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
        )

        if result.returncode == 0:
            print(f"DAG {dag_id} triggered successfully: {dag_run_id}")
            print(f"Output: {result.stdout}")
            return type(
                "obj",
                (object,),
                {
                    "status_code": 200,
                    "text": f'{{"dag_run_id": "{dag_run_id}", "status": "success"}}',
                    "json": lambda: {"dag_run_id": dag_run_id, "status": "success"},
                },
            )()
        else:
            error_msg = result.stderr if result.stderr else "Unknown error"
            print(f"DAG trigger failed: {error_msg}")
            return type(
                "obj",
                (object,),
                {
                    "status_code": 500,
                    "text": error_msg,
                    "json": lambda: {"error": error_msg},
                },
            )()

    except Exception as e:
        print(f"Error triggering DAG: {str(e)}")
        return type(
            "obj",
            (object,),
            {"status_code": 500, "text": str(e), "json": lambda: {"error": str(e)}},
        )()


@app.post("/new_row")
def receive_sheet_update(event: SheetEvent):

    print(f"Received update from Google Sheets:")
    print(f"  Sheet: {event.sheet}")
    print(f"  Row:   {event.row}")
    print(f"  Data:  {event.data}")
    print(f"  Time:  {event.timestamp}")

    required_fields = [
        "name",
        "mobile",
        "grade",
        "location",
        "data_source",
        "data_source_2",
        "data_source_1",
    ]
    required_indices = [0, 1, 2, 3, 4, 5, 6]

    missing_or_empty = []
    for idx, field_name in zip(required_indices, required_fields):
        if (
            idx >= len(event.data)
            or event.data[idx] is None
            or str(event.data[idx]).strip() == ""
        ):
            missing_or_empty.append(f"{field_name} (index {idx})")

    if missing_or_empty:
        print(f"Row is incomplete. Missing or empty fields: {missing_or_empty}")
        print("Skipping DAG trigger until row is complete.")
        return {
            "status": "INCOMPLETE",
            "message": f"Row is incomplete. Missing fields: {', '.join(missing_or_empty)}",
            "airflow_status": None,
            "airflow_response": None,
        }

    print("Row is complete. Checking for duplicate trigger...")

    row_key = f"{event.sheet}:{event.row}:{json.dumps(event.data, ensure_ascii=False)}"
    row_hash = hashlib.md5(row_key.encode("utf-8")).hexdigest()
    current_time = time.time()

    debounce_window = 10

    if row_hash in recent_triggers:
        last_trigger_time = recent_triggers[row_hash]
        if current_time - last_trigger_time < debounce_window:
            print(
                f"Duplicate trigger detected within {debounce_window}s. Skipping DAG trigger."
            )
            return {
                "status": "DUPLICATE",
                "message": f"Duplicate row detected within {debounce_window} seconds. Skipped to prevent double insert.",
                "airflow_status": None,
                "airflow_response": None,
            }

    recent_triggers[row_hash] = current_time

    old_keys = [
        k for k, v in recent_triggers.items() if current_time - v > debounce_window * 2
    ]
    for k in old_keys:
        del recent_triggers[k]

    print("No duplicate detected. Proceeding with DAG trigger.")

    dag_conf = {
        "sheet": event.sheet,
        "row": event.row,
        "data": event.data,
        "timestamp": event.timestamp,
    }

    airflow_response = trigger_dag(dag_conf)

    try:
        airflow_json = airflow_response.json()
    except Exception:
        airflow_json = {"error": "Invalid JSON response from Airflow"}

    return {
        "status": "OK",
        "airflow_status": airflow_response.status_code,
        "airflow_response": airflow_json,
    }


@app.post("/full_load")
def receive_full_load(event: FullLoadEvent):

    print(f"Received full load request from Google Sheets:")
    print(f"  Sheets: {list(event.sheets.keys())}")
    print(f"  Time:   {event.timestamp}")

    if not event.sheets:
        print("No sheets provided in payload. Aborting full load.")
        return {
            "status": "ERROR",
            "message": "No sheets provided in payload",
            "airflow_status": None,
            "airflow_response": None,
        }

    print(
        f"Full load triggered with {len(event.sheets)} sheets. Preparing payload file..."
    )

    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        host_data_dir = os.path.join(base_dir, "airflow", "data")
        os.makedirs(host_data_dir, exist_ok=True)

        fname = f"full_load_sheets_{int(time.time())}.json"
        host_payload_path = os.path.join(host_data_dir, fname)

        payload_content = {
            "sheets": event.sheets,
            "timestamp": event.timestamp,
        }
        with open(host_payload_path, "w", encoding="utf-8") as f:
            json.dump(payload_content, f, ensure_ascii=False)

        container_payload_path = f"/opt/airflow/data/{fname}"
        print(
            f"Wrote payload to {host_payload_path} (container path: {container_payload_path})"
        )

        dag_conf = {
            "payload_file": container_payload_path,
            "timestamp": event.timestamp,
        }

        airflow_response = trigger_dag(dag_conf, dag_id=FULL_LOAD_DAG_ID)
    except Exception as write_err:
        print(f"Failed to write payload file: {write_err}")
        return {
            "status": "ERROR",
            "message": f"Failed to write payload file: {write_err}",
            "airflow_status": None,
            "airflow_response": None,
        }

    try:
        airflow_json = airflow_response.json()
    except Exception:
        airflow_json = {"error": "Invalid JSON response from Airflow"}

    return {
        "status": "OK",
        "airflow_status": airflow_response.status_code,
        "airflow_response": airflow_json,
    }
