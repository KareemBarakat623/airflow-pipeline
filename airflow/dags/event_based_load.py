from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import boto3
import json
import os
import re
import time
import io
import yaml

# Load configuration from config.yaml
def load_config():
    config_path = "/opt/airflow/config.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

config = load_config()

# AWS S3 Configuration from config.yaml
S3_BUCKET = config['s3']['bucket']
S3_REGION = config['aws']['region']
AWS_ACCESS_KEY_ID = config['aws']['access_key_id']
AWS_SECRET_ACCESS_KEY = config['aws']['secret_access_key']
AWS_SESSION_TOKEN = config['aws'].get('session_token')  # Optional for session-based accounts

# S3 paths
S3_FULL_LOAD_PATH = f"s3://{S3_BUCKET}/{config['s3']['full_load_path']}"
S3_INCREMENTAL_BASE = f"s3://{S3_BUCKET}/{config['s3']['incremental_base']}"
INCREMENTAL_FILE = config['airflow']['incremental_file']

COLUMN_MAPPING = {
    0: "name",
    1: "mobile",
    2: "grade",
    3: "location",
    4: "data_source",
    5: "data_source_2",
    6: "data_source_1",
}


def clean_and_validate_mobile(mobile):
    if pd.isna(mobile) or mobile is None:
        return None
    if not isinstance(mobile, str):
        mobile = str(mobile).strip()
    if "/" in mobile:
        parts = mobile.split("/")
        for part in parts:
            digits = re.sub(r"\D", "", part)
            if len(digits) == 9 and digits.startswith(("77", "78", "79")):
                return digits
        return None
    mobile = re.sub(r"\D", "", mobile)
    if len(mobile) == 9 and mobile[:2] in ["77", "78", "79"]:
        return mobile
    if len(mobile) == 12 and mobile.startswith("962"):
        return mobile[3:]
    return None


def normalize_mobile(mobile):
    if mobile is None:
        return None
    mobile = str(mobile)
    if len(mobile) == 9 and mobile[:2] in ["77", "78", "79"]:
        return "962" + mobile
    return mobile


def validate_grade(grade):
    """Check if grade is valid (not 'غير معرف')."""
    if pd.isna(grade) or grade is None:
        return False
    grade_str = str(grade).strip()
    if grade_str == "غير معرف":
        return False
    return True


def process_incoming_data(**context):
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    if not conf:
        conf = {}

    print(f"Received configuration: {conf}")

    if not conf or "data" not in conf:
        print("No data in configuration. Skipping.")
        return

    row_data = conf.get("data", [])
    sheet_name = conf.get("sheet", "unknown")
    row_num = conf.get("row", 0)
    timestamp = conf.get("timestamp", "")

    print(f"Processing data from sheet '{sheet_name}', row {row_num}")
    print(f"Row data: {row_data}")
    print(f"Timestamp: {timestamp}")

    row_dict = {}
    for idx, col_name in COLUMN_MAPPING.items():
        row_dict[col_name] = row_data[idx] if idx < len(row_data) else None
    row_dict["sheet_name"] = sheet_name
    row_dict["timestamp"] = timestamp

    mobile_raw = row_dict.get("mobile")
    mobile_clean = clean_and_validate_mobile(mobile_raw)
    if not mobile_clean:
        print(f"Invalid mobile format: {mobile_raw}. Skipping insert.")
        return
    row_dict["mobile"] = normalize_mobile(mobile_clean)

    grade_value = row_dict.get("grade")
    if not validate_grade(grade_value):
        print(f"Invalid grade value: {grade_value}. Skipping insert.")
        return

    print(f"Mapped row: {row_dict}")

    # These lines need to be inside the function
    os.makedirs(os.path.dirname(INCREMENTAL_FILE), exist_ok=True)

    with open(INCREMENTAL_FILE, "w", encoding="utf-8") as f:
        json.dump([row_dict], f, ensure_ascii=False, indent=2)

    print("Wrote complete row to incremental file")


def insert_incremental_rows():
    if not os.path.exists(INCREMENTAL_FILE):
        print("Incremental file missing. Nothing to load.")
        return

    with open(INCREMENTAL_FILE, "r", encoding="utf-8") as f:
        rows = json.load(f)
    if not rows:
        print("Incremental file empty.")
        return

    incremental_df = pd.DataFrame(rows)
    print(f"Inserting {len(incremental_df)} new row(s)")
    print(f"Columns: {incremental_df.columns.tolist()}")

    try:
        # Initialize S3 client with credentials from config.yaml
        s3_config = {
            'region_name': S3_REGION,
            'aws_access_key_id': AWS_ACCESS_KEY_ID,
            'aws_secret_access_key': AWS_SECRET_ACCESS_KEY
        }
        if AWS_SESSION_TOKEN:
            s3_config['aws_session_token'] = AWS_SESSION_TOKEN
        
        s3_client = boto3.client("s3", **s3_config)

        # Read existing full load data for duplicate checking
        try:
            response = s3_client.get_object(
                Bucket=S3_BUCKET, Key="full_load/event_data.parquet"
            )
            existing_df = pd.read_parquet(io.BytesIO(response["Body"].read()))
            print(f"Current S3 full load has {len(existing_df)} rows")
        except s3_client.exceptions.NoSuchKey:
            existing_df = pd.DataFrame()
            print("No existing full load found in S3. Starting fresh.")
        except Exception as e:
            existing_df = pd.DataFrame()
            print(f"Could not read existing data: {e}")

        # Get today's date for partitioning
        today = datetime.utcnow()
        date_partition = today.strftime("%Y/%m/%d")
        timestamp_str = today.isoformat().replace(":", "-").split(".")[0]
        incremental_key = f"incremental/{date_partition}/event_{timestamp_str}.parquet"

        # Check for duplicates using composite key
        for idx, row in incremental_df.iterrows():
            row_dict = row.to_dict()

            if len(existing_df) > 0:
                key_match = (
                    (existing_df["name"].astype(str) == str(row_dict.get("name", "")))
                    & (
                        existing_df["mobile"].astype(str)
                        == str(row_dict.get("mobile", ""))
                    )
                    & (
                        existing_df["data_source"].astype(str)
                        == str(row_dict.get("data_source", ""))
                    )
                    & (
                        existing_df["sheet_name"].astype(str)
                        == str(row_dict.get("sheet_name", ""))
                    )
                )

                if key_match.any():
                    existing_id = existing_df[key_match]["id"].iloc[0]
                    print(
                        f"Row {idx}: Duplicate detected, existing ID {existing_id}. Skipping insert."
                    )
                    continue

            # Assign new ID
            max_id = existing_df["id"].max() if len(existing_df) > 0 else 0
            row_dict["id"] = int(max_id) + 1

            # Update in-memory DataFrame
            new_row_df = pd.DataFrame([row_dict])
            existing_df = pd.concat([existing_df, new_row_df], ignore_index=True)

            print(
                f"Row {idx}: Inserted new record (ID: {row_dict['id']}, name: {row_dict.get('name', 'N/A')})"
            )

        # Write incremental data to S3 as Parquet
        parquet_buffer = io.BytesIO()
        incremental_df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
        parquet_buffer.seek(0)

        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=incremental_key,
            Body=parquet_buffer.getvalue(),
            ContentType="application/octet-stream",
        )
        print(
            f"Wrote {len(incremental_df)} rows to S3: s3://{S3_BUCKET}/{incremental_key}"
        )
        
        # Trigger Glue crawler to catalog the new incremental data
        from glue_utils import start_crawler
        print("Triggering Glue crawler for incremental data...")
        start_crawler('incremental')

        # Clean up temporary JSON file
        try:
            os.remove(INCREMENTAL_FILE)
        except FileNotFoundError:
            pass

        print(f"Completed insertion of {len(incremental_df)} row(s).")

    except Exception as e:
        print(f"Error inserting incremental rows: {str(e)}")
        raise


with DAG(
    dag_id="event_based_load",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    process_data_task = PythonOperator(
        task_id="process_incoming_data",
        python_callable=process_incoming_data,
    )

    incremental_insert_task = PythonOperator(
        task_id="insert_incremental_rows",
        python_callable=insert_incremental_rows,
    )

    process_data_task >> incremental_insert_task
