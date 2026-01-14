
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import boto3
import os
import re
import json
import io

# AWS S3 Configuration
S3_BUCKET = "s3-joacademy-event-data-bucket-211"
S3_REGION = "eu-north-1"
s3_client = boto3.client("s3", region_name=S3_REGION)

# S3 paths
S3_FULL_LOAD_PATH = "full_load/event_data.csv"
S3_INCREMENTAL_BASE = "incremental"


def clean_and_validate_mobile(mobile):
    """Validate mobile number format and extract valid digits."""
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
    """Add 962 prefix to standardize mobile number format."""
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


def run_full_excel_load(**kwargs):
    print("Starting full load from Google Sheets...")

    try:
        print("Connecting to AWS S3...")

        dag_run = kwargs.get('dag_run')
        conf = dag_run.conf if dag_run else {}
        if not conf:
            conf = {}

        sheets_data = {}
        payload_file = conf.get('payload_file') if conf else None
        if payload_file and os.path.exists(payload_file):
            try:
                with open(payload_file, 'r', encoding='utf-8') as f:
                    file_payload = json.load(f)
                sheets_data = file_payload.get('sheets', {})
                print(f"Loaded sheets from payload file: {payload_file}")
            except Exception as pf_err:
                print(f"Failed to read payload file {payload_file}: {pf_err}")
        if not sheets_data:
            sheets_data = conf.get('sheets', {})
        
        if not sheets_data:
            print("No sheets provided in configuration. Aborting full load.")
            return

        sheets_to_skip = ["test", "Template", "الاحصائيات"]

        dataframes = []
        for sheet_name, rows in sheets_data.items():
            if sheet_name in sheets_to_skip:
                print(f"Skipping sheet: {sheet_name}")
                continue
            
            if not rows:
                print(f"Sheet {sheet_name} is empty, skipping")
                continue
            
            df = pd.DataFrame(rows)
            canonical = ["name", "mobile", "grade", "location", "data_source", "data_source_2", "data_source_1"]
            for idx, col_name in enumerate(canonical):
                if idx < df.shape[1]:
                    df.rename(columns={idx: col_name}, inplace=True)
                else:
                    df[col_name] = None
            df = df.copy()
            df["sheet_name"] = sheet_name
            dataframes.append(df)

        if not dataframes:
            print("No sheets to load after filtering. Aborting full load.")
            return

        combined_df = pd.concat(dataframes, ignore_index=True)

        combined_df.reset_index(drop=False, inplace=True)
        combined_df.rename(columns={"index": "id"}, inplace=True)
        combined_df["id"] = combined_df["id"] + 1

        print("Validating and normalizing mobile numbers...")
        if 'mobile' in combined_df.columns:
            mobile_series = combined_df['mobile']
        elif combined_df.shape[1] > 1:
            print("Warning: 'mobile' column not found; falling back to column index 1")
            mobile_series = combined_df.iloc[:, 1]
        else:
            mobile_series = pd.Series([None] * len(combined_df))

        combined_df['mobile_clean'] = mobile_series.apply(clean_and_validate_mobile)
        valid_rows = combined_df[combined_df['mobile_clean'].notna()]
        invalid_mobile_count = len(combined_df) - len(valid_rows)
        if invalid_mobile_count > 0:
            print(f"Found {invalid_mobile_count} rows with invalid mobile numbers, filtering them out")
        combined_df = valid_rows.copy()
        combined_df['mobile'] = combined_df['mobile_clean'].apply(normalize_mobile)
        combined_df = combined_df.drop(columns=['mobile_clean'])
        
        print("Validating grade field...")
        if 'grade' in combined_df.columns:
            combined_df['grade_valid'] = combined_df['grade'].apply(validate_grade)
            valid_rows = combined_df[combined_df['grade_valid'] == True]
            invalid_grade_count = len(combined_df) - len(valid_rows)
            if invalid_grade_count > 0:
                print(f"Found {invalid_grade_count} rows with invalid grade ('غير معرف'), filtering them out")
            combined_df = valid_rows.copy()
            combined_df = combined_df.drop(columns=['grade_valid'])
        
        if len(combined_df) == 0:
            print("All rows had invalid mobile numbers. Aborting full load.")
            return

        combined_df["timestamp"] = None

        # Keep only the canonical columns (filter out columns 7-11)
        columns_to_keep = ["id", "name", "mobile", "grade", "location", "data_source", "data_source_2", "data_source_1", "sheet_name", "timestamp"]
        existing_columns = [col for col in columns_to_keep if col in combined_df.columns]
        combined_df = combined_df[existing_columns]
        print(f"Keeping only canonical columns: {existing_columns}")

        # Ensure all string columns are properly typed for CSV
        for col in combined_df.columns:
            if combined_df[col].dtype == 'object':
                combined_df[col] = combined_df[col].astype(str)

        # Write full load to S3 as CSV using shared client with default creds/role
        # Convert DataFrame to CSV and upload to S3
        csv_buffer = io.BytesIO()
        combined_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=S3_FULL_LOAD_PATH,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        print(f"Full load written to S3: s3://{S3_BUCKET}/{S3_FULL_LOAD_PATH}")

    except Exception as e:
        print(f"Error in run_full_excel_load: {str(e)}")
        raise


with DAG(
    dag_id="full_excel_load",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    full_load_task = PythonOperator(
        task_id="run_full_excel_load",
        python_callable=run_full_excel_load,
        op_kwargs={},
    )
