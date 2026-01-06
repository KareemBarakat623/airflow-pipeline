from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import sqlite3
import re
import shutil
import time
import os


DB_PATH = "/opt/airflow/sqlite/my_db.db"


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


def normalize_all_mobiles():
    ts = int(time.time())
    backup_path = f"{DB_PATH}.bak.{ts}"
    shutil.copy(DB_PATH, backup_path)
    print(f"Backup created at {backup_path}")

    conn = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
    df = pd.read_sql("SELECT * FROM excel_filtered_sheets", conn)
    print(f"Loaded {len(df)} rows")

    df['mobile_raw'] = df['mobile']
    df['mobile_clean'] = df['mobile_raw'].apply(clean_and_validate_mobile)
    df_valid = df[df['mobile_clean'].notna()].copy()
    df_invalid = df[df['mobile_clean'].isna()].copy()

    df_valid['mobile'] = df_valid['mobile_clean'].apply(normalize_mobile)

    print(f"Valid rows: {len(df_valid)}; Invalid rows to delete: {len(df_invalid)}")

    conn.execute("DELETE FROM excel_filtered_sheets")
    conn.commit()
    df_valid.drop(columns=['mobile_raw', 'mobile_clean'], inplace=True)
    df_valid.to_sql("excel_filtered_sheets", conn, if_exists="append", index=False)
    conn.commit()
    conn.close()

    if len(df_invalid) > 0:
        invalid_log = df_invalid[['id', 'name', 'mobile_raw']]
        print("Removed invalid rows:")
        print(invalid_log.to_string(index=False))
    print("Normalization complete.")


with DAG(
    dag_id="normalize_mobiles",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    normalize_task = PythonOperator(
        task_id="normalize_all_mobiles",
        python_callable=normalize_all_mobiles,
    )
