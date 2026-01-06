# Airflow Google Sheets to S3 Data Pipeline

Event-driven ETL pipeline that ingests data from Google Sheets, validates and normalizes it, and stores it as Parquet files in AWS S3. Built with Apache Airflow, FastAPI, and Docker.

## Architecture

- **Google Sheets** → Apps Script triggers on edit
- **FastAPI webhook** → Validates completeness, debounces duplicate triggers
- **Apache Airflow DAGs** → Validates mobile/grade fields, writes to S3
- **AWS S3** → Parquet files (full load + incremental with date partitioning)
- **AWS Athena** (optional) → SQL query layer

## Prerequisites

- Docker Desktop installed and running
- Docker Compose installed
- AWS account with S3 access
- Valid AWS credentials (Access Key ID, Secret Access Key, and Session Token if using STS)

## Setup

### 1. Clone the repository
```bash
git clone https://github.com/KareemBarakat623/airflow-pipeline.git
cd airflow-pipeline
```

### 2. Configure AWS credentials
```bash
cp .env.example .env
```

Edit `.env` and fill in your AWS credentials:
```
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_SESSION_TOKEN=your_session_token_here  # Only if using STS/assumed role
```

### 3. (Optional) Configure S3 bucket and region
If you want to use your own S3 bucket, edit these files:
- `airflow/dags/event_based_load.py` - lines 14-15
- `airflow/dags/full_excel_load.py` - lines 12-13

Update:
```python
S3_BUCKET = "your-bucket-name"
S3_REGION = "your-region"  # e.g., us-east-1, eu-north-1
```

Default configuration:
- Bucket: `s3-joacademy-event-data-bucket-211`
- Region: `eu-north-1`

### 4. Start the Docker stack
```bash
docker-compose down
docker-compose up -d
```

Wait 30-60 seconds for Airflow to fully initialize.

### 5. Access Airflow UI
Open your browser: http://localhost:8080

Default credentials are set by Airflow standalone mode (check logs if needed).

## Usage

### Triggering DAGs

#### Full Load DAG
Loads all data from Google Sheets, validates, and writes to `s3://bucket/full_load/event_data.parquet`.

**Via Airflow UI:**
1. Go to http://localhost:8080
2. Find `full_excel_load` DAG
3. Click "Trigger DAG" (play button)
4. Provide configuration with sheets data or payload file path

**Via Apps Script:**
The `sendFullDataLoad()` function in Google Sheets Apps Script triggers this DAG via the FastAPI webhook.

#### Incremental/Event-based DAG
Processes row-by-row edits from Google Sheets and writes to `s3://bucket/incremental/YYYY/MM/DD/event_*.parquet`.

**Via Airflow UI:**
1. Find `event_based_load` DAG
2. Trigger with proper configuration

**Via Apps Script:**
The `onEdit()` trigger in Google Sheets Apps Script automatically triggers this DAG on cell edits.

### Data Validation

Both DAGs apply the following validation rules:

**Mobile number validation:**
- Accepts 9-digit numbers starting with 77, 78, or 79
- Accepts 12-digit numbers starting with 962
- Accepts slash-separated formats (e.g., "77/1234567")
- Normalizes to 962 prefix format (e.g., 771234567 → 962771234567)
- Invalid numbers are filtered out

**Grade validation:**
- Rejects rows where grade = "غير معرف" (unknown)
- All other grade values are accepted

**Column filtering:**
- Keeps only canonical columns: id, name, mobile, grade, location, data_source, data_source_2, data_source_1, sheet_name, timestamp
- Extra columns (7-11) are removed

## Querying Data with AWS Athena (Optional)

### 1. Create Athena database
```sql
CREATE DATABASE event_data;
```

### 2. Create full load table
```sql
CREATE EXTERNAL TABLE event_data.full_load (
  id BIGINT,
  name STRING,
  mobile STRING,
  grade STRING,
  location STRING,
  data_source STRING,
  data_source_2 STRING,
  data_source_1 STRING,
  sheet_name STRING,
  timestamp STRING
)
STORED AS PARQUET
LOCATION 's3://s3-joacademy-event-data-bucket-211/full_load/';
```

### 3. Create incremental table with partitions
```sql
CREATE EXTERNAL TABLE event_data.incremental (
  id BIGINT,
  name STRING,
  mobile STRING,
  grade STRING,
  location STRING,
  data_source STRING,
  data_source_2 STRING,
  data_source_1 STRING,
  sheet_name STRING,
  timestamp STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION 's3://s3-joacademy-event-data-bucket-211/incremental/';
```

### 4. Load partitions
```sql
MSCK REPAIR TABLE event_data.incremental;
```

### 5. Query the data
```sql
-- Full load data
SELECT * FROM event_data.full_load LIMIT 10;

-- Incremental data
SELECT * FROM event_data.incremental WHERE year = 2026 AND month = 1 LIMIT 10;

-- Combined view
SELECT * FROM event_data.full_load
UNION ALL
SELECT id, name, mobile, grade, location, data_source, data_source_2, data_source_1, sheet_name, timestamp
FROM event_data.incremental;
```

## Webhook Setup (Optional)

If you need to expose the FastAPI webhook to Google Sheets Apps Script:

1. Ensure the API server is running (check `server.py`)
2. Use a tunneling service like ngrok to expose localhost:
   ```bash
   ngrok http 8000
   ```
3. Update your Google Apps Script with the ngrok URL

Without a tunnel, localhost endpoints won't be reachable from Google Sheets.

## Project Structure

```
.
├── airflow/
│   ├── dags/
│   │   ├── event_based_load.py      # Incremental row-by-row processing
│   │   ├── full_excel_load.py       # Full bulk load from all sheets
│   │   └── normalize_mobiles.py     # Legacy normalization DAG
│   ├── data/                         # Payload files and temp data
│   ├── logs/                         # Airflow execution logs (gitignored)
│   └── airflow.cfg                   # Airflow configuration
├── server.py                         # FastAPI webhook server
├── docker-compose.yml                # Docker orchestration
├── dockerfile                        # Airflow container image
├── .env.example                      # Template for environment variables
├── .gitignore                        # Git ignore rules
└── README.md                         # This file
```

## Troubleshooting

### AccessDenied errors
- Verify AWS credentials in `.env` are correct
- Ensure credentials have `s3:PutObject`, `s3:GetObject`, and `s3:ListBucket` permissions
- If using an assumed role, ensure `AWS_SESSION_TOKEN` is set
- Check bucket policy doesn't block your IAM user/role

### Airflow UI not accessible
- Wait 30-60 seconds after `docker-compose up -d` for initialization
- Check container logs: `docker logs workspace-airflow-1`
- Ensure port 8080 is not in use: `netstat -ano | findstr :8080` (Windows)

### DAG execution fails
- Check Airflow logs in the UI under the task run
- Verify S3 bucket exists and region matches configuration
- Ensure payload file paths are correct in DAG configuration

### PyArrow type errors
- Already handled: all object columns are explicitly cast to string before Parquet write
- If you encounter new type issues, check the DAG code for proper type conversion

## Security Notes

- Never commit `.env` file or AWS credentials to git
- Rotate AWS keys immediately if accidentally exposed
- Use IAM roles with minimal required permissions
- Consider using AWS Secrets Manager or Airflow Connections for credential management in production

## Contributors

Built for JO Academy event data management pipeline.
