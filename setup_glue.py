#!/usr/bin/env python3
"""
Setup script to initialize AWS Glue infrastructure:
- Create Glue database
- Create crawlers for full load and incremental data

Run this script once before running the pipeline for the first time.
"""

import sys
import os

# Add the dags directory to the path so we can import glue_utils
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'airflow', 'dags'))

from glue_utils import setup_glue_infrastructure


if __name__ == "__main__":
    print("=" * 60)
    print("AWS Glue Infrastructure Setup")
    print("=" * 60)
    print()
    print("This script will create:")
    print("  1. Glue database: airflow_event_data_db")
    print("  2. Full load crawler: airflow-full-load-crawler")
    print("  3. Incremental crawler: airflow-incremental-crawler")
    print()
    print("Using configuration from config.yaml")
    print()
    
    try:
        success = setup_glue_infrastructure()
        
        if success:
            print()
            print("=" * 60)
            print("✓ Setup completed successfully!")
            print("=" * 60)
            print()
            print("Next steps:")
            print("  1. Run your Airflow DAGs to load data to S3")
            print("  2. Crawlers will automatically catalog the data")
            print("  3. Query the data using AWS Athena")
            print()
            sys.exit(0)
        else:
            print()
            print("=" * 60)
            print("✗ Setup failed. Please check the error messages above.")
            print("=" * 60)
            sys.exit(1)
            
    except Exception as e:
        print()
        print("=" * 60)
        print(f"✗ Error during setup: {e}")
        print("=" * 60)
        sys.exit(1)
