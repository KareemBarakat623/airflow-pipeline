"""
AWS Glue utility functions for database and crawler management.
"""
import boto3
import yaml
import os
from botocore.exceptions import ClientError


def load_config():
    """Load configuration from config.yaml."""
    config_paths = [
        "/opt/airflow/config.yaml",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "config.yaml")),
    ]

    for config_path in config_paths:
        if os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)

    raise FileNotFoundError(
        "config.yaml not found. Expected at /opt/airflow/config.yaml or project root."
    )


def get_glue_client():
    """Create and return AWS Glue client with credentials from config."""
    config = load_config()
    
    glue_config = {
        'region_name': config['aws']['region'],
        'aws_access_key_id': config['aws']['access_key_id'],
        'aws_secret_access_key': config['aws']['secret_access_key']
    }
    
    if config['aws'].get('session_token'):
        glue_config['aws_session_token'] = config['aws']['session_token']
    
    return boto3.client('glue', **glue_config)


def create_glue_database_if_not_exists():
    """Create Glue database if it doesn't already exist."""
    config = load_config()
    glue_client = get_glue_client()
    database_name = config['glue']['database_name']
    
    try:
        glue_client.get_database(Name=database_name)
        print(f"Glue database '{database_name}' already exists.")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            print(f"Creating Glue database: {database_name}")
            try:
                glue_client.create_database(
                    DatabaseInput={
                        'Name': database_name,
                        'Description': 'Database for Airflow event data stored in S3 as Parquet'
                    }
                )
                print(f"Successfully created Glue database: {database_name}")
                return True
            except Exception as create_err:
                print(f"Error creating database: {create_err}")
                return False
        else:
            print(f"Error checking database: {e}")
            return False


def create_crawler_if_not_exists(crawler_type='full_load'):
    """
    Create Glue crawler if it doesn't already exist.
    
    Args:
        crawler_type: Either 'full_load' or 'incremental'
    """
    config = load_config()
    glue_client = get_glue_client()
    
    crawler_config = config['glue']['crawlers'][crawler_type]
    crawler_name = crawler_config['name']
    database_name = config['glue']['database_name']
    role_arn = config['glue']['service_role_arn']
    s3_path = crawler_config['s3_path']
    
    try:
        glue_client.get_crawler(Name=crawler_name)
        print(f"Crawler '{crawler_name}' already exists.")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            print(f"Creating Glue crawler: {crawler_name}")
            try:
                glue_client.create_crawler(
                    Name=crawler_name,
                    Role=role_arn,
                    DatabaseName=database_name,
                    Description=crawler_config['description'],
                    Targets={
                        'S3Targets': [
                            {
                                'Path': s3_path
                            }
                        ]
                    },
                    SchemaChangePolicy={
                        'UpdateBehavior': 'LOG',
                        'DeleteBehavior': 'LOG'
                    },
                    RecrawlPolicy={
                        'RecrawlBehavior': 'CRAWL_NEW_FOLDERS_ONLY'
                    },
                    Configuration='{"Version":1.0,"CrawlerOutput":{"Partitions":{"AddOrUpdateBehavior":"InheritFromTable"}}}'
                )
                print(f"Successfully created crawler: {crawler_name}")
                return True
            except Exception as create_err:
                print(f"Error creating crawler: {create_err}")
                return False
        else:
            print(f"Error checking crawler: {e}")
            return False


def start_crawler(crawler_type='full_load'):
    """
    Start a Glue crawler to catalog new data.
    
    Args:
        crawler_type: Either 'full_load' or 'incremental'
    """
    config = load_config()
    glue_client = get_glue_client()
    
    crawler_name = config['glue']['crawlers'][crawler_type]['name']
    
    try:
        # Check if crawler is already running
        response = glue_client.get_crawler(Name=crawler_name)
        state = response['Crawler']['State']
        
        if state == 'RUNNING':
            print(f"Crawler '{crawler_name}' is already running. Skipping trigger.")
            return True
        
        # Start the crawler
        glue_client.start_crawler(Name=crawler_name)
        print(f"Successfully started Glue crawler: {crawler_name}")
        return True
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'CrawlerRunningException':
            print(f"Crawler '{crawler_name}' is already running.")
            return True
        else:
            print(f"Error starting crawler '{crawler_name}': {e}")
            return False
    except Exception as e:
        print(f"Unexpected error starting crawler: {e}")
        return False


def setup_glue_infrastructure():
    """
    Setup complete Glue infrastructure: database and both crawlers.
    This should be run once during initial setup.
    """
    print("Setting up Glue infrastructure...")
    
    # Create database
    if not create_glue_database_if_not_exists():
        print("Failed to create database. Aborting setup.")
        return False
    
    # Create full load crawler
    if not create_crawler_if_not_exists('full_load'):
        print("Failed to create full load crawler.")
        return False
    
    # Create incremental crawler
    if not create_crawler_if_not_exists('incremental'):
        print("Failed to create incremental crawler.")
        return False
    
    print("Glue infrastructure setup completed successfully!")
    return True
