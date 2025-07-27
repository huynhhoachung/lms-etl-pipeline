import json
import boto3
import pandas as pd
from sqlalchemy import create_engine, text, inspect
import os
import logging
from io import StringIO
from datetime import datetime
import pytz
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Initialize a session with AWS credentials
session = boto3.Session()

# Function to create a SQLAlchemy engine for the business tracking database
def create_db_engine():
    db_url = f"postgresql://{os.environ.get('BUSINESS_TRACKING_USER')}:{os.environ.get('BUSINESS_TRACKING_PASSWORD')}@" \
             f"{os.environ.get('BUSINESS_TRACKING_HOST')}:{os.environ.get('BUSINESS_TRACKING_PORT')}/" \
             f"{os.environ.get('BUSINESS_TRACKING_DBNAME')}?sslmode={os.environ.get('BUSINESS_TRACKING_SSLMODE')}"
    engine = create_engine(db_url) 
    logger.info("SQLAlchemy engine created for business tracking database.")
    return engine

# Function to retrieve the schema of the destination table
def get_table_schema(engine, schema, table_name):
    """Retrieve the schema of the destination table including column names and data types."""
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name, schema=schema)
    schema_info = {col['name']: col['type'] for col in columns}
    logger.info(f"Schema for table {table_name}: {schema_info}")
    return schema_info

# Function to convert DataFrame data types to match the destination table schema
def convert_dataframe_dtypes(df, schema_info):
    """Convert DataFrame data types to match the destination table schema."""
        # Remove '.0' from all columns ending with 'id'
    for column in df.columns:
        #if column.endswith('id') or column.endswith('code'):
        df[column] = df[column].apply(lambda x: str(x).replace('.0', '') if isinstance(x, float) else x)

    for column, dtype in schema_info.items():
        if column in df.columns:
            try:
                if 'INTEGER' in str(dtype) or 'BIGINT' in str(dtype):
                    df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
                    # Replace NaN with None (which translates to NULL in PostgreSQL)
                    df[column] = df[column].replace({np.nan: None})
                elif 'VARCHAR' in str(dtype) or 'TEXT' in str(dtype):
                    # Replace 'nan' with empty strings
                    df[column] = df[column].astype(str)
                    df[column].replace({'nan': ' '}, inplace=True)
                elif 'BOOLEAN' in str(dtype):
                    df[column] = df[column].astype(bool)
                elif 'DATE' in str(dtype) or 'TIMESTAMP' in str(dtype):
                    df[column] = pd.to_datetime(df[column], errors='coerce').dt.tz_localize('UTC')
                elif 'JSON' in str(dtype):
                    df[column] = df[column].apply(lambda x: json.dumps(x) if not pd.isna(x) else None)
                logger.info(f"Converted column '{column}' to {dtype}.")
            except Exception as e:
                logger.error(f"Failed to convert column '{column}' to {dtype}: {e}", exc_info=True)
                raise Exception(f"Failed to convert column '{column}' to {dtype}: {str(e)}")


# Function to upsert a DataFrame into a PostgreSQL table
def upsert_dataframe_to_postgres(df, table_name, engine, unique_key, schema):
    """Upsert a DataFrame into a PostgreSQL table."""
    with engine.connect() as connection:
        try:
            # Ensure schema and table names are properly quoted to avoid syntax errors
            upsert_stmt = text(f"""
                INSERT INTO "{schema}"."{table_name}" ({', '.join([f'"{col}"' for col in df.columns])})
                VALUES ({', '.join([f':{col}' for col in df.columns])})
                ON CONFLICT ("{unique_key}") DO UPDATE SET
                {', '.join([f'"{col}" = EXCLUDED."{col}"' for col in df.columns if col != unique_key])}
            """)
            with connection.begin() as transaction:
                connection.execute(upsert_stmt, df.to_dict(orient='records'))
            logger.info(f"Data upserted to {table_name} successfully.")
        except Exception as e:
            logger.error(f"An error occurred: {e}", exc_info=True)
            raise Exception(f"An error occurred during upsert: {str(e)}")

# Send an SNS notification with the given message
def send_sns_notification(message):
    """Send an SNS notification with the given subject and message."""
    sns_client = session.client('sns')
    try:
        sns_client.publish(
            TopicArn=os.environ.get("SNS_TOPIC_ARN"),
            Subject='Slack Notification',
            Message= f'6fr-s3-to-rds failed: {message}'
        )
        logger.info("SNS notification sent successfully.")
    except Exception as e:
        logger.error(f"Failed to send SNS notification: {e}", exc_info=True)
        raise Exception(f"Failed to send SNS notification: {str(e)}")

# Function to retrieve data from S3
def retrieve_data_from_s3():
    """Retrieve data from S3 and return as a DataFrame."""
    s3_client = session.client('s3')
    bucket_name = os.environ.get("S3_BUCKET_NAME")
    file_path = os.environ.get("S3_DEPARTMENT_MEMBERS_PATH")
    
    response = s3_client.get_object(Bucket=bucket_name, Key=file_path)
    data = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(data))
    logger.info("Data retrieved from S3 successfully.")
    return df

# Function to parse ISO 8601 datetime strings in multiple columns of a DataFrame
def parse_datetime_columns(df, column_names):
    """Parse ISO 8601 datetime strings in the specified columns of a DataFrame."""
    for column_name in column_names:
        try:
            df[column_name] = pd.to_datetime(df[column_name], format='%m-%d-%Y %H:%M:%S', errors='coerce')
            if df[column_name].dt.tz is None:
                df[column_name] = df[column_name].dt.tz_localize('UTC')
            else:
                df[column_name] = df[column_name].dt.tz_convert('UTC')
            # Convert NaT to None
            df[column_name] = df[column_name].replace({pd.NaT: None})
            logger.info(f"Parsed datetime column '{column_name}' successfully.")
        except Exception as e:
            logger.error(f"Failed to parse datetime column '{column_name}': {e}", exc_info=True)
            raise Exception(f"Failed to parse datetime column '{column_name}': {str(e)}")

def lambda_handler(event, context):
    """Lambda function to retrieve data from S3 and upsert it to RDS"""
    try:
        # Retrieve data from S3
        df = retrieve_data_from_s3()

        # Create database engine
        engine = create_db_engine()

        # Retrieve schema of the destination table
        schema_info = get_table_schema(engine, os.environ.get("BUSINESS_TRACKING_SCHEMA"), 'department_members')

        # Convert DataFrame data types to match the destination table schema
        convert_dataframe_dtypes(df, schema_info)

        # Parse datetime column
        parse_datetime_columns(df, ['date_hired','date_terminated','date_edited','date_added','last_login_date'])
        # Upsert data to RDS
        upsert_dataframe_to_postgres(df, 'department_members', engine, 'lms_user_id', os.environ.get("BUSINESS_TRACKING_SCHEMA"))

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "6fr data upserted to rds successfully!"
            }),
        }
    except Exception as e:
        error_message = str(e)
        logger.error(error_message, exc_info=True)
        send_sns_notification(error_message)
        return {
            'body': error_message
        }