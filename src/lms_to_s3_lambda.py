import pandas as pd
import urllib3
import json
import logging
import boto3
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Initialize a session with AWS credentials
session = boto3.Session()

# Obtain an access token from the LMS API use to extract the data
def get_access_token():
    url = f"{os.environ.get('REST_API_URL')}/authenticate"
    payload = json.dumps({
        "username": os.environ.get("LMS_USERNAME"),
        "password": os.environ.get("LMS_PASSWORD"),
        "privateKey": os.environ.get("LMS_PRIVATE_KEY")
    })
    
    headers = {"x-api-key": os.environ.get("LMS_PRIVATE_KEY"), "x-api-version":"2"}
    http = urllib3.PoolManager()
    response = http.request('POST', url, body=payload, headers=headers)
    
    if response.status == 200:
        logger.info("Retrieved access token successfully!")
        return json.loads(response.data.decode('utf-8'))
    else:
        logger.error("Failed to obtain access token in get_access_token", exc_info=True)
        raise Exception("Failed to obtain access token")

# Fetch data from the LMS API for a given endpoint
def extract_data(endpoint, access_token, params=None):
    url = f"{os.environ.get('REST_API_URL')}/{endpoint}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-api-key": os.environ.get("LMS_PRIVATE_KEY"),
        "x-api-version": "2"
    }
    http = urllib3.PoolManager()
    response = http.request('GET', url, headers=headers, fields=params)
    
    if response.status == 200:
        logger.info(f"Retrieved data from {endpoint} endpoint successfully!")
        return json.loads(response.data.decode('utf-8'))
    else:
        logger.error("Failed to retrieve data in extract_data", exc_info=True)
        raise Exception("Failed to retrieve data")

# Determine the API endpoint based on the data type and id(optional)
def get_api_endpoint(data, additional_id=''):
    endpoints = {
        "enrollments": f"courses/{os.environ.get('COURSE_ID')}/sessions/{additional_id}/enrollments",
        "sessions": f"courses/{os.environ.get('COURSE_ID')}/sessions",
        "courses": f"courses/{os.environ.get('COURSE_ID')}",
        "users": f"users/{additional_id}",
        "list_users": f"users"
    }
    
    url = endpoints.get(data)
    if url is None:
        logger.error("Invalid data type in get_api_endpoint", exc_info=True)
    return url
        
def process_users_data(users_data):
    """
    Processes the users_data by removing specified columns and flattening the users.

    Parameters:
    users_data (dict): The data containing user information.

    Returns:
    pd.DataFrame: A DataFrame with the processed user data.
    """
    # Remove specified columns
    for key in ['totalItems', 'limit', 'offset', 'returnedItems']:
        users_data.pop(key, None)

    # Flatten the users data
    users_df = pd.json_normalize(users_data['users'])

    return users_df

# Function to convert column names of a DataFrame
def convert_column_names(df):
    """
    Converts column names of a DataFrame based on a predefined conversion dictionary.
    
    Parameters:
    df (pd.DataFrame): The DataFrame whose columns need to be renamed.
    
    Returns:
    pd.DataFrame: A DataFrame with renamed columns.
    """
    try:
        conversion_dict = {
            'id': 'lms_user_id',
            'departmentId': 'department_id',
            'firstName': 'first_name',
            'middleName': 'middle_name',
            'lastName': 'last_name',
            'username': 'user_name',
            'password': 'password',
            'emailAddress': 'email_address',
            'externalId': 'illum_id',
            'ccEmailAddresses': 'cc_email_addresses',
            'languageId': 'language_id',
            'gender': 'gender',
            'address': 'address',
            'address2': 'address_2',
            'city': 'city',
            'provinceId': 'province_id',
            'countryId': 'country_id',
            'postalCode': 'postal_code',
            'phone': 'phone',
            'employeeNumber': 'employee_number',
            'location': 'location',
            'jobTitle': 'job_title',
            'referenceNumber': 'reference_number',
            'dateHired': 'date_hired',
            'dateTerminated': 'date_terminated',
            'dateEdited': 'date_edited',
            'dateAdded': 'date_added',
            'lastLoginDate': 'last_login_date',
            'notes': 'notes',
            'roleIds': 'role_ids',
            'activeStatus': 'active_status',
            'isLearner': 'is_learner',
            'isAdmin': 'is_admin',
            'isInstructor': 'is_instructor',
            'isManager': 'is_manager',
            'supervisorId': 'supervisor_id',
            'hasUsername': 'has_user_name'
        }
        logger.info("Convert all the columns name")
        return df.rename(columns=conversion_dict)
    except Exception as e:
        logger.error(f"Error converting column names: {e}", exc_info=True)
        raise Exception(f"Error converting column names: {str(e)}")

# Function to consolidate custom fields into a single column
def consolidate_custom_fields(df):
    """
    Consolidates custom fields into a single JSON column.
    
    Parameters:
    df (pd.DataFrame): The DataFrame containing custom fields.
    
    Returns:
    pd.DataFrame: A DataFrame with a single 'custom_fields' column.
    """
    try:
        custom_fields_columns = [col for col in df.columns if col.startswith('customFields.')]
        df['custom_fields'] = df[custom_fields_columns].apply(lambda row: row.dropna().to_dict(), axis=1)
        df = df.drop(columns=custom_fields_columns)
        logger.info("Consolilate all the json columns into 1")
        return df
    except Exception as e:
        logger.error(f"Error consolidating custom fields: {e}", exc_info=True)
        raise Exception(f"Error consolidating custom fields: {str(e)}")

# Function to upload data to an S3 bucket
def upload_to_s3(data, bucket_name, file_path):
    """Upload data to an S3 bucket."""
    s3_client = session.client('s3')
    try:
        s3_client.put_object(Bucket=bucket_name, Key=file_path, Body=data)
        logger.info(f"Uploaded data to s3://{bucket_name}/{file_path}")
    except Exception as e:
        logger.error(f"Failed to upload data to S3: {e}", exc_info=True)
        raise Exception(f"Failed to upload data to S3: {str(e)}")

# Send an SNS notification with the given message
def send_sns_notification(message):
    """Send an SNS notification with the given subject and message."""
    sns_client = session.client('sns')
    try:
        sns_client.publish(
            TopicArn=os.environ.get("SNS_TOPIC_ARN"),
            Subject='Slack Notification',
            Message= f'6fr-etl-lambda failed: {message}'
        )
        logger.info("SNS notification sent successfully.")
    except Exception as e:
        logger.error(f"Failed to send SNS notification: {e}", exc_info=True)
        raise Exception(f"Failed to send SNS notification: {str(e)}")

# Main handler function for the AWS Lambda
def lambda_handler(event, context):
    try:
        access_token = get_access_token()

        # Add filter parameter for departmentId
        params = {
            "_filter": "departmentId eq guid'department_id'"
        }
        users_data = extract_data(get_api_endpoint("list_users"), access_token, params=params)
        
        full_users_df = process_users_data(users_data)
        
        # Convert column names
        full_users_df = convert_column_names(full_users_df)
        
        # Consolidate custom fields into a single column
        full_users_df = consolidate_custom_fields(full_users_df)

        # Convert DataFrame to CSV and upload to S3
        csv_data = full_users_df.to_csv(index=False)
        upload_to_s3(csv_data, os.environ.get("S3_BUCKET_NAME"), os.environ.get("S3_DEPARTMENT_MEMBERS_PATH"))

        return {
            'statusCode': 200,
            'body': '6fr data upload to s3 successfully!'
        }

    except Exception as e:
        error_message = str(e)
        logger.error(error_message, exc_info=True)
        send_sns_notification(error_message)
        return {
            'body': error_message
        }