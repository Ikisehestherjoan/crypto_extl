# import libraries
from util import generate_schema, get_redshift_connection,\
execute_sql, create_bucket,create_database_conn
import main
import pandas as pd
import requests
import boto3
from datetime import datetime
import logging
from io import StringIO
import psycopg2
from botocore.exceptions import NoCredentialsError
import ast
from dotenv import dotenv_values
dotenv_values()

# Get credentials from environment variable file
config = dotenv_values('.env')

# # Create a boto3 s3 client for bucket operations
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

def get_data_from_api():
    url = config.get('URL')
    headers = ast.literal_eval(config.get('HEADERS'))
    querystring = ast.literal_eval(config.get('QUERYSTRING'))

    try:
        # Send a request to Rapid API and handle potential connection errors
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()  # Raise an error for bad responses (4xx or 5xx)

        # Parse the response as JSON
        data = response.json()
         #print(data)

        # Extract relevant data
        crypto_data = data.get('data', {}).get('coins', [])

        # Define columns to keep
        columns_to_keep = ['symbol', 'name', 'price', 'rank']
        crypto_df = pd.DataFrame(crypto_data)[columns_to_keep]
        #crypto_df.to_csv('cryptodata.csv', index=False)
        #print(crypto_df)
        return crypto_df
    except requests.RequestException as e:
        print(f'Error in API request: {e}')
    except KeyError as e:
        print(f'Error extracting data from response: {e}')
    except Exception as e:
        print(f'Unexpected error: {e}')


# Transform the data
def transform_data():
    info = get_data_from_api()
    info['price'] = info['price'].apply(lambda x: float(x)) # convert string column to float value
    return info

# transformed_data = transform_data() # Transform the data
# transformed_data.to_csv('transformed_cryp.csv', index = False)




# create_database_conn()

# # ====== BUCKET CREATION =====


def load_csv_data():
    try:
        s3, bucket_name, region = create_database_conn()

        # Specify the local path to your JSON file
        local_file_path = 'transformed_cryp.csv'

        # Specify the S3 key (file name in the S3 bucket)
        file_name = f"{datetime.now().strftime('%Y-%m-%d-%H-%M')}" # Create a file name
        s3_key = f'{bucket_name}/{file_name}/{local_file_path}'

        # Upload the JSON file to S3
        s3.upload_file(local_file_path, bucket_name, s3_key)
        print(f"CSV file '{local_file_path}' uploaded to S3 bucket '{bucket_name}' as '{s3_key}'.")
    except NoCredentialsError:
        print("Credentials not available or incorrect.")
    except Exception as e:
        print(f"Error: {e}")

# Call the functions
# create_bucket()
# load_csv_data()
# print('JSON FILE FINALLY UPLOADED TO S3 BUCKECT')
        

       
# # LOADING TO REDSHIFT

        
# COPY INTO THE AWS_REDSHIFT


def load_to_redshift(tranbucket_name, folder_name, file_name, tablename):
    iam_role = config.get('IAM_ROLE')
    conn = get_redshift_connection()
    file_path = f's3://{tranbucket_name}/{tranbucket_name}/{folder_name}/{file_name}.csv'
    copy_query = f"""
        COPY {tablename}
        FROM '{file_path}'
        IAM_ROLE '{iam_role}'
        CSV
        DELIMITER ','
        QUOTE '"'
        ACCEPTINVCHARS
        TIMEFORMAT 'auto'
        IGNOREHEADER 1
    """
    try:
        execute_sql(copy_query, conn) 
        print('Data successfully loaded to Redshift')
        logging.info('Data successfully loaded to Redshift')
    except Exception as e:
        print(f'Error loading data to Redshift: {e}')
        logging.error(f'Error loading data to Redshift: {e}')





