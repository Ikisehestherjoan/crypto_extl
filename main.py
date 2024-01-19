
# Import libraries
from time import sleep
from datetime import datetime
from util import generate_schema, execute_sql
from etl import get_data_from_api, load_csv_data,load_to_redshift, transform_data

# Main method to run the pipeline
def main():
    bucket_name = 'cryptobuc'
    tablename = 'cryptodata'
    folder_name ='transformed_cryp'
    file_name='2024-01-19-00-13'
    counter = 0
    # A while loop to send 5 requests to the API
    while counter < 3:
        data =get_data_from_api() # Extract data from API
        transformed_data = transform_data() # Transform the data
        load_csv_data(transformed_data, bucket_name)
        counter+= 1
        sleep(10) # Wait 30 seconds before sending another request to the API
    print('API data pulled and written written to s3 bucket')

    create_table_query = generate_schema(data, tablename) # generate ddl of target table
    # Create a taget table for in Redshift
    load_to_redshift(tablename, folder_name,file_name, tablename)
    
# main()