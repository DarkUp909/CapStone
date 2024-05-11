# Import libraries
import os
import boto3
import requests
import snowflake.connector as sf
import toml
from dotenv import load_dotenv 

# This function is for connection to snowflake by environment variables
def connection_to_snowflake(schema):
    conn = sf.connect(user =os.getenv('USER'), password = os.getenv('PASSWORD'), \
                 account = os.getenv('ACCOUNT'), warehouse= os.getenv('WAREHOUSE'), \
                  database= os.getenv('DATABASE'),  schema=schema,  role= os.getenv('ROLE') )
    return conn
# This function saves the data to the distionation  
def save_data_to_destionation(response, destination_folder, file_name):
        file_path = os.path.join(destination_folder, file_name)
        with open(file_path, 'wb') as file:
            file.write(response.content)
        
        with open(file_path, 'r') as file:
            file_content = file.read()
            print("File Content:")
            print(file_content)
            
# This is the function to add format for the csv file             
def csv_file_format(cursor):
        create_csv_format = f"CREATE or REPLACE FILE FORMAT COMMA_CSV TYPE ='CSV' FIELD_DELIMITER = ',';"
        cursor.execute(create_csv_format)
# This is the function to add stage              
def stage(cursor, stage_name ,local_file_path):
        create_stage_query = f"CREATE OR REPLACE STAGE {stage_name} FILE_FORMAT =COMMA_CSV"
        cursor.execute(create_stage_query)
        list_stage_query = f"LIST @{stage_name}"
        cursor.execute(list_stage_query)
        copy_into_stage_query = f"PUT 'file://{local_file_path}' @{stage_name}"
        cursor.execute(copy_into_stage_query)

def lambda_handler(event, context):
    
    
    
    # The connection to the toml file
    app_config = toml.load('config.toml')
    url = app_config['url']['url']
    destination_folder = app_config['destination']['destination_folder']
    file_name = app_config['destination']['file_name']


    # This function is to connect between environment variables and lambda code
    load_dotenv()

    local_file_path = '/tmp/inventory.csv'
    schema = 'RAW'
    table = 'inventory'
    stage_name = 'inv_Stage'
    
    # The snowflake connection
    conn = connection_to_snowflake(schema)
    cursor = conn.cursor()
   

    # Download the data from the API endpoint
    response = requests.get(url)
    response.raise_for_status()
    
    # Save the data to the destination file in /tmp directory
    save_data_to_destionation(response, destination_folder, file_name)
    
    # Use schema
    use_schema = f"use schema {schema};"
    cursor.execute(use_schema)
    
    # Create csv file format
    csv_file_format(cursor)
    
    # Use stage to download the data into it 
    stage(cursor, stage_name, local_file_path)
    
    # Truncate table
    truncate_table = f"truncate table {schema}.{table};"  
    cursor.execute(truncate_table)    


    # Load the data from the stage into a table (example)
    copy_into_query = f"COPY INTO {schema}.{table} FROM @{stage_name}/{file_name} FILE_FORMAT =COMMA_CSV on_error='continue';"  
    cursor.execute(copy_into_query)


    print("File uploaded to Snowflake successfully.")


    return {
        'statusCode': 200,
        'body': 'File downloaded and uploaded to Snowflake successfully.'
    }
