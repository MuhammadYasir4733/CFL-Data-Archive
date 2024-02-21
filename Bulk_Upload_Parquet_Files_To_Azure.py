import os
import shutil
import json
import pandas as pd
import urllib.parse 
from io import BytesIO
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient

# Function to read configuration from JSON file
def read_config(filename):
    with open(filename, 'r') as f:
        config = json.load(f)
    return config

# Function to create connection to database
def create_connection(config):
    try:
        db_config = config['database']
        params = urllib.parse.quote_plus(f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_config['ip']},{db_config['port']};DATABASE={db_config['databasename']};UID={db_config['username']};PWD={db_config['password']}")
        connection_string = f"mssql+pyodbc:///?odbc_connect={params}"
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        print(f"Error creating connection: {e}")
        return None

# Function to execute sql queries
def execute_sql_query(engine, sql_query):
    try:
        df = pd.read_sql_query(sql_query, engine)
        return df
    except Exception as e:
        print(f"Error executing SQL query: {e}")
        return None

# Function to upload files recursively to Azure Blob Storage
def upload_directory_to_blob(source_dir, storage_connection_string, container_name):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        for root, _, files in os.walk(source_dir):
            for file in files:
                local_path = os.path.join(root, file)
                blob_path = os.path.relpath(local_path, source_dir).replace("\\", "/")  # Convert path to Azure format
                blob_client = container_client.get_blob_client(blob_path)
                
                with open(local_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                    
                print(f"Uploaded {local_path} to Azure Blob Storage as {blob_path}.")
    except Exception as e:
        print(f"Error uploading to Azure Blob Storage: {e}")


# Main function 
def main(start_date_str, end_date_str):
    try:
        
        # Connection Credentials
        config = read_config('config.json')
        
        # Extract database connection parameters from config
        engine = create_connection(config)
        if engine is None:
            return
        
        # Extract Azure storage connection parameters from config
        azure_storage_connection_string = config['azure_storage']['connection_string']
        azure_container_name = config['azure_storage']['container_name']

        # Convert start and end date strings to datetime objects
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

        # Create a temporary directory to store Parquet files
        temp_dir = "temp_files"

        # Get list of user tables in the database
        user_tables_query = "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA IN ('Essentials','Data') AND TABLE_SCHEMA <> 'sys'"
        user_tables_df = execute_sql_query(engine, user_tables_query)

        # if gets tables then 
        if user_tables_df is not None:
            # Iterate over each table
            for index, row in user_tables_df.iterrows():
                table_schema_str = row['TABLE_SCHEMA']
                table_name_str = row['TABLE_NAME']
                
                # Iterate over each day within the specified range
                current_date = start_date
                while current_date <= end_date:
                    # SQL query for the given day
                    sql_query = f"SELECT * FROM [{table_schema_str}].[{table_name_str}] WHERE CONVERT(DATE, CreatedAt) = '{current_date.strftime('%Y-%m-%d')}'"
                    
                    # Execute the query and read the results into a DataFrame
                    df = execute_sql_query(engine, sql_query)
                    
                    if df is not None:
                        # Define the file name with appropriate hierarchy
                        file_name = f"{current_date.strftime('%Y')}/{current_date.strftime('%m')}/{current_date.strftime('%d')}/{table_name_str}/{table_name_str}_{current_date.strftime('%Y_%m_%d')}.parquet"
                        
                        # Create directories if they do not exist
                        os.makedirs(os.path.join(temp_dir, current_date.strftime('%Y'), current_date.strftime('%m'), current_date.strftime('%d'), table_name_str), exist_ok=True)

                        # Write DataFrame to Parquet file
                        parquet_filename = os.path.join(temp_dir, file_name)
                        df.to_parquet(parquet_filename, engine='pyarrow', compression='gzip', index=None)

                    # File Name
                    print(f"File Name :  {table_name_str}_{current_date}")

                    # Move to the next day
                    current_date += timedelta(days=1)
                    
        # Upload files to Azure Blob Storage with directory structure
        upload_directory_to_blob(temp_dir, azure_storage_connection_string, azure_container_name)

        # Remove temp_files directory and its subdirectories
        shutil.rmtree(temp_dir)

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    try:

        start_date_input = input("Enter start date (YYYY-MM-DD): ")
        end_date_input = input("Enter end date (YYYY-MM-DD): ")
        
        # call main function
        main(start_date_input, end_date_input)
    except Exception as e:
        print(f"An error occurred: {e}")
