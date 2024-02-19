import pandas as pd
import urllib.parse 
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
from io import BytesIO

# function to create connection to database
def create_connection(username, password, ip, port, database_name):
    try:
        params = urllib.parse.quote_plus(f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={ip},{port};DATABASE={database_name};UID={username};PWD={password}")
        connection_string = f"mssql+pyodbc:///?odbc_connect={params}"
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        print(f"Error creating connection: {e}")
        return None

# function to execute sql queries
def execute_sql_query(engine, sql_query):
    try:
        df = pd.read_sql_query(sql_query, engine)
        return df
    except Exception as e:
        print(f"Error executing SQL query: {e}")
        return None

# function to upload parquet to blob storage
def upload_to_azure_blob(df, storage_connection_string, container_name, file_name):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        # Write DataFrame to BytesIO as Parquet
        buffer = BytesIO()
        df.to_parquet(buffer, engine='pyarrow', compression='gzip', index=None)

        # Upload buffer to Azure Blob Storage with appropriate hierarchy
        blob_path = '/'.join(file_name.split('/')[:-1])  # Extract path without the filename
        blob_name = file_name.split('/')[-1]  # Extract filename
        container_client.upload_blob(name=blob_path+'/'+blob_name, data=buffer.getvalue())

        # print(f"Uploaded {file_name} to Azure Blob Storage.")
    except Exception as e:
        print(f"Error uploading to Azure Blob Storage: {e}")

# main function 
def main(database_name, start_date_str, end_date_str):
    try:
        # Database credentials
        username = 'sa'
        password = 'spts@3311'
        ip = 'localhost'
        port = '1433'
        azure_storage_connection_string='DefaultEndpointsProtocol=https;AccountName=wimetrixarchives;AccountKey=Sx0gn7kLgnrMQThX5VocxAv/hbFy4KNjf7muVvx8boySjHMadub/rquhjMcWO/ifWLMubjhfhiue+ASt4AVs3w==;EndpointSuffix=core.windows.net'
        azure_container_name='test'

        # Create database connection
        engine = create_connection(username, password, ip, port, database_name)
        if engine is None:
            return

        # Convert start and end date strings to datetime objects
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

        # Get list of user tables in the database
        user_tables_query = "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA IN ('Essentials','Data') AND TABLE_SCHEMA <> 'sys'"
        user_tables_df = execute_sql_query(engine, user_tables_query)

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
                    # print(sql_query)

                    # Execute the query and read the results into a DataFrame
                    df = execute_sql_query(engine, sql_query)
                    if df is not None and not df.empty:
                        # Define the file name with appropriate hierarchy
                        file_name = f"{database_name}/{current_date.strftime('%Y')}/{current_date.strftime('%m')}/{current_date.strftime('%d')}/{table_name_str}/{table_name_str}_{current_date.strftime('%Y_%m_%d')}.parquet"
                        upload_to_azure_blob(df, azure_storage_connection_string, azure_container_name, file_name)
                    
                    # Move to the next day
                    current_date += timedelta(days=1)
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    try:
        database_name = input("Enter Database Name: ")
        start_date_input = input("Enter start date (YYYY-MM-DD): ")
        end_date_input = input("Enter end date (YYYY-MM-DD): ")
        
        # call main function
        main(database_name, start_date_input, end_date_input)
    except Exception as e:
        print(f"An error occurred: {e}")
