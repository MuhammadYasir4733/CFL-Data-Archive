import os
import pandas as pd
import urllib.parse 
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


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

# function to write data into csv files
def export_data_to_file(df,output_dir,date,table_name_str):
    try:
        ### Export DataFrame to CSV
        # csv_file_name = os.path.join(output_dir, f"{table_name_str}_{date.strftime('%Y_%m_%d')}.csv")
        # df.to_csv(csv_file_name, index=False)

        # Export DataFrame to Parquet
        parquet_file_name = os.path.join(output_dir, f"{table_name_str}_{date.strftime('%Y_%m_%d')}.parquet")
        df.to_parquet(path=parquet_file_name, engine='pyarrow', compression='gzip', index=None, partition_cols=None, storage_options=None)
    except Exception as e:
        print(f"Error exporting data to file: {e}")

# function to upload parquet to blob storage
def upload_to_azure_blob(local_file_path, storage_connection_string, container_name):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        # Extracting relative path from the absolute path
        relative_path = os.path.relpath(local_file_path, start=base_path)

        # Uploading blob to Azure Storage
        with open(local_file_path, "rb") as data:
            container_client.upload_blob(name=relative_path, data=data)

        print(f"Uploaded {relative_path} to Azure Blob Storage.")
    except Exception as e:
        print(f"Error uploading to Azure Blob Storage: {e}")




# main function 
def main(base_path,database_name, start_date_str, end_date_str):
    try:
        # Database credentials
        username = 'sa'
        password = 'Admin@3311'
        ip = 'localhost'
        port = '1437'
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
                        # Define the desired path
                        full_path = os.path.join(base_path,database_name, current_date.strftime("%Y"), current_date.strftime("%m"), current_date.strftime("%d"))

                        # Path for table output directory in day
                        table_output_dir = os.path.join(full_path, table_name_str)

                        # Create directories {BasePath\\DatabaseName\\Year\\Month\\Day\\TableName}
                        if not os.path.exists(table_output_dir):
                            os.makedirs(table_output_dir)

                        # Call function to export data in file
                        export_data_to_file(df, table_output_dir, current_date, table_name_str)

                        parquet_file_path = os.path.join(table_output_dir, f"{table_name_str}_{current_date.strftime('%Y_%m_%d')}.parquet")
                        upload_to_azure_blob(parquet_file_path, azure_storage_connection_string, azure_container_name)

                        # print(f"Table Here {table_name_str} : ")
                    
                    # Move to the next day
                    current_date += timedelta(days=1)
            #     print("End of While Loop")
            # print("End of Outer for Loop")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    try:
        base_path= input("Enter Base Path : ")
        database_name = input("Enter Database Name: ")
        start_date_input = input("Enter start date (YYYY-MM-DD): ")
        end_date_input = input("Enter end date (YYYY-MM-DD): ")
        
        # call main function
        main(base_path,database_name, start_date_input, end_date_input)


    except Exception as e:
        print(f"An error occurred: {e}")

