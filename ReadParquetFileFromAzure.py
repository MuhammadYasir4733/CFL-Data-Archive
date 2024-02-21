import io
import json
import pandas as pd
from azure.storage.blob import BlobServiceClient

# Function to read configuration from JSON file
def read_config(filename):
    with open(filename, 'r') as f:
        config = json.load(f)
    return config

def read_parquet_from_azure_blob(storage_connection_string, container_name, file_path):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_path)
        
        # Download the blob's content as bytes
        blob_data = blob_client.download_blob().readall()
        
        # Read Parquet file from bytes
        df = pd.read_parquet(io.BytesIO(blob_data))
        
        return df
    except Exception as e:
        print(f"Error reading Parquet file from Azure Blob Storage: {e}")
        return None


# Connection Credentials
config = read_config('config.json')

# Extract Azure storage connection parameters from config
azure_storage_connection_string = config['azure_storage']['connection_string']
azure_container_name = config['azure_storage']['container_name']

# Parquet file path
file_path = '2023/11/11/PieceWiseScan/PieceWiseScan_2023_11_11.parquet'

df = read_parquet_from_azure_blob(azure_storage_connection_string, azure_container_name, file_path)

if df is not None:
    print(df)

