import pandas as pd
from azure.storage.blob import BlobServiceClient
import io

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

# Connection string of blob
azure_storage_connection_string = 'DefaultEndpointsProtocol=https;AccountName=wimetrixarchives;AccountKey=Sx0gn7kLgnrMQThX5VocxAv/hbFy4KNjf7muVvx8boySjHMadub/rquhjMcWO/ifWLMubjhfhiue+ASt4AVs3w==;EndpointSuffix=core.windows.net'
container_name = 'cfl'

# Parquet file path
file_path = '2023/03/10/PieceWiseScan/PieceWiseScan_2023_03_10.parquet'

df = read_parquet_from_azure_blob(azure_storage_connection_string, container_name, file_path)

if df is not None:
    print(df)

