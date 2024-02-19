import pandas as pd
from azure.storage.blob import BlobServiceClient
import io
import glob

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

# Example usage:
azure_storage_connection_string = 'DefaultEndpointsProtocol=https;AccountName=wimetrixarchives;AccountKey=Sx0gn7kLgnrMQThX5VocxAv/hbFy4KNjf7muVvx8boySjHMadub/rquhjMcWO/ifWLMubjhfhiue+ASt4AVs3w==;EndpointSuffix=core.windows.net'
container_name = 'cfl'

# Define the directory pattern
base_dir = 'CFLSooperWizer/2022/04/*/'

# Use glob to find all files matching the pattern
files = glob.glob(base_dir + '/*.parquet')

# Read each Parquet file and print its content
for file_path in files:
    df = read_parquet_from_azure_blob(azure_storage_connection_string, container_name, file_path)
    if df is not None:
        print(file_path)
        print(df)




# import pandas as pd
# from azure.storage.blob import BlobServiceClient
# import io

# def read_parquet_from_azure_blob(storage_connection_string, container_name, file_path):
#     try:
#         blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
#         blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_path)
        
#         # Download the blob's content as bytes
#         blob_data = blob_client.download_blob().readall()
        
#         # Read Parquet file from bytes
#         df = pd.read_parquet(io.BytesIO(blob_data))
        
#         return df
#     except Exception as e:
#         print(f"Error reading Parquet file from Azure Blob Storage: {e}")
#         return None

# # Example usage:
# azure_storage_connection_string = 'DefaultEndpointsProtocol=https;AccountName=wimetrixarchives;AccountKey=Sx0gn7kLgnrMQThX5VocxAv/hbFy4KNjf7muVvx8boySjHMadub/rquhjMcWO/ifWLMubjhfhiue+ASt4AVs3w==;EndpointSuffix=core.windows.net'
# container_name = 'cfl'
# file_path = 'CFLSooperWizer/2022/04/06/CutReport/CutReport_2022_04_06.parquet'

# df = read_parquet_from_azure_blob(azure_storage_connection_string, container_name, file_path)

# if df is not None:
#     print(df)

