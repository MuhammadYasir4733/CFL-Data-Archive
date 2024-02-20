from pyspark.sql import SparkSession

appName = "PySpark Parquet Example"
master = "local"

# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

# https://wimetrixarchives.blob.core.windows.net/cfl/2023/04/15/PieceWiseScan/PieceWiseScan_2023_04_15.parquet

# Base path in Azure Blob Storage
azure_blob_path = "wasbs://cfl@wimetrixarchives.blob.core.windows.net/2023/01/"

# Table name
tableName = "PieceWiseScan"

# Path pattern
path_pattern = f"{azure_blob_path}*/{tableName}/{tableName}_*.parquet"

# Read Parquet files of PieceWiseScan for all days of January 2023 from Azure Blob Storage
df = spark.read.parquet(path_pattern)

print(df.schema)
# df.show()
