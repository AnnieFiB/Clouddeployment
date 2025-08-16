# ==================================================================================
# # Batch ETL Pipeline (Cloud deployment)
# ## Case Study - Cloud -Azure Blob Storage, Azure Databricks, synapse, data factory
# ==================================================================================
# ### Libraries and Dependencies

import pandas as pd
import gdown
import tempfile
import shutil
from pyspark.sql import SparkSession
import os
from azure.storage.blob import BlobServiceClient
import sys
from dotenv import find_dotenv ,load_dotenv

load_dotenv(find_dotenv())

# spark = SparkSession.builder.getOrCreate()

# ====================
# ### Extraction Layer
# ====================
# - Download the NYPD arrests data from Google Drive
print("Extracting nypd raw data (csv) from google drive")

# - Download CSV content from Google Drive into a temporary file path 
with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
    temp_path = tmp.name

file_id = "1c5c3Mr2qcndsAo0OSAwDrQDMXZD2jgwk"
gdown.download(f"https://drive.google.com/uc?id={file_id}", temp_path, quiet=False)
nypd_df = pd.read_csv(temp_path)
os.remove(temp_path)

nypd_df.to_parquet("nypd_arrests_data.parquet", index=False)

# nypd_spark_df = spark.createDataFrame(nypd_df)
# nypd_spark_df.coalesce(1).write.mode("overwrite").parquet("nypd_arrests_data")

# ====================
# Upload the parquet file to Azure Blob Storage

connect_str = os.getenv('NYPD_STORAGE_CONNECTION_STRING')
container_name = os.getenv('nypd_container_name')

blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_client = blob_service_client.get_container_client(container_name)

blob_name = "raw-data/nypd_arrests_data.parquet" 
blob_client = container_client.get_blob_client(blob_name)
with open("nypd_arrests_data.parquet", "rb") as data:
    blob_client.upload_blob(data, overwrite=True)
print(f"Uploaded nypd_arrests_data.parquet to Azure Blob Storage in container '{container_name}' as '{blob_name}'")


# Clean up the local parquet file
os.remove("nypd_arrests_data.parquet")    
