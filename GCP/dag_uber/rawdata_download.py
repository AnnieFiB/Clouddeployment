# ==================================================================================
# # Batch ETL Pipeline (Cloud deployment)
# ## Case Study( On-Cloud) - GCP, Airflow, BigQuery
# ==================================================================================
# ### Libraries and Dependencies

import os
import tempfile
from pathlib import Path
import gdown
from google.cloud import storage 

# ====================
# ### Extraction Layer
# ====================
# - Download the uber data from Google Drive to Google Cloud Storage bucket


