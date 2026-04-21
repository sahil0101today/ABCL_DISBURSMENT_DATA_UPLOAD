# -*- coding: utf-8 -*-
"""
Created on Mon Apr 20 14:47:57 2026

@author: sahil_pc
"""

import os
import json
import base64
import numpy as np
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery


encoded_key = os.environ.get("GCP_SA_KEY")
decoded_key = json.loads(base64.b64decode(encoded_key))

credentials = service_account.Credentials.from_service_account_info(decoded_key)

client = bigquery.Client(
    credentials=credentials,
    project="bigqueryfacebook"
)

table_id = "bigqueryfacebook.ABCL.ABCL_DISBURSMENT_DATA"

query = f"""
SELECT *
FROM `{table_id}`
"""
df1 = client.query(query).to_dataframe()

###########################################################################

# Read inputs from environment variables
file1_base64 = os.getenv("FILE1")
file2_base64 = os.getenv("FILE2")

# Decode and save
def save_file(base64_str, filename):
    with open(filename, "wb") as f:
        f.write(base64.b64decode(base64_str))

save_file(file1_base64, "file1.csv")
save_file(file2_base64, "file2.csv")

# Read CSVs
df2 = pd.read_csv("file1.csv")
df3 = pd.read_csv("file2.csv")

###############################################################################

table_id = "bigqueryfacebook.ABCL.ABCL_MOE_DATA"

job_config = bigquery.LoadJobConfig(
    schema=schema,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    autodetect=False
)
# -----------------------------
# 5. Upload
# -----------------------------
job = client.load_table_from_dataframe(
    df3,
    table_id,
    job_config=job_config
)

job.result()
print("✅ Data appended successfully to BigQuery 🚀")

###############################################################################
