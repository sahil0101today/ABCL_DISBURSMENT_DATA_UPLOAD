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

table_id = "bigqueryfacebook.ABCL.ABCL_ENG_DISB_MASTER_DATA"

query = f"""
SELECT *
FROM `{table_id}`
"""
df1 = client.query(query).to_dataframe()
print(df1.head())

###########################################################################

# Read file path from environment variable
file_path = os.getenv("FILE_PATH", "input.csv")  # fallback to input.csv

# Read CSV directly
df2 = pd.read_csv(file_path)

print(df2.head())
###############################################################################

df1['UUID'] = df1['MOBILE_NUMBER'] + "_" + df1['EVENT_TIME'] + "_" + df1['CAMPAIGN_TAG']
df2['UUID'] = df2['MOBILE_NUMBER'] + "_" + df2['EVENT_TIME'] + "_" + df2['CAMPAIGN_TAG']

df1['DISBURSMENT_AMOUNT'] = pd.to_numeric(df1['DISBURSMENT_AMOUNT'], errors='coerce').fillna(0)
df2['DISBURSMENT_AMOUNT'] = pd.to_numeric(df2['DISBURSMENT_AMOUNT'], errors='coerce').fillna(0)

df2_dummy = df2[~df2["DISBURSMENT_DATE"].isna()].reset_index(drop=True)

# Create lookup from df2
df2_lookup = df2_dummy.set_index('UUID')

new_rows = []

# Track UUIDs used
used_uuids = set()

for idx, row in df1.iterrows():

    uuid = row['UUID']

    # Check UUID exists in df2
    if uuid in df2_lookup.index:

        df2_row = df2_lookup.loc[uuid]

        # If multiple records exist in df2 for same UUID
        if isinstance(df2_row, pd.DataFrame):
            df2_row = df2_row.iloc[0]

        # If DISBURSMENT_DATE in df1 is empty/null
        if pd.isna(row['DISBURSMENT_DATE']):

            df1.at[idx, 'DISBURSMENT_AMOUNT'] = df2_row['DISBURSMENT_AMOUNT']
            df1.at[idx, 'DISBURSMENT_DATE'] = df2_row['DISBURSMENT_DATE']

            # Mark UUID as used for modification
            used_uuids.add(uuid)

        # If already present in df1, create new row from df2
        else:
            new_rows.append(df2_row.to_dict())

            # Mark UUID as used for new row addition
            used_uuids.add(uuid)

# Append new rows to df1
if len(new_rows) > 0:
    df1 = pd.concat([df1, pd.DataFrame(new_rows)], ignore_index=True)

# Remove used UUIDs from df2
df2_remaining = df2[~df2['UUID'].isin(used_uuids)].reset_index(drop=True)
df2_remaining = df2[~df2['UUID'].isin(df1["UUID"])].reset_index(drop=True)


df_final = pd.concat([df1, df2_remaining], ignore_index=True)

df_final['DISBURSMENT_DATE'] = df_final['DISBURSMENT_DATE'].str.replace(' 0', '', regex=False)

df_final['DISBURSMENT_DATE'] = pd.to_datetime(
    df_final['DISBURSMENT_DATE'],
    errors='coerce',   # prevents crash
    dayfirst=True      # important for Indian format
)

df_final['DISBURSMENT_DATE'] = df_final['DISBURSMENT_DATE'].dt.strftime('%d-%m-%Y')
#####################################################################################

table_id = "bigqueryfacebook.ABCL.ABCL_ENG_DISB_MASTER_DATA"
dataset_id = "ABCL"
table_name = "ABCL_ENG_DISB_MASTER_DATA"

# -----------------------------
# Drop table if exists
# -----------------------------
query_delete = f"""
DROP TABLE IF EXISTS `{table_id}`;
"""

try:
    client.query(query_delete).result()
    print(f"🗑️ Table dropped (if existed): {table_id}")
except Exception as e:
    print(f"⚠️ Drop failed for {table_id}: {e}")

# -----------------------------
# Clean dataframe types
# -----------------------------
for col in df_final.select_dtypes(include=["object"]).columns:
    df_final[col] = df_final[col].astype("string")

# -----------------------------
# Build schema dynamically
# -----------------------------
schema = []

for col, dtype in df_final.dtypes.items():
    if "int" in str(dtype):
        field_type = "INT64"
    elif "float" in str(dtype):
        field_type = "FLOAT64"
    elif "datetime" in str(dtype):
        field_type = "TIMESTAMP"
    else:
        field_type = "STRING"

    schema.append(bigquery.SchemaField(col, field_type))

# -----------------------------
# Load job config
# -----------------------------
job_config = bigquery.LoadJobConfig(
    schema=schema,
    write_disposition="WRITE_TRUNCATE"
)

# -----------------------------
# Table reference (correct way)
# -----------------------------
table_ref = f"{dataset_id}.{table_name}"

# -----------------------------
# Upload DataFrame to BigQuery
# -----------------------------
job = client.load_table_from_dataframe(
    df_final,
    table_ref,
    job_config=job_config
)



job.result()

print(f"✅ {table_name} uploaded successfully to BigQuery")
