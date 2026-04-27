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

# Read inputs from environment variables
file1_base64 = os.getenv("FILE1")

# Decode and save
def save_file(base64_str, filename):
    with open(filename, "wb") as f:
        f.write(base64.b64decode(base64_str))

save_file(file1_base64, "file1.csv")

# Read CSVs
df2 = pd.read_csv("file1.csv")
print(df2.head())

###############################################################################

df1['UUID'] = df1['MOBILE_NUMBER'] + "_" + df1['EVENT_TIME'] + "_" + df1['CAMPAIGN_TAG']
df2['UUID'] = df2['MOBILE_NUMBER'] + "_" + df2['EVENT_TIME'] + "_" + df2['CAMPAIGN_TAG']

df1['DISBURSMENT_AMOUNT'] = pd.to_numeric(df1['DISBURSMENT_AMOUNT'], errors='coerce').fillna(0)
df2['DISBURSMENT_AMOUNT'] = pd.to_numeric(df2['DISBURSMENT_AMOUNT'], errors='coerce').fillna(0)

df2_dummy = df2[~df2["DISBURSMENT_AMOUNT"].isna()].reset_index()

merged_df = df1.merge(
    df2_dummy[['UUID', "EVENT_TIME",	"MOBILE_NUMBER",	"CAMPAIGN_ID",	"CAMPAIGN_CHANNEL",	"CAMPAIGN_TAG",	"EXPORT_DAY",	"LAST_ATTRIBUTION",	"DISBURSMENT_DATE",	"DISBURSMENT_AMOUNT"]],
    on='UUID',
    how='left',
    suffixes=('_df1', '_df2')
)

merged_df["DISBURSMENT_AMOUNT_df1"] = merged_df["DISBURSMENT_AMOUNT_df1"]+merged_df["DISBURSMENT_AMOUNT_df2"]

merged_df.drop(columns=["EVENT_TIME_df2",	"MOBILE_NUMBER_df2",	"CAMPAIGN_ID_df2",	"CAMPAIGN_CHANNEL_df2",	"CAMPAIGN_TAG_df2",	"EXPORT_DAY_df2",	"LAST_ATTRIBUTION_df2",	"DISBURSMENT_DATE_df2",	"DISBURSMENT_AMOUNT_df2"], inplace=True)
merged_df.columns = merged_df.columns.str.replace('_df1', '', regex=False).str.replace('_df2', '', regex=False)

df2 = df2[~df2['UUID'].isin(merged_df['UUID'])]

df_final = pd.concat([merged_df, df2], ignore_index=True)


######################################################################################

table_id = "bigqueryfacebook.ABCL.ABCL_ENG_DISB_MASTER_DATA"
dataset_id = "ABCL"
table_name = "ABCL_DISBURSMENT_DATA"

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



