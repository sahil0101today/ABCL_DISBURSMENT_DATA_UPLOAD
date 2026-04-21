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
print(df1.head())

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
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    autodetect=True
)

job = client.load_table_from_dataframe(
    df3,
    table_id,
    job_config=job_config
)

job.result()

print("✅ Data appended successfully to BigQuery 🚀")
###############################################################################

df2 = df2[~df2["DISBURSMENT_AMOUNT"].isna()].reset_index()


# Ensure COUNT numeric
df1['COUNT'] = pd.to_numeric(df1['COUNT'], errors='coerce').fillna(0).astype(int)
df2['COUNT'] = pd.to_numeric(df2['COUNT'], errors='coerce').fillna(0).astype(int)

try:
    for df in [df1, df2]:
        df['START_DATE'] = pd.to_datetime(df['START_DATE'], format='%d-%m-%Y')
        df['END_DATE'] = pd.to_datetime(df['END_DATE'], format='%d-%m-%Y')
except:
    pass

####################################################################################
# Case 1 - Same customer has disbursed again but we have not sent any communication to him

# -------------------------------
# Step 0: Preserve df2 index
# -------------------------------
df2 = df2.reset_index().rename(columns={'index': 'df2_index'})

# -------------------------------
# Step 1: Create UUID
# -------------------------------
df1['UUID'] = df1['MOBILE_NUMBER'] + "_" + df1['CAMPAIGN_TAG'] + "_" + df1['COUNT'].astype(str)
df2['UUID'] = df2['MOBILE_NUMBER'] + "_" + df2['CAMPAIGN_TAG'] + "_" + df2['COUNT'].astype(str)

# -------------------------------
# Step 2: Ensure numeric
# -------------------------------
df1['DISBURSMENT_AMOUNT'] = pd.to_numeric(df1['DISBURSMENT_AMOUNT'], errors='coerce').fillna(0)
df2['DISBURSMENT_AMOUNT'] = pd.to_numeric(df2['DISBURSMENT_AMOUNT'], errors='coerce').fillna(0)

# -------------------------------
# Step 3: Merge
# -------------------------------
merged_df = df1.merge(
    df2[['UUID', 'START_DATE', 'END_DATE', 'DISBURSMENT_AMOUNT', 'df2_index']],
    on='UUID',
    how='left',
    suffixes=('_df1', '_df2')
)

# -------------------------------
# Step 4: Condition
# -------------------------------
mask = (
    (merged_df['END_DATE_df1'] >= merged_df['START_DATE_df2']) &
    (merged_df['END_DATE_df1'] <= merged_df['END_DATE_df2'])
)

# -------------------------------
# Step 5: Update amount
# -------------------------------
merged_df.loc[mask, 'DISBURSMENT_AMOUNT_df1'] += merged_df.loc[mask, 'DISBURSMENT_AMOUNT_df2']

# -------------------------------
# Step 6: Push back to df1
# -------------------------------
df1['DISBURSMENT_AMOUNT'] = merged_df['DISBURSMENT_AMOUNT_df1']

# -------------------------------
# Step 7: Remove used rows from df2
# -------------------------------
indices_to_drop = merged_df.loc[mask, 'df2_index'].dropna().unique()

df2 = df2[~df2['df2_index'].isin(indices_to_drop)].copy()

# Drop helper column
df2 = df2.drop(columns=['df2_index'])



##############################################################################

#Case 2 - Same customer has disbursed again but we have sent same LOB communication to him
# Create UUID (without COUNT)
df1['UUID2'] = df1['MOBILE_NUMBER'] + "_" + df1['CAMPAIGN_TAG']
df2['UUID2'] = df2['MOBILE_NUMBER'] + "_" + df2['CAMPAIGN_TAG']

# Ensure COUNT numeric
df1['COUNT'] = pd.to_numeric(df1['COUNT'], errors='coerce').fillna(0)
df2['COUNT'] = pd.to_numeric(df2['COUNT'], errors='coerce').fillna(0)

# -------------------------------
# Step 1: Preserve df2 index
# -------------------------------
df2 = df2.reset_index().rename(columns={'index': 'df2_index'})

# -------------------------------
# Step 2: Merge
# -------------------------------
merged = df2.merge(
    df1[['UUID2', 'END_DATE', 'COUNT']],
    on='UUID2',
    how='inner',
    suffixes=('_df2', '_df1')
)

# -------------------------------
# Step 3: Apply condition
# -------------------------------
mask = (
    (merged['END_DATE_df1'] >= merged['START_DATE']) &
    (merged['END_DATE_df1'] <= merged['END_DATE_df2']) &
    (merged['COUNT_df2'] > merged['COUNT_df1'])
)

valid_rows = merged[mask].copy()

# -------------------------------
# Step 4: Adjust COUNT
# -------------------------------
valid_rows['COUNT'] = valid_rows['COUNT_df2'] - valid_rows['COUNT_df1']

# Remove zero/negative
valid_rows = valid_rows[valid_rows['COUNT'] > 0]

# -------------------------------
# Step 5: Prepare new records
# -------------------------------
valid_rows = valid_rows.rename(columns={
    'END_DATE_df2': 'END_DATE'
})

# Keep df2 structure
cols_to_keep = df2.columns
new_records = valid_rows[cols_to_keep]

# Remove helper column before append
new_records = new_records.drop(columns=['df2_index'])

# -------------------------------
# Step 6: Append to df1
# -------------------------------
df1 = pd.concat([df1, new_records], ignore_index=True)

# -------------------------------
# Step 7: Remove used rows from df2
# -------------------------------
indices_to_drop = valid_rows['df2_index'].unique()

df2 = df2[~df2['df2_index'].isin(indices_to_drop)].copy()

# Drop helper column
df2 = df2.drop(columns=['df2_index'])

##############################################################################
# Case 3 - Same customer has disbursed again but we have sent different LOB communication to him

# Create UUID in df1
df1['UUID3'] = df1['MOBILE_NUMBER'] + "_" + df1['CAMPAIGN_TAG']

# Store new records + indices to drop
new_rows = []
indices_to_drop = []

# Iterate df2
for idx, row in df2.iterrows():

    mobile = row['MOBILE_NUMBER']
    tag = row['CAMPAIGN_TAG']

    if pd.isna(tag):
        continue

    # Clean + split
    tag_parts = [x.strip() for x in tag.split(',')]

    if len(tag_parts) <= 1:
        continue

    # Progressive reduction
    for i in range(len(tag_parts)-1, 0, -1):

        dummy_tag = ",".join(tag_parts[:i])
        removed_part = ",".join(tag_parts[i:])

        uuid = mobile + "_" + dummy_tag

        match_df1 = df1[df1['UUID3'] == uuid]

        if not match_df1.empty:

            for _, df1_row in match_df1.iterrows():

                if (
                    df1_row['END_DATE'] >= row['START_DATE'] and
                    df1_row['END_DATE'] <= row['END_DATE']
                ):
                    # ✅ Create new record
                    new_row = row.copy()

                    # Only keep removed part
                    new_row['CAMPAIGN_TAG'] = removed_part
                    new_row['COUNT'] = len([x.strip() for x in removed_part.split(',')])

                    new_rows.append(new_row)

                    # 🔥 Mark this df2 row for removal
                    indices_to_drop.append(idx)

                    break

            break  # stop once matched

# Append to df1
if new_rows:
    df1 = pd.concat([df1, pd.DataFrame(new_rows)], ignore_index=True)

# ✅ Remove used rows from df2
if indices_to_drop:
    df2 = df2.drop(indices_to_drop).reset_index(drop=True)
##############################################################################
# Case 4 - Partial match + new tags extraction (UPDATED LOGIC)

new_rows = []
indices_to_drop = []

# Iterate df2
for idx, row in df2.iterrows():

    mobile = row['MOBILE_NUMBER']
    tag2 = row['CAMPAIGN_TAG']

    if pd.isna(tag2):
        continue

    tags2 = [x.strip() for x in tag2.split(',')]

    # Get df1 records for same mobile
    df1_subset = df1[df1['MOBILE_NUMBER'] == mobile]

    if df1_subset.empty:
        continue

    best_match = None

    for _, df1_row in df1_subset.iterrows():

        tag1 = df1_row['CAMPAIGN_TAG']
        tags1 = [x.strip() for x in tag1.split(',')]

        # 🔥 NEW LOGIC: Partial match using set intersection
        common_tags = set(tags1).intersection(set(tags2))

        if len(common_tags) == 0:
            continue

        # Date condition
        if not (
            df1_row['END_DATE'] >= row['START_DATE'] and
            df1_row['END_DATE'] <= row['END_DATE']
        ):
            continue

        # 🔥 Identify new tags (present in df2 but not in df1)
        new_tags = [t for t in tags2 if t not in tags1]

        if not new_tags:
            continue

        best_match = df1_row

        # Create new record
        new_row = row.copy()
        new_row['CAMPAIGN_TAG'] = ",".join(new_tags)
        new_row['COUNT'] = len(new_tags)

        new_rows.append(new_row)
        indices_to_drop.append(idx)

        break  # stop after first valid match

# Append to df1
if new_rows:
    df1 = pd.concat([df1, pd.DataFrame(new_rows)], ignore_index=True)

# Remove used rows from df2
if indices_to_drop:
    df2 = df2.drop(indices_to_drop).reset_index(drop=True)

##############################################################################


df1 =df1[["MOBILE_NUMBER","CAMPAIGN_TAG","COUNT","DISBURSMENT_DATE","DISBURSMENT_AMOUNT","START_DATE","END_DATE"]]
df2 =df2[["MOBILE_NUMBER","CAMPAIGN_TAG","COUNT","DISBURSMENT_DATE","DISBURSMENT_AMOUNT","START_DATE","END_DATE"]]

df = pd.concat([df1, df2], ignore_index=True)

######################################################################################

table_id = "bigqueryfacebook.ABCL.ABCL_DISBURSMENT_DATA"
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
for col in df.select_dtypes(include=["object"]).columns:
    df[col] = df[col].astype("string")

# -----------------------------
# Build schema dynamically
# -----------------------------
schema = []

for col, dtype in df.dtypes.items():
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
    df,
    table_ref,
    job_config=job_config
)

job.result()

print(f"✅ {table_name} uploaded successfully to BigQuery")
