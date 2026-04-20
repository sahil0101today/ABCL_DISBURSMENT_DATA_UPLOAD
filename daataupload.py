# -*- coding: utf-8 -*-
"""
Created on Mon Apr 20 14:47:57 2026

@author: sahil_pc
"""

import base64
import os
import pandas as pd

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
df1 = pd.read_csv("file1.csv")
df2 = pd.read_csv("file2.csv")

print("File1 Head:")
print(df1.head())

print("File2 Head:")
print(df2.head())

# Sample processing
merged = pd.concat([df1, df2])
merged.to_csv("output.csv", index=False)

print("Processing Completed!")