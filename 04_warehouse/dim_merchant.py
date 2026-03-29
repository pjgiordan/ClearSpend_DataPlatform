# =============================================================================
# ClearSpend Data Platform - Curated Layer
# Script: dim_merchant.py
# Note: No separate merchants file exists — derived directly from transactions
# =============================================================================

import psycopg2
import pandas as pd
from sqlalchemy import create_engine

conn = psycopg2.connect(
    host="localhost", port=5432,
    database="clearspend", user="postgres", password="Figaro123"
)
conn.autocommit = True
cursor = conn.cursor()
engine = create_engine("postgresql+psycopg2://postgres:Figaro123@localhost:5432/clearspend")

cursor.execute("CREATE SCHEMA IF NOT EXISTS curated")

# Extract distinct merchants using SQL — avoids loading all 13M rows into Python
# Group by merchant_id + city + state and count frequency
print("Extracting distinct merchants from transactions...")
df = pd.read_sql("""
    SELECT
        merchant_id,
        merchant_city,
        merchant_state,
        COUNT(*) AS frequency
    FROM transformation.transactions
    WHERE merchant_id IS NOT NULL
    GROUP BY merchant_id, merchant_city, merchant_state
""", con=engine)

# Keep only the most frequent city/state per merchant
# Ensures one clean row per merchant_id for a proper star schema dimension
df = df.sort_values("frequency", ascending=False)
df = df.drop_duplicates(subset=["merchant_id"], keep="first")
df = df.drop(columns=["frequency"])

dim_merchant = df.sort_values("merchant_id").reset_index(drop=True)
dim_merchant.insert(0, "merchant_key", range(1, len(dim_merchant) + 1))

print(f"dim_merchant: {len(dim_merchant)} rows")

# Load into curated schema — drop and recreate for idempotency
cursor.execute("DROP TABLE IF EXISTS curated.dim_merchant")
cursor.execute("""
    CREATE TABLE curated.dim_merchant (
        merchant_key    INTEGER PRIMARY KEY,
        merchant_id     INTEGER,
        merchant_city   VARCHAR(100),
        merchant_state  VARCHAR(50)
    )
""")

records = dim_merchant.where(dim_merchant.notna(), other=None).values.tolist()
placeholders = ",".join(["%s"] * len(dim_merchant.columns))
cursor.executemany(f"INSERT INTO curated.dim_merchant VALUES ({placeholders})", records)

print(f"✓ Loaded {len(dim_merchant)} rows into curated.dim_merchant")
cursor.close()
conn.close()