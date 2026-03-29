# =============================================================================
# ClearSpend Data Platform - Curated Layer
# Script: dim_mcc.py
# Note: updated_by is intentionally excluded — no business value in warehouse
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

# Read cleaned MCC data — only code and description needed in warehouse
df = pd.read_sql("SELECT code, description FROM transformation.mcc", con=engine)
print(f"Loaded {len(df)} rows from transformation.mcc")

# Build dim_mcc
# mcc_code = natural key (matches transactions.mcc)
# mcc_key = surrogate key generated here
dim_mcc = pd.DataFrame({
    "mcc_code":    df["code"],
    "description": df["description"],
})

dim_mcc = dim_mcc.sort_values("mcc_code").reset_index(drop=True)
dim_mcc.insert(0, "mcc_key", range(1, len(dim_mcc) + 1))

print(f"dim_mcc: {len(dim_mcc)} rows")

# Load into curated schema — drop and recreate for idempotency
cursor.execute("DROP TABLE IF EXISTS curated.dim_mcc")
cursor.execute("""
    CREATE TABLE curated.dim_mcc (
        mcc_key     INTEGER PRIMARY KEY,
        mcc_code    INTEGER,
        description VARCHAR(255)
    )
""")

records = dim_mcc.where(dim_mcc.notna(), other=None).values.tolist()
placeholders = ",".join(["%s"] * len(dim_mcc.columns))
cursor.executemany(f"INSERT INTO curated.dim_mcc VALUES ({placeholders})", records)

print(f"✓ Loaded {len(dim_mcc)} rows into curated.dim_mcc")
cursor.close()
conn.close()