# =============================================================================
# ClearSpend Data Platform - Transformation Layer
# =============================================================================

import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host="localhost", port=5432,
    database="clearspend", user="postgres", password="Figaro123"
)
conn.autocommit = True
cursor = conn.cursor()

cursor.execute("CREATE SCHEMA IF NOT EXISTS transformation")
print("Schema 'transformation' ready!")

# Extract from ingestion layer
df = pd.read_sql_query("SELECT * FROM ingestion.mcc", con=conn)
print(f"Loaded {len(df)} rows from ingestion.mcc")

# Remove surrounding quotes and MCC prefix from code (e.g. "3000" → 3000, MCC3066 → 3066)
df["code"] = df["code"].str.replace('"', '', regex=False).str.strip()
df["code"] = df["code"].str.replace('MCC', '', case=False, regex=False).str.strip()

# Remove junk rows — keep only rows where code is purely numeric
# Catches metadata rows like NOTE and COMMENT regardless of position
df = df[df["code"].str.match(r'^\d+$', na=False)]
print(f"After removing junk rows: {len(df)} rows remaining")

# Convert code to integer for correct sorting and joining with transactions.mcc
df["code"] = df["code"].astype(int)

# Standardize description to title case and fix apostrophe issue (e.g. Women'S → Women's)
df["description"] = df["description"].str.strip().str.title()
df["description"] = df["description"].str.replace("'S ", "'s ", regex=False)

# Drop notes column (no business value)
# Keep updated_by as an audit trail in the transformation layer
df = df[["code", "description", "updated_by"]]

# Remove duplicate codes — after cleaning, variants like "4899" and MCC4899 resolve to same code
df = df.drop_duplicates(subset=["code"], keep="first")
print(f"After removing duplicates: {len(df)} rows remaining")

# Load into transformation schema — drop and recreate for idempotency
cursor.execute("DROP TABLE IF EXISTS transformation.mcc")
cursor.execute("""
    CREATE TABLE transformation.mcc (
        code        INTEGER,
        description VARCHAR(255),
        updated_by  VARCHAR(100)
    )
""")

placeholders = ",".join(["%s" for _ in range(len(df.columns))])
for _, row in df.iterrows():
    values = [None if pd.isna(v) else v for v in row]
    cursor.execute(f"INSERT INTO transformation.mcc VALUES ({placeholders})", values)

print(f"✓ Loaded {len(df)} cleaned rows into transformation.mcc")
cursor.close()
conn.close()