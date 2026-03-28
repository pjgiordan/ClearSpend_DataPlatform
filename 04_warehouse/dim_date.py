# =============================================================================
# ClearSpend Data Platform - Curated Layer
# Script: dim_date.py
# Purpose: Generate calendar dimension covering the full transaction date range
# Source: transformation.transactions → Target: curated.dim_date
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
print("Schema 'curated' ready!")

# Get date range directly from transactions in SQL — avoids loading 13M rows into Python
result = pd.read_sql("""
    SELECT MIN(date) AS min_date, MAX(date) AS max_date
    FROM transformation.transactions
    WHERE date IS NOT NULL
""", con=engine)

min_date = result["min_date"].iloc[0]
max_date = result["max_date"].iloc[0]
print(f"Transaction date range: {min_date} → {max_date}")

# Generate one row per calendar day across the full date range
date_range = pd.date_range(start=min_date, end=max_date, freq="D")

dim_date = pd.DataFrame({
    "full_date":   date_range.date,
    "year":        date_range.year,
    "quarter":     date_range.quarter,
    "month":       date_range.month,
    "month_name":  date_range.strftime("%B"),
    "week_number": date_range.isocalendar().week.astype(int),
    "day":         date_range.day,
    "day_of_week": date_range.dayofweek + 1,  # 1=Monday, 7=Sunday
    "day_name":    date_range.strftime("%A"),
    "is_weekend":  (date_range.dayofweek >= 5).astype(int),  # 1 if Sat/Sun
})

# Add surrogate key as first column
dim_date.insert(0, "date_key", range(1, len(dim_date) + 1))
dim_date["full_date"] = pd.to_datetime(dim_date["full_date"])

print(f"dim_date: {len(dim_date)} rows")

# Load into curated schema — drop and recreate for idempotency
cursor.execute("DROP TABLE IF EXISTS curated.dim_date")
cursor.execute("""
    CREATE TABLE curated.dim_date (
        date_key    INTEGER PRIMARY KEY,
        full_date   DATE,
        year        INTEGER,
        quarter     INTEGER,
        month       INTEGER,
        month_name  VARCHAR(10),
        week_number INTEGER,
        day         INTEGER,
        day_of_week INTEGER,
        day_name    VARCHAR(10),
        is_weekend  INTEGER
    )
""")

records = dim_date.where(dim_date.notna(), other=None).values.tolist()
placeholders = ",".join(["%s"] * len(dim_date.columns))
cursor.executemany(f"INSERT INTO curated.dim_date VALUES ({placeholders})", records)

print(f"✓ Loaded {len(dim_date)} rows into curated.dim_date")
cursor.close()
conn.close()