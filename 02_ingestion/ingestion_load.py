# ClearSpend Data Platform - Ingestion Load
# Loads raw CSV files into PostgreSQL ingestion schema using bulk COPY

import psycopg2

# Connect to clearspend database
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="clearspend",
    user="postgres",
    password="Figaro123"
)
conn.autocommit = True
cursor = conn.cursor()

# Define tables and their CSV file paths
tables = [
    {
        "table": "ingestion.users",
        "file": "/Users/mac2024/Desktop/ClearSpend_DataPlatform/data/raw/users_data.csv"
    },
    {
        "table": "ingestion.cards",
        "file": "/Users/mac2024/Desktop/ClearSpend_DataPlatform/data/raw/cards_data.csv"
    },
    {
        "table": "ingestion.mcc",
        "file": "/Users/mac2024/Desktop/ClearSpend_DataPlatform/data/raw/mcc_data.csv"
    },
    {
        "table": "ingestion.transactions",
        "file": "/Users/mac2024/Desktop/ClearSpend_DataPlatform/data/raw/transactions_data.csv"
    },
]

for t in tables:
    table_name = t["table"]
    file_path = t["file"]

    # Truncate table before loading
    cursor.execute(f"TRUNCATE TABLE {table_name}")

    # Bulk load using COPY
    with open(file_path, "r", encoding="utf-8") as f:
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)

    # Count rows loaded
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    print(f"✓ Loaded {count} rows into {table_name}")

cursor.close()
conn.close()
print("All tables loaded successfully!")