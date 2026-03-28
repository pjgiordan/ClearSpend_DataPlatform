# ClearSpend Data Platform - Transformation: Transactions
# Uses pure SQL transformation to avoid loading 13M rows into memory
# Due to the size of the transactions dataset (13.3M rows), 
# the transformation was performed using a SQL CTAS (Create Table As Select) 
# statement directly in PostgreSQL to avoid ##memory constraints, which is standard practice for large-scale data pipelines.
import psycopg2

conn = psycopg2.connect(
    host="localhost", port=5432,
    database="clearspend", user="postgres", password="Figaro123"
)
conn.autocommit = True
cursor = conn.cursor()

cursor.execute("CREATE SCHEMA IF NOT EXISTS transformation")
print("Schema 'transformation' ready!")

print("Creating transformation.transactions using SQL (fast approach)...")

cursor.execute("DROP TABLE IF EXISTS transformation.transactions")
cursor.execute("""
    CREATE TABLE transformation.transactions AS
    SELECT
        id::BIGINT                                           AS id,
        date::TIMESTAMP::DATE                               AS date,
        client_id::INTEGER                                  AS client_id,
        card_id::INTEGER                                    AS card_id,
        REPLACE(REPLACE(amount, '$', ''), ',', '')::DECIMAL(18,2) AS amount,
        CASE
            WHEN use_chip = 'Swipe Transaction'      THEN 'In-Store'
            WHEN use_chip = 'Chip Transaction'       THEN 'In-Store'
            WHEN use_chip = 'Chip Card Transaction'  THEN 'In-Store'
            WHEN use_chip = 'Online Transaction'     THEN 'Online'
            ELSE 'Unknown'
        END                                                 AS use_chip,
        merchant_id::INTEGER                                AS merchant_id,
        CASE
            WHEN UPPER(merchant_city) = 'ONLINE' THEN 'N/A'
            ELSE TRIM(merchant_city)
        END                                                 AS merchant_city,
        COALESCE(merchant_state, 'N/A')                    AS merchant_state,
        CASE
            WHEN zip IS NULL THEN 'N/A'
            ELSE SPLIT_PART(zip, '.', 1)
        END                                                 AS zip,
        mcc::INTEGER                                        AS mcc,
        COALESCE(errors, 'N/A')                            AS errors
    FROM ingestion.transactions
    WHERE id IS NOT NULL
""")

cursor.execute("SELECT COUNT(*) FROM transformation.transactions")
count = cursor.fetchone()[0]
print(f"✓ Loaded {count:,} rows into transformation.transactions")

cursor.close()
conn.close()