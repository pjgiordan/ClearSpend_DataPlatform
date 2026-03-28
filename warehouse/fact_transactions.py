# =============================================================================
# ClearSpend Data Platform - Curated Layer
# Script: fact_transactions.py
# Purpose: Build central fact table joining all dimension surrogate keys
# Grain: one row per transaction
# Source: transformation.transactions + all curated dimensions
# Target: curated.fact_transactions
# =============================================================================

import psycopg2

conn = psycopg2.connect(
    host="localhost", port=5432,
    database="clearspend", user="postgres", password="Figaro123"
)
conn.autocommit = True
cursor = conn.cursor()

cursor.execute("CREATE SCHEMA IF NOT EXISTS curated")

# Drop and recreate fact table
# BIGSERIAL auto-generates transaction_key — no need to manage it manually
# LEFT JOINs used so unmatched transactions are still included
cursor.execute("DROP TABLE IF EXISTS curated.fact_transactions")
cursor.execute("""
    CREATE TABLE curated.fact_transactions (
        transaction_key  BIGSERIAL PRIMARY KEY,
        transaction_id   BIGINT,
        date_key         INTEGER,
        customer_key     INTEGER,
        card_key         INTEGER,
        merchant_key     INTEGER,
        mcc_key          INTEGER,
        amount           DECIMAL(18,2),
        is_refund        BOOLEAN,
        use_chip         VARCHAR(10),
        zip              VARCHAR(10),
        errors           VARCHAR(255)
    )
""")
print("curated.fact_transactions table created!")

# Populate fact table entirely in SQL to avoid loading 13M rows into Python
# is_refund derived from negative amount — negative transactions are refunds
print("Inserting into fact_transactions (this may take a few minutes)...")
cursor.execute("""
    INSERT INTO curated.fact_transactions (
        transaction_id,
        date_key,
        customer_key,
        card_key,
        merchant_key,
        mcc_key,
        amount,
        is_refund,
        use_chip,
        zip,
        errors
    )
    SELECT
        t.id                        AS transaction_id,
        dd.date_key                 AS date_key,
        dc.customer_key             AS customer_key,
        dca.card_key                AS card_key,
        dm.merchant_key             AS merchant_key,
        dmcc.mcc_key                AS mcc_key,
        t.amount                    AS amount,
        CASE WHEN t.amount < 0
             THEN TRUE
             ELSE FALSE
        END                         AS is_refund,
        t.use_chip                  AS use_chip,
        t.zip                       AS zip,
        t.errors                    AS errors
    FROM transformation.transactions t
    LEFT JOIN curated.dim_date      dd   ON t.date        = dd.full_date
    LEFT JOIN curated.dim_customer  dc   ON t.client_id   = dc.customer_id
    LEFT JOIN curated.dim_card      dca  ON t.card_id     = dca.card_id
    LEFT JOIN curated.dim_merchant  dm   ON t.merchant_id = dm.merchant_id
    LEFT JOIN curated.dim_mcc       dmcc ON t.mcc         = dmcc.mcc_code
""")

cursor.execute("SELECT COUNT(*) FROM curated.fact_transactions")
row_count = cursor.fetchone()[0]
print(f"✓ Loaded {row_count:,} rows into curated.fact_transactions")

cursor.close()
conn.close()