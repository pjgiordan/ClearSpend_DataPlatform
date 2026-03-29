# =============================================================================
# ClearSpend Data Platform - Data Mart: Customer Analytics Team
# Purpose: Create customer analytics mart tables answering business questions
#          on lifetime value, channel behaviour, card usage, and fraud detection
# =============================================================================

import psycopg2

conn = psycopg2.connect(
    host="localhost", port=5432,
    database="clearspend", user="postgres", password="Figaro123"
)
conn.autocommit = True
cursor = conn.cursor()

cursor.execute("CREATE SCHEMA IF NOT EXISTS marts")
print("Schema 'marts' ready!")

# -----------------------------------------------------------------------------
# 1. Customer lifetime value
# Answers: "What is the lifetime value of each customer?"
# Includes demographic context, spend metrics, and transaction date range
# Refunds excluded from lifetime spend as they reduce customer value
# -----------------------------------------------------------------------------
cursor.execute("DROP TABLE IF EXISTS marts.customer_ltv")
cursor.execute("""
    CREATE TABLE marts.customer_ltv AS
    SELECT
        c.customer_id,
        c.gender,
        c.current_age,
        c.employment_status,
        c.education_level,
        c.yearly_income,
        c.total_debt,
        c.credit_score,
        COUNT(*)                                                        AS total_transactions,
        SUM(CASE WHEN f.is_refund = FALSE THEN f.amount ELSE 0 END)    AS lifetime_spend,
        SUM(CASE WHEN f.is_refund = TRUE
            THEN ABS(f.amount) ELSE 0 END)                             AS total_refunded,
        ROUND(AVG(CASE WHEN f.is_refund = FALSE
            THEN f.amount END), 2)                                      AS avg_transaction_amount,
        MIN(d.full_date)                                                AS first_transaction_date,
        MAX(d.full_date)                                                AS last_transaction_date
    FROM curated.fact_transactions f
    JOIN curated.dim_customer c ON f.customer_key = c.customer_key
    JOIN curated.dim_date d ON f.date_key = d.date_key
    GROUP BY c.customer_id, c.gender, c.current_age, c.employment_status,
             c.education_level, c.yearly_income, c.total_debt, c.credit_score
    ORDER BY lifetime_spend DESC
""")
print("✓ marts.customer_ltv created")

# -----------------------------------------------------------------------------
# 2. Online vs in-store channel behaviour
# Answers: "How do customers behave online vs in-store?"
# Breaks down by channel with spend, transaction count, and percentage
# -----------------------------------------------------------------------------
cursor.execute("DROP TABLE IF EXISTS marts.customer_channel")
cursor.execute("""
    CREATE TABLE marts.customer_channel AS
    SELECT
        c.customer_id,
        SUM(CASE WHEN f.use_chip = 'Online'
            THEN 1 ELSE 0 END)                                         AS online_transactions,
        SUM(CASE WHEN f.use_chip = 'In-Store'
            THEN 1 ELSE 0 END)                                         AS instore_transactions,
        SUM(CASE WHEN f.use_chip = 'Online'
            THEN f.amount ELSE 0 END)                                  AS online_spend,
        SUM(CASE WHEN f.use_chip = 'In-Store'
            THEN f.amount ELSE 0 END)                                  AS instore_spend,
        ROUND(SUM(CASE WHEN f.use_chip = 'Online' THEN 1 ELSE 0 END)
            * 100.0 / NULLIF(COUNT(*), 0), 2)                         AS online_pct,
        ROUND(SUM(CASE WHEN f.use_chip = 'In-Store' THEN 1 ELSE 0 END)
            * 100.0 / NULLIF(COUNT(*), 0), 2)                         AS instore_pct
    FROM curated.fact_transactions f
    JOIN curated.dim_customer c ON f.customer_key = c.customer_key
    GROUP BY c.customer_id
    ORDER BY online_pct DESC
""")
print("✓ marts.customer_channel created")

# -----------------------------------------------------------------------------
# 3. Active cards per customer
# Answers: "How many active cards does a typical customer have?"
# Joins dim_card directly to dim_customer via client_id
# Includes card type breakdown, chip status, dark web flags, and credit limits
# -----------------------------------------------------------------------------
cursor.execute("DROP TABLE IF EXISTS marts.customer_cards")
cursor.execute("""
    CREATE TABLE marts.customer_cards AS
    SELECT
        c.customer_id,
        COUNT(dc.card_key)                                              AS total_cards,
        SUM(CASE WHEN dc.has_chip = 'Yes' THEN 1 ELSE 0 END)          AS cards_with_chip,
        SUM(CASE WHEN dc.card_on_dark_web = 'Yes' THEN 1 ELSE 0 END)  AS dark_web_cards,
        ROUND(AVG(dc.credit_limit), 2)                                 AS avg_credit_limit,
        SUM(dc.credit_limit)                                           AS total_credit_limit,
        SUM(CASE WHEN dc.card_type = 'Credit'
            THEN 1 ELSE 0 END)                                         AS credit_cards,
        SUM(CASE WHEN dc.card_type = 'Debit'
            THEN 1 ELSE 0 END)                                         AS debit_cards,
        SUM(CASE WHEN dc.card_type = 'Debit (Prepaid)'
            THEN 1 ELSE 0 END)                                         AS prepaid_cards
    FROM curated.dim_customer c
    LEFT JOIN curated.dim_card dc ON c.customer_id = dc.client_id
    GROUP BY c.customer_id
    ORDER BY total_cards DESC
""")
print("✓ marts.customer_cards created")

# -----------------------------------------------------------------------------
# 4. Suspicious transaction patterns
# Answers: "Can we identify suspicious transaction patterns?"
# Three flag types:
#   a) Dark web card — card linked to known dark web exposure
#   b) Transaction error — transaction recorded with an error
#   c) High value outlier — amount > customer avg + 3 standard deviations
# Uses customer-level statistics to make outlier detection personalised
# -----------------------------------------------------------------------------
cursor.execute("DROP TABLE IF EXISTS marts.suspicious_transactions")
cursor.execute("""
    CREATE TABLE marts.suspicious_transactions AS
    WITH customer_stats AS (
        -- Calculate per-customer average and standard deviation for outlier detection
        SELECT
            customer_key,
            AVG(amount)    AS avg_amount,
            STDDEV(amount) AS std_amount
        FROM curated.fact_transactions
        WHERE is_refund = FALSE
        GROUP BY customer_key
    )
    SELECT
        f.transaction_id,
        c.customer_id,
        f.card_key,
        d.full_date,
        f.amount,
        cs.avg_amount                                                   AS customer_avg_amount,
        f.use_chip,
        f.errors,
        dc.card_on_dark_web,
        CASE
            WHEN dc.card_on_dark_web = 'Yes'
                THEN 'Dark Web Card'
            WHEN f.errors != 'N/A'
                THEN 'Transaction Error'
            WHEN f.amount > cs.avg_amount + 3 * cs.std_amount
                THEN 'High Value Outlier'
            ELSE 'Other'
        END                                                             AS flag_reason
    FROM curated.fact_transactions f
    JOIN curated.dim_date d     ON f.date_key     = d.date_key
    JOIN curated.dim_card dc    ON f.card_key     = dc.card_key
    JOIN curated.dim_customer c ON f.customer_key = c.customer_key
    JOIN customer_stats cs      ON f.customer_key = cs.customer_key
    WHERE dc.card_on_dark_web = 'Yes'
       OR f.errors != 'N/A'
       OR f.amount > cs.avg_amount + 3 * cs.std_amount
    ORDER BY d.full_date DESC
""")
print("✓ marts.suspicious_transactions created")

cursor.close()
conn.close()
print("Customer analytics mart complete!")