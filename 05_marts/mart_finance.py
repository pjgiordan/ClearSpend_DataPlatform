# =============================================================================
# ClearSpend Data Platform - Data Mart: Finance Team
# Purpose: Create finance mart tables answering business questions on
#          revenue, refunds, geographic performance, and category spending
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
# 1. Monthly revenue and refund summary
# Answers: "What is our total revenue by month?"
#          "What percentage of transactions are refunds?"
# Net revenue = SUM(amount) since refunds are already stored as negative values
# -----------------------------------------------------------------------------
cursor.execute("DROP TABLE IF EXISTS marts.finance_monthly")
cursor.execute("""
    CREATE TABLE marts.finance_monthly AS
    SELECT
        d.year,
        d.month,
        d.month_name,
        COUNT(*)                                                        AS total_transactions,
        SUM(CASE WHEN f.is_refund = FALSE THEN f.amount ELSE 0 END)    AS gross_revenue,
        SUM(CASE WHEN f.is_refund = TRUE THEN ABS(f.amount) ELSE 0 END) AS total_refund_amount,
        SUM(f.amount)                                                   AS net_revenue,
        SUM(CASE WHEN f.is_refund THEN 1 ELSE 0 END)                   AS refund_count,
        ROUND(SUM(CASE WHEN f.is_refund THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(*), 0), 2)                                   AS refund_rate_pct
    FROM curated.fact_transactions f
    JOIN curated.dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name
    ORDER BY d.year, d.month
""")
print("✓ marts.finance_monthly created")

# -----------------------------------------------------------------------------
# 2. Revenue by state
# Answers: "Which states generate the most revenue?"
# Excludes online transactions (merchant_state = N/A)
# -----------------------------------------------------------------------------
cursor.execute("DROP TABLE IF EXISTS marts.finance_by_state")
cursor.execute("""
    CREATE TABLE marts.finance_by_state AS
    SELECT
        m.merchant_state,
        COUNT(*)                                                        AS total_transactions,
        SUM(CASE WHEN f.is_refund = FALSE THEN f.amount ELSE 0 END)    AS gross_revenue,
        SUM(f.amount)                                                   AS net_revenue,
        SUM(CASE WHEN f.is_refund THEN 1 ELSE 0 END)                   AS refund_count,
        ROUND(SUM(f.amount) * 100.0 /
            SUM(SUM(f.amount)) OVER (), 2)                             AS pct_of_total_revenue
    FROM curated.fact_transactions f
    JOIN curated.dim_merchant m ON f.merchant_key = m.merchant_key
    WHERE m.merchant_state != 'N/A'
    GROUP BY m.merchant_state
    ORDER BY net_revenue DESC
""")
print("✓ marts.finance_by_state created")

# -----------------------------------------------------------------------------
# 3. Revenue by merchant category (MCC)
# Answers: "Which merchant categories drive the highest spending?"
# -----------------------------------------------------------------------------
cursor.execute("DROP TABLE IF EXISTS marts.finance_by_category")
cursor.execute("""
    CREATE TABLE marts.finance_by_category AS
    SELECT
        mc.mcc_code,
        mc.description                                                  AS category,
        COUNT(*)                                                        AS total_transactions,
        SUM(CASE WHEN f.is_refund = FALSE THEN f.amount ELSE 0 END)    AS gross_revenue,
        SUM(f.amount)                                                   AS net_revenue,
        ROUND(AVG(CASE WHEN f.is_refund = FALSE
            THEN f.amount END), 2)                                      AS avg_transaction_amount
    FROM curated.fact_transactions f
    JOIN curated.dim_mcc mc ON f.mcc_key = mc.mcc_key
    GROUP BY mc.mcc_code, mc.description
    ORDER BY net_revenue DESC
""")
print("✓ marts.finance_by_category created")

cursor.close()
conn.close()
print("Finance mart complete!")