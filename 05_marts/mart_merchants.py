# =============================================================================
# ClearSpend Data Platform - Data Mart: Merchant Partnerships Team
# Purpose: Create merchant partnerships mart tables answering business questions
#          on transaction volume, industry growth, error rates, and geography
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
# 1. Merchant performance by transaction volume and revenue
# Answers: "Which merchants generate the highest transaction volume?"
# Includes merchant category for industry context
# -----------------------------------------------------------------------------
cursor.execute("DROP TABLE IF EXISTS marts.merchant_performance")
cursor.execute("""
    CREATE TABLE marts.merchant_performance AS
    SELECT
        m.merchant_id,
        m.merchant_city,
        m.merchant_state,
        mc.description                                                  AS category,
        COUNT(*)                                                        AS total_transactions,
        SUM(CASE WHEN f.is_refund = FALSE THEN f.amount ELSE 0 END)    AS gross_revenue,
        SUM(f.amount)                                                   AS net_revenue,
        SUM(CASE WHEN f.is_refund THEN 1 ELSE 0 END)                   AS refund_count,
        ROUND(AVG(CASE WHEN f.is_refund = FALSE
            THEN f.amount END), 2)                                      AS avg_transaction_amount
    FROM curated.fact_transactions f
    JOIN curated.dim_merchant m ON f.merchant_key = m.merchant_key
    LEFT JOIN curated.dim_mcc mc ON f.mcc_key = mc.mcc_key
    GROUP BY m.merchant_id, m.merchant_city, m.merchant_state, mc.description
    ORDER BY total_transactions DESC
""")
print("✓ marts.merchant_performance created")

# -----------------------------------------------------------------------------
# 2. Industry revenue by year (for growth analysis)
# Answers: "What industries are growing the fastest?"
# Stores raw yearly totals — growth percentage can be calculated on top of this
# -----------------------------------------------------------------------------
cursor.execute("DROP TABLE IF EXISTS marts.industry_growth")
cursor.execute("""
    CREATE TABLE marts.industry_growth AS
    WITH spending_by_year AS (
        SELECT
            mc.mcc_code,
            mc.description                                              AS category,
            d.year,
            COUNT(*)                                                    AS total_transactions,
            SUM(f.amount)                                               AS net_revenue
        FROM curated.fact_transactions f
        JOIN curated.dim_mcc mc ON f.mcc_key = mc.mcc_key
        JOIN curated.dim_date d ON f.date_key = d.date_key
        GROUP BY mc.mcc_code, mc.description, d.year
    )
    SELECT
        curr.mcc_code,
        curr.category,
        curr.year,
        curr.total_transactions,
        curr.net_revenue,
        prev.net_revenue                                                AS prior_year_revenue,
        ROUND(
            (curr.net_revenue - prev.net_revenue)
            * 100.0 / NULLIF(prev.net_revenue, 0), 2
        )                                                               AS yoy_growth_pct
    FROM spending_by_year curr
    LEFT JOIN spending_by_year prev
        ON curr.mcc_code = prev.mcc_code
        AND curr.year = prev.year + 1
    ORDER BY yoy_growth_pct DESC NULLS LAST
""")
print("✓ marts.industry_growth created")

# -----------------------------------------------------------------------------
# 3. Merchant error rates
# Answers: "Which merchants have the highest error rates?"
# Includes error type aggregation to show which errors are most common
# Only includes merchants that have had at least one error transaction
# -----------------------------------------------------------------------------
cursor.execute("DROP TABLE IF EXISTS marts.merchant_errors")
cursor.execute("""
    CREATE TABLE marts.merchant_errors AS
    SELECT
        m.merchant_id,
        m.merchant_city,
        m.merchant_state,
        COUNT(*)                                                        AS total_transactions,
        SUM(CASE WHEN f.errors != 'N/A' THEN 1 ELSE 0 END)            AS error_count,
        ROUND(SUM(CASE WHEN f.errors != 'N/A' THEN 1 ELSE 0 END)
            * 100.0 / NULLIF(COUNT(*), 0), 2)                         AS error_rate_pct,
        STRING_AGG(DISTINCT CASE WHEN f.errors != 'N/A'
            THEN f.errors END, ', ')                                   AS error_types
    FROM curated.fact_transactions f
    JOIN curated.dim_merchant m ON f.merchant_key = m.merchant_key
    GROUP BY m.merchant_id, m.merchant_city, m.merchant_state
    HAVING SUM(CASE WHEN f.errors != 'N/A' THEN 1 ELSE 0 END) > 0
    ORDER BY error_rate_pct DESC
""")
print("✓ marts.merchant_errors created")

# -----------------------------------------------------------------------------
# 4. Revenue distribution by geography (state and city level)
# Answers: "How is revenue distributed geographically?"
# Excludes online transactions (merchant_state = N/A)
# Includes city level detail for more granular geographic analysis
# -----------------------------------------------------------------------------
cursor.execute("DROP TABLE IF EXISTS marts.revenue_by_geography")
cursor.execute("""
    CREATE TABLE marts.revenue_by_geography AS
    SELECT
        m.merchant_state,
        m.merchant_city,
        COUNT(*)                                                        AS total_transactions,
        SUM(CASE WHEN f.is_refund = FALSE THEN f.amount ELSE 0 END)    AS gross_revenue,
        SUM(f.amount)                                                   AS net_revenue,
        ROUND(AVG(CASE WHEN f.is_refund = FALSE
            THEN f.amount END), 2)                                      AS avg_transaction_amount,
        SUM(CASE WHEN f.is_refund THEN 1 ELSE 0 END)                   AS refund_count
    FROM curated.fact_transactions f
    JOIN curated.dim_merchant m ON f.merchant_key = m.merchant_key
    WHERE m.merchant_state != 'N/A'
    GROUP BY m.merchant_state, m.merchant_city
    ORDER BY net_revenue DESC
""")
print("✓ marts.revenue_by_geography created")

cursor.close()
conn.close()
print("Merchant partnerships mart complete!")