# =============================================================================
# ClearSpend Data Platform - Curated Layer
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

# Read cleaned users from transformation layer
df = pd.read_sql("SELECT * FROM transformation.users", con=engine)
print(f"Loaded {len(df)} rows from transformation.users")

# Build dim_customer
# customer_id = natural key (matches transactions.client_id)
# customer_key = surrogate key generated here
dim_customer = pd.DataFrame({
    "customer_id":       df["id"],
    "gender":            df["gender"],
    "birth_year":        df["birth_year"],
    "birth_month":       df["birth_month"],
    "current_age":       df["current_age"],
    "retirement_age":    df["retirement_age"],
    "address":           df["address"],
    "latitude":          df["latitude"],
    "longitude":         df["longitude"],
    "per_capita_income": df["per_capita_income"],
    "yearly_income":     df["yearly_income"],
    "total_debt":        df["total_debt"],
    "credit_score":      df["credit_score"],
    "num_credit_cards":  df["num_credit_cards"],
    "employment_status": df["employment_status"],
    "education_level":   df["education_level"],
})

dim_customer = dim_customer.sort_values("customer_id").reset_index(drop=True)
dim_customer.insert(0, "customer_key", range(1, len(dim_customer) + 1))

print(f"dim_customer: {len(dim_customer)} rows")

# Load into curated schema — drop and recreate for idempotency
cursor.execute("DROP TABLE IF EXISTS curated.dim_customer")
cursor.execute("""
    CREATE TABLE curated.dim_customer (
        customer_key        INTEGER PRIMARY KEY,
        customer_id         INTEGER,
        gender              VARCHAR(20),
        birth_year          INTEGER,
        birth_month         INTEGER,
        current_age         INTEGER,
        retirement_age      INTEGER,
        address             VARCHAR(255),
        latitude            DECIMAL(18,2),
        longitude           DECIMAL(18,2),
        per_capita_income   DECIMAL(18,2),
        yearly_income       DECIMAL(18,2),
        total_debt          DECIMAL(18,2),
        credit_score        INTEGER,
        num_credit_cards    INTEGER,
        employment_status   VARCHAR(50),
        education_level     VARCHAR(100)
    )
""")

records = dim_customer.where(dim_customer.notna(), other=None).values.tolist()
placeholders = ",".join(["%s"] * len(dim_customer.columns))
cursor.executemany(f"INSERT INTO curated.dim_customer VALUES ({placeholders})", records)

print(f"✓ Loaded {len(dim_customer)} rows into curated.dim_customer")
cursor.close()
conn.close()