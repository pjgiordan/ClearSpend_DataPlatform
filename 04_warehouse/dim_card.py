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

# Read cleaned cards from transformation layer
df = pd.read_sql("SELECT * FROM transformation.cards", con=engine)
print(f"Loaded {len(df)} rows from transformation.cards")

# Build dim_card
# card_id = natural key (matches transactions.card_id)
# card_key = surrogate key generated here
dim_card = pd.DataFrame({
    "card_id":               df["id"],
    "client_id":             df["client_id"],
    "card_brand":            df["card_brand"],
    "card_type":             df["card_type"],
    "card_number":           df["card_number"],
    "expires":               df["expires"],
    "cvv":                   df["cvv"],
    "has_chip":              df["has_chip"],
    "num_cards_issued":      df["num_cards_issued"],
    "credit_limit":          df["credit_limit"],
    "acct_open_date":        df["acct_open_date"],
    "year_pin_last_changed": df["year_pin_last_changed"],
    "card_on_dark_web":      df["card_on_dark_web"],
    "issuer_bank_name":      df["issuer_bank_name"],
    "issuer_bank_state":     df["issuer_bank_state"],
    "issuer_bank_type":      df["issuer_bank_type"],
    "issuer_risk_rating":    df["issuer_risk_rating"],
})

dim_card = dim_card.sort_values("card_id").reset_index(drop=True)
dim_card.insert(0, "card_key", range(1, len(dim_card) + 1))

print(f"dim_card: {len(dim_card)} rows")

# Load into curated schema — drop and recreate for idempotency
cursor.execute("DROP TABLE IF EXISTS curated.dim_card")
cursor.execute("""
    CREATE TABLE curated.dim_card (
        card_key                INTEGER PRIMARY KEY,
        card_id                 INTEGER,
        client_id               INTEGER,
        card_brand              VARCHAR(50),
        card_type               VARCHAR(50),
        card_number             VARCHAR(20),
        expires                 VARCHAR(10),
        cvv                     VARCHAR(3),
        has_chip                BOOLEAN,
        num_cards_issued        INTEGER,
        credit_limit            DECIMAL(18,2),
        acct_open_date          VARCHAR(10),
        year_pin_last_changed   INTEGER,
        card_on_dark_web        BOOLEAN,
        issuer_bank_name        VARCHAR(100),
        issuer_bank_state       VARCHAR(3),
        issuer_bank_type        VARCHAR(20),
        issuer_risk_rating      VARCHAR(10)
    )
""")

records = dim_card.where(dim_card.notna(), other=None).values.tolist()
placeholders = ",".join(["%s"] * len(dim_card.columns))
cursor.executemany(f"INSERT INTO curated.dim_card VALUES ({placeholders})", records)

print(f"✓ Loaded {len(dim_card)} rows into curated.dim_card")
cursor.close()
conn.close()