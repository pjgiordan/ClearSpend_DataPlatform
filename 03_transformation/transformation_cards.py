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
df = pd.read_sql_query("SELECT * FROM ingestion.cards", con=conn)
print(f"Loaded {len(df)} rows from ingestion.cards")

# Convert and validate id and client_id — drop rows with null id
df["id"] = pd.to_numeric(df["id"], errors="coerce")
df["client_id"] = pd.to_numeric(df["client_id"], errors="coerce")
df = df.dropna(subset=["id"])
df["id"] = df["id"].astype(int)
df["client_id"] = df["client_id"].astype(int)

# Standardize card_brand — multiple typos, abbreviations, and casing variants found
df["card_brand"] = df["card_brand"].str.strip().str.replace(r"\s+", "", regex=True)
card_brand_map = {
    "V": "Visa", "VISA": "Visa", "visa": "Visa", "Vis": "Visa",
    "V!sa": "Visa", "Vissa": "Visa", "VVisa": "Visa", "visa-card": "Visa",
    "MASTERCARD": "Mastercard", "mastercard": "Mastercard",
    "MasterCard": "Mastercard", "Master Card": "Mastercard",
    "Amex": "American Express", "amex": "American Express", "AMEX": "American Express",
    "DISCOVER": "Discover", "discover": "Discover",
    "Dis cover": "Discover", "Dis  cover": "Discover",
    "unknown": "Unknown",
}
df["card_brand"] = df["card_brand"].replace(card_brand_map)
df["card_brand"] = df["card_brand"].fillna("Unknown")

# Standardize card_type — multiple typos, abbreviations, and casing variants found
df["card_type"] = df["card_type"].str.strip().str.replace(r"\s+", "", regex=True)
card_type_map = {
    "D": "Debit", "DB": "Debit", "DEB": "Debit", "DEBIT": "Debit",
    "debit": "Debit", "Debti": "Debit", "Deibt": "Debit", "Debiit": "Debit",
    "DeBiT": "Debit", "DebitCard": "Debit", "BankDebit": "Debit", "De bit": "Debit",
    "DP": "Debit (Prepaid)", "DPP": "Debit (Prepaid)", "PPD": "Debit (Prepaid)",
    "DB-PP": "Debit (Prepaid)", "Prepaid": "Debit (Prepaid)",
    "PrepaidDebit": "Debit (Prepaid)", "DebitPrepaid": "Debit (Prepaid)",
    "Debit(Prepaid)": "Debit (Prepaid)", "Debit(Prepaid)Card": "Debit (Prepaid)",
    "Debit(Prepiad)": "Debit (Prepaid)", "Debit(Prepayed)": "Debit (Prepaid)",
    "Debit(PREPAID)": "Debit (Prepaid)", "Debti(Prepaid)": "Debit (Prepaid)",
    "Debti(Prepiad)": "Debit (Prepaid)", "DeBiT(PrePaid)": "Debit (Prepaid)",
    "DEBIT(PREPAID)": "Debit (Prepaid)", "debit(prepaid)": "Debit (Prepaid)",
    "CC": "Credit", "CR": "Credit", "CRED": "Credit", "CREDIT": "Credit",
    "credit": "Credit", "Cedit": "Credit", "Credt": "Credit",
    "Crdeit": "Credit", "CrEdIt": "Credit", "Card-Credit": "Credit",
    "CreditCard": "Credit", "Card-Credit": "Credit",
    "unknown": "Unknown",
}
df["card_type"] = df["card_type"].replace(card_type_map)
df["card_type"] = df["card_type"].fillna("Unknown")

# Clean card_number — stored as float (e.g. 4344676511950444.0), convert to string
# Deduplicate on card_number as it is the unique identifier for each card
df["card_number"] = df["card_number"].apply(
    lambda x: str(int(float(x))) if pd.notna(x) and x != "" else None
)
df = df.drop_duplicates(subset=["card_number"], keep="last")
print(f"After removing duplicate card numbers: {len(df)} rows remaining")

# Parse expires and acct_open_date — format Mon-YY → MM/YYYY (e.g. Dec-22 → 12/2022)
df["expires"] = pd.to_datetime(df["expires"], format="%b-%y", errors="coerce")
df["expires"] = df["expires"].dt.strftime("%m/%Y")
df["expires"] = df["expires"].fillna("Unknown")

df["acct_open_date"] = pd.to_datetime(df["acct_open_date"], format="%b-%y", errors="coerce")
df["acct_open_date"] = df["acct_open_date"].dt.strftime("%m/%Y")
df["acct_open_date"] = df["acct_open_date"].fillna("Unknown")

# Convert CVV to string and pad to 3 digits (e.g. 56 → 056, 7 → 007)
df["cvv"] = df["cvv"].apply(
    lambda x: str(int(float(x))).zfill(3) if pd.notna(x) and x != "" else None
)
# Convert has_chip to boolean
df["has_chip"] = df["has_chip"].str.strip().str.upper().map({"YES": True, "NO": False})

df["num_cards_issued"] = pd.to_numeric(df["num_cards_issued"], errors="coerce")

# Parse credit_limit — removes $, handles k/m suffixes, flips negatives to positive
def parse_credit_limit(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 0.0
    val = str(val).strip().replace("$", "").replace(",", "")
    if val.lower() in ["n/a", "error_value", "limit_unknown", "9999999", ""]:
        return None
    word_map = {"ten thousand": 10000.0}
    if val.lower() in word_map:
        return word_map[val.lower()]
    if val.lower().endswith("k"):
        try: return float(val[:-1]) * 1_000
        except ValueError: pass
    if val.lower().endswith("m"):
        try: return float(val[:-1]) * 1_000_000
        except ValueError: pass
    try: return abs(float(val))
    except ValueError: return None

df["credit_limit"] = df["credit_limit"].apply(parse_credit_limit)

df["year_pin_last_changed"] = pd.to_numeric(df["year_pin_last_changed"], errors="coerce")

# Convert card_on_dark_web to boolean
df["card_on_dark_web"] = df["card_on_dark_web"].str.strip().str.upper().map({"YES": True, "NO": False})

# Standardize issuer_bank_name — abbreviations, casing variants, and Chase/JPMorgan merged
df["issuer_bank_name"] = df["issuer_bank_name"].str.strip()
bank_name_map = {
    "ALLY BANK": "Ally Bank", "Ally Bk": "Ally Bank", "ally bank": "Ally Bank",
    "BANK OF AMERICA": "Bank of America", "Bk of America": "Bank of America",
    "bank of america": "Bank of America",
    "CAPITAL ONE": "Capital One", "capital one": "Capital One",
    "Chase Bank": "JPMorgan Chase", "Chase Bk": "JPMorgan Chase",
    "CHASE BANK": "JPMorgan Chase", "chase bank": "JPMorgan Chase",
    "JP Morgan Chase": "JPMorgan Chase", "JPMORGAN CHASE": "JPMorgan Chase",
    "jpmorgan chase": "JPMorgan Chase",
    "CITI": "Citi", "citi": "Citi",
    "DISCOVER BANK": "Discover Bank", "Discover Bk": "Discover Bank",
    "discover bank": "Discover Bank",
    "PNC BANK": "PNC Bank", "PNC Bk": "PNC Bank", "pnc bank": "PNC Bank",
    "TRUIST": "Truist", "truist": "Truist",
    "U.S. BANK": "U.S. Bank", "U.S. Bk": "U.S. Bank", "u.s. bank": "U.S. Bank",
    "WELLS FARGO": "Wells Fargo", "wells fargo": "Wells Fargo",
}
df["issuer_bank_name"] = df["issuer_bank_name"].replace(bank_name_map)

# Standardize issuer_bank_state — full state names and lowercase variants to abbreviations
df["issuer_bank_state"] = df["issuer_bank_state"].str.strip()
state_map = {
    "California": "CA", "Illinois": "IL", "Michigan": "MI",
    "Minnesota": "MN", "New York": "NY", "North Carolina": "NC",
    "Pennsylvania": "PA", "Virginia": "VA",
    "ca": "CA", "il": "IL", "mi": "MI", "mn": "MN",
    "nc": "NC", "ny": "NY", "pa": "PA", "va": "VA",
}
df["issuer_bank_state"] = df["issuer_bank_state"].replace(state_map)

# Standardize issuer_bank_type — casing variants and descriptive suffixes removed
df["issuer_bank_type"] = df["issuer_bank_type"].str.strip()
bank_type_map = {
    "NATIONAL": "National", "National Bank": "National", "national": "National",
    "ONLINE": "Online", "Online Bank": "Online", "Online Only": "Online", "online": "Online",
    "REGIONAL": "Regional", "Regional Bank": "Regional", "regional": "Regional",
}
df["issuer_bank_type"] = df["issuer_bank_type"].replace(bank_type_map)

# Standardize issuer_risk_rating — casing variants and descriptive suffixes removed
df["issuer_risk_rating"] = df["issuer_risk_rating"].str.strip()
risk_map = {
    "LOW": "Low", "low": "Low", "Low Risk": "Low",
    "MEDIUM": "Medium", "medium": "Medium", "Med": "Medium",
    "HIGH": "High", "high": "High", "High Risk": "High",
}
df["issuer_risk_rating"] = df["issuer_risk_rating"].replace(risk_map)

# Load into transformation schema — drop and recreate for idempotency
cursor.execute("DROP TABLE IF EXISTS transformation.cards")
cursor.execute("""
    CREATE TABLE transformation.cards (
        id                      INTEGER,
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

placeholders = ",".join(["%s" for _ in range(len(df.columns))])
for _, row in df.iterrows():
    values = [None if pd.isna(v) else v for v in row]
    cursor.execute(f"INSERT INTO transformation.cards VALUES ({placeholders})", values)

print(f"✓ Loaded {len(df)} cleaned rows into transformation.cards")
cursor.close()
conn.close()