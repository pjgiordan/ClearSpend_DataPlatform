# =============================================================================
# ClearSpend Data Platform - Transformation Layer
# Script: clean_users.py
# Source: ingestion.users → Target: transformation.users
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
df = pd.read_sql_query("SELECT * FROM ingestion.users", con=conn)
print(f"Loaded {len(df)} rows from ingestion.users")

# Remove null IDs and duplicates — keep last occurrence as most recent record
df = df.dropna(subset=["id"])
df["id"] = df["id"].astype(int)
df = df.drop_duplicates(subset=["id"], keep="last")

# Recalculate current_age from birth_year/birth_month — stored value becomes stale over time
today = pd.Timestamp.today()
df["birth_year"] = pd.to_numeric(df["birth_year"], errors="coerce")
df["birth_month"] = pd.to_numeric(df["birth_month"], errors="coerce")
df["current_age"] = df.apply(
    lambda row: int(today.year - row["birth_year"] - (
        1 if today.month < row["birth_month"] else 0
    ))
    if pd.notna(row["birth_year"]) and pd.notna(row["birth_month"]) else None,
    axis=1
)

df["retirement_age"] = pd.to_numeric(df["retirement_age"], errors="coerce")

# Parse currency fields — removes $, commas, and handles k/m suffixes
def parse_currency(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 0.0
    val = str(val).strip().replace("$", "").replace(",", "")
    if val.lower().endswith("k"):
        try: return float(val[:-1]) * 1_000
        except ValueError: pass
    if val.lower().endswith("m"):
        try: return float(val[:-1]) * 1_000_000
        except ValueError: pass
    try: return float(val)
    except ValueError: return 0.0

df["per_capita_income"] = df["per_capita_income"].apply(parse_currency)
df["yearly_income"] = df["yearly_income"].apply(parse_currency)
df["total_debt"] = df["total_debt"].apply(parse_currency)

# Validate credit score — values outside 300-850 are impossible and set to null
df["credit_score"] = pd.to_numeric(df["credit_score"], errors="coerce")
df.loc[(df["credit_score"] < 300) | (df["credit_score"] > 850), "credit_score"] = None

# Negative or null card counts are invalid — set to 0
df["num_credit_cards"] = pd.to_numeric(df["num_credit_cards"], errors="coerce")
df["num_credit_cards"] = df["num_credit_cards"].apply(
    lambda x: 0 if pd.isna(x) or x < 0 else int(x)
)

# Gender is already clean — strip whitespace only
df["gender"] = df["gender"].str.strip()

# Fix address casing (some street names were fully uppercase) and extra spaces
df["address"] = df["address"].apply(
    lambda x: " ".join(
        word.title() if word.isupper() else word
        for word in str(x).split()
    ) if pd.notna(x) else None
)
df["address"] = df["address"].str.replace(r"\s+", " ", regex=True).str.strip()

# Latitude and longitude are clean — convert from VARCHAR to numeric
df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

# Standardize employment_status — multiple typos and casing variants found
df["employment_status"] = df["employment_status"].str.strip()
employment_map = {
    "Un-employed": "Unemployed", "UNEMPLOYED": "Unemployed",
    "unemployed": "Unemployed", "Unemployd": "Unemployed",
    "STUDENT": "Student", "Studnt": "Student", "student": "Student",
    "SELF-EMPLOYED": "Self-Employed", "SELF EMPLOYED": "Self-Employed",
    "self-employed": "Self-Employed", "Self-Employd": "Self-Employed",
    "Retird": "Retired", "Ret.": "Retired",
    "retired": "Retired", "RETIRED": "Retired",
    "EMPLOYED": "Employed", "Empl0yed": "Employed", "employed": "Employed",
}
df["employment_status"] = df["employment_status"].replace(employment_map)

# Standardize education_level — abbreviations, typos, and casing variants found
df["education_level"] = df["education_level"].str.strip()
df["education_level"] = df["education_level"].str.replace(r"\s+", " ", regex=True)
education_map = {
    "high school": "High School", "highschool": "High School",
    "Highschool": "High School", "HIGH SCHOOL": "High School", "HS": "High School",
    "associate degree": "Associate Degree", "Associate Degree Degree": "Associate Degree",
    "Associate Degree Deg": "Associate Degree", "Associate": "Associate Degree",
    "ASSOCIATE DEGREE": "Associate Degree", "Assoc Degree": "Associate Degree",
    "Associate deg.": "Associate Degree",
    "BACHELOR DEGREE": "Bachelor Degree", "Bachelor Degrees": "Bachelor Degree",
    "Bachelor": "Bachelor Degree", "Bachelor's Degree": "Bachelor Degree",
    "BA/BS": "Bachelor Degree", "Bachelors": "Bachelor Degree",
    "Masters": "Masters Degree", "masters degree": "Masters Degree",
    "master degree": "Masters Degree", "Master Degree": "Masters Degree",
    "MASTER DEGREE": "Masters Degree", "Masters Degree Degree": "Masters Degree",
    "MS/MA": "Masters Degree", "Master's Degree": "Masters Degree",
    "DOCTORATE": "Doctorate",
}
df["education_level"] = df["education_level"].replace(education_map)

# Load into transformation schema — drop and recreate for idempotency
cursor.execute("DROP TABLE IF EXISTS transformation.users")
cursor.execute("""
    CREATE TABLE transformation.users (
        id                  INTEGER,
        current_age         INTEGER,
        retirement_age      INTEGER,
        birth_year          INTEGER,
        birth_month         INTEGER,
        gender              VARCHAR(20),
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

placeholders = ",".join(["%s" for _ in range(len(df.columns))])
for _, row in df.iterrows():
    values = [None if pd.isna(v) else v for v in row]
    cursor.execute(f"INSERT INTO transformation.users VALUES ({placeholders})", values)

print(f"✓ Loaded {len(df)} cleaned rows into transformation.users")
cursor.close()
conn.close()