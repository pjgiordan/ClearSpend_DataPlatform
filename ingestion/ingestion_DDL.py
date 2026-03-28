# ClearSpend Data Platform - Ingestion DDL
# Creates the database, schema, and raw tables in PostgreSQL

import psycopg2

# Step 1: Connect to default postgres database to create 'clearspend' db
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres",
    user="postgres",
    password="Figaro123"
)
conn.autocommit = True
cursor = conn.cursor()

# Create database if it doesn't exist
cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'clearspend'")
if not cursor.fetchone():
    cursor.execute("CREATE DATABASE clearspend")
    print("Database 'clearspend' created!")
else:
    print("Database 'clearspend' already exists.")

cursor.close()
conn.close()

# Step 2: Connect to clearspend database
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="clearspend",
    user="postgres",
    password="Figaro123"
)
conn.autocommit = True
cursor = conn.cursor()

# Create ingestion schema
cursor.execute("CREATE SCHEMA IF NOT EXISTS ingestion")
print("Schema 'ingestion' created!")

# transactions table
cursor.execute("DROP TABLE IF EXISTS ingestion.transactions")
cursor.execute("""
    CREATE TABLE ingestion.transactions (
        id VARCHAR(50),
        date VARCHAR(50),
        client_id VARCHAR(50),
        card_id VARCHAR(50),
        amount VARCHAR(50),
        use_chip VARCHAR(50),
        merchant_id VARCHAR(50),
        merchant_city VARCHAR(100),
        merchant_state VARCHAR(10),
        zip VARCHAR(20),
        mcc VARCHAR(20),
        errors VARCHAR(255)
    )
""")
print("Table ingestion.transactions created!")

# users table
cursor.execute("DROP TABLE IF EXISTS ingestion.users")
cursor.execute("""
    CREATE TABLE ingestion.users (
        id VARCHAR(50),
        current_age VARCHAR(50),
        retirement_age VARCHAR(50),
        birth_year VARCHAR(10),
        birth_month VARCHAR(10),
        gender VARCHAR(20),
        address VARCHAR(255),
        latitude VARCHAR(20),
        longitude VARCHAR(20),
        per_capita_income VARCHAR(50),
        yearly_income VARCHAR(50),
        total_debt VARCHAR(50),
        credit_score VARCHAR(20),
        num_credit_cards VARCHAR(10),
        employment_status VARCHAR(50),
        education_level VARCHAR(100)
    )
""")
print("Table ingestion.users created!")

# cards table
cursor.execute("DROP TABLE IF EXISTS ingestion.cards")
cursor.execute("""
    CREATE TABLE ingestion.cards (
        id VARCHAR(50),
        client_id VARCHAR(50),
        card_brand VARCHAR(50),
        card_type VARCHAR(50),
        card_number VARCHAR(50),
        expires VARCHAR(20),
        cvv VARCHAR(10),
        has_chip VARCHAR(10),
        num_cards_issued VARCHAR(10),
        credit_limit VARCHAR(50),
        acct_open_date VARCHAR(20),
        year_pin_last_changed VARCHAR(10),
        card_on_dark_web VARCHAR(10),
        issuer_bank_name VARCHAR(100),
        issuer_bank_state VARCHAR(10),
        issuer_bank_type VARCHAR(50),
        issuer_risk_rating VARCHAR(20)
    )
""")
print("Table ingestion.cards created!")

# mcc table
cursor.execute("DROP TABLE IF EXISTS ingestion.mcc")
cursor.execute("""
    CREATE TABLE ingestion.mcc (
        code VARCHAR(20),
        description VARCHAR(255),
        notes VARCHAR(255),
        updated_by VARCHAR(100)
    )
""")
print("Table ingestion.mcc created!")

cursor.close()
conn.close()
print("All tables created successfully!")