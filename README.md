# ClearSpend Data Platform

End-to-end financial data pipeline for ClearSpend — ingestion, transformation, dimensional warehouse, and business data marts using Python and PostgreSQL.

**Course:** Data Engineering and Data Governance | EBC 2199  
**Authors:** Joel Odili-Lock (i6272212), Paul Carl Giordanelli (i6350376), Marnix Heutz (i6326166)

---

## Project Overview

This pipeline transforms four raw CSV source files into a structured analytical warehouse with eleven business-facing mart tables, covering all twelve business questions from the Finance, Customer Analytics, and Merchant Partnerships teams.

---

## Tech Stack

- **Python 3** — data loading and transformation scripts
- **PostgreSQL 18** — database, warehouse, and marts
- **psycopg2** — PostgreSQL connection for DDL and DML operations
- **SQLAlchemy + pandas** — efficient reading of large tables into Python
- **pgAdmin 4** — database management and query interface

---

## Repository Structure

```
ClearSpend_DataPlatform/
│
├── 01_data/
│   └── raw/                          # Raw CSV source files
│       ├── users_data.csv
│       ├── cards_data.csv
│       └── mcc_data.csv
│       # transactions_data.csv excluded — exceeds GitHub's 100MB limit
│
├── 02_ingestion/
│   ├── ingestion_DDL.py              # Creates database, schemas, and raw tables
│   └── ingestion_load.py             # Bulk loads CSV files into ingestion schema
│
├── 03_transformation/
│   ├── transformation_transactions.py # Cleans transactions using SQL CTAS
│   ├── transformation_cards.py        # Cleans cards table using Python
│   ├── transformation_users.py        # Cleans users table using Python
│   └── transformation_mcc.py          # Cleans MCC reference table using Python
│
├── 04_warehouse/
│   ├── dim_date.py                   # Generates calendar dimension
│   ├── dim_customer.py               # Builds customer dimension
│   ├── dim_card.py                   # Builds card dimension
│   ├── dim_merchant.py               # Derives merchant dimension from transactions
│   ├── dim_mcc.py                    # Builds MCC category dimension
│   └── fact_transactions.py          # Builds central fact table (13.3M rows)
│
├── 05_marts/
│   ├── mart_finance.py               # 3 tables for Finance team
│   ├── mart_customer.py              # 4 tables for Customer Analytics team
│   └── mart_merchant.py              # 4 tables for Merchant Partnerships team
│
├── 06_docs/
│   └── clearspend_star_schema.drawio # Star schema diagram
│
├── .gitignore                        # Excludes transactions_data.csv
└── README.md
```

---

## Prerequisites

1. **Python 3.x** with the following packages:
```bash
pip3 install psycopg2-binary sqlalchemy pandas
```

2. **PostgreSQL 18** running locally on port 5432 with:
   - User: `postgres`
   - Password: your local password (update in each script if different from default)

3. **Raw data files** placed in `01_data/raw/`:
   - `transactions_data.csv` (~1.2GB, not in GitHub)
   - `users_data.csv`
   - `cards_data.csv`
   - `mcc_data.csv`

---

## How to Run the Pipeline

Run scripts in the following order. Each script drops and recreates its target tables, so the pipeline can be safely re-run from any step.

### Step 1 — Create database and raw tables
```bash
python3 02_ingestion/ingestion_DDL.py
```
Creates the `clearspend` database and all four raw tables in the `ingestion` schema.

### Step 2 — Load raw data
```bash
python3 02_ingestion/ingestion_load.py
```
Bulk loads all four CSV files using PostgreSQL's COPY command. The transactions file (~13.3M rows) takes a few minutes.

### Step 3 — Transform the data
```bash
python3 03_transformation/transformation_mcc.py
python3 03_transformation/transformation_users.py
python3 03_transformation/transformation_cards.py
python3 03_transformation/transformation_transactions.py
```
These four scripts can be run in any order. Each reads from `ingestion` and writes cleaned data to the `transformation` schema.

### Step 4 — Build the warehouse dimensions
```bash
python3 04_warehouse/dim_date.py
python3 04_warehouse/dim_customer.py
python3 04_warehouse/dim_card.py
python3 04_warehouse/dim_merchant.py
python3 04_warehouse/dim_mcc.py
```
These five scripts can be run in any order.

### Step 5 — Build the fact table
```bash
python3 04_warehouse/fact_transactions.py
```
Must run **after** all five dimension scripts. Joins 13.3M transactions to all dimensions using a pure SQL INSERT SELECT statement.

### Step 6 — Build the data marts
```bash
python3 05_marts/mart_finance.py
python3 05_marts/mart_customer.py
python3 05_marts/mart_merchant.py
```
Must run **after** the fact table is built. These can be run in any order.

---

## Database Schemas

| Schema | Tables | Purpose |
|---|---|---|
| `ingestion` | 4 tables | Raw data, unchanged from source |
| `transformation` | 4 tables | Cleaned and properly typed data |
| `curated` | 6 tables | Star schema warehouse (5 dims + 1 fact) |
| `marts` | 11 tables | Pre-computed business team reporting |

---

## Data Mart Tables

| Team | Table | Business Question |
|---|---|---|
| Finance | `marts.finance_monthly` | Revenue by month + refund rate |
| Finance | `marts.finance_by_state` | Revenue by state |
| Finance | `marts.finance_by_category` | Spending by merchant category |
| Customer Analytics | `marts.customer_ltv` | Customer lifetime value |
| Customer Analytics | `marts.customer_channel` | Online vs in-store behaviour |
| Customer Analytics | `marts.customer_cards` | Active cards per customer |
| Customer Analytics | `marts.suspicious_transactions` | Suspicious transaction patterns |
| Merchant Partnerships | `marts.merchant_performance` | Merchants by transaction volume |
| Merchant Partnerships | `marts.industry_growth` | Fastest growing industries YoY |
| Merchant Partnerships | `marts.merchant_errors` | Merchants with highest error rates |
| Merchant Partnerships | `marts.revenue_by_geography` | Revenue by state and city |

---

## Notes

- `transactions_data.csv` is excluded from GitHub as it exceeds the 100MB file size limit. Place it manually in `01_data/raw/` before running the pipeline.
- All credentials are currently hardcoded. In a production environment these would be moved to environment variables.
- The transformation for `transactions_data` uses a pure SQL CTAS approach rather than pandas to avoid loading 13.3M rows into memory.
