# Atlikon x SportsBar - Data Consolidation Pipeline

Atlikon, a sports products company, recently acquired **SportsBar**, an energy drinks and nutrition brand. SportsBar had poorly maintained operational data with inconsistent formats, duplicates, misspellings, and missing values. This project builds an end-to-end ETL pipeline on **Databricks** that cleans SportsBar's raw data and merges it into Atlikon's unified **Gold Layer** data warehouse using the **Medallion Architecture** (Bronze → Silver → Gold).

---

## Business Context

| Company | Role | Products |
|---------|------|----------|
| **Atlikon** | Parent company | Sports products (existing data warehouse) |
| **SportsBar** | Acquired company | Energy bars, protein bars, granola, dairy, snacks, electrolytes |

**Goal:** Integrate SportsBar's messy customer, product, pricing, and order data into Atlikon's existing gold layer so that downstream analytics and reporting treat both companies as one unified dataset.

---

## Architecture

```
                        Medallion Architecture (Delta Lake)

  RAW (S3)                 BRONZE                  SILVER                    GOLD
 ┌──────────┐         ┌──────────────┐       ┌──────────────┐        ┌─────────────────┐
 │customers │────────→│ Raw ingest   │──────→│ Deduplicated │──────→│ dim_customers    │
 │.csv      │         │ + metadata   │       │ + cleaned    │       │ (merged w/parent)│
 └──────────┘         └──────────────┘       └──────────────┘       └─────────────────┘
 ┌──────────┐         ┌──────────────┐       ┌──────────────┐        ┌─────────────────┐
 │products  │────────→│ Raw ingest   │──────→│ Deduplicated │──────→│ dim_products     │
 │.csv      │         │ + metadata   │       │ + categorized│       │ (merged w/parent)│
 └──────────┘         └──────────────┘       └──────────────┘       └─────────────────┘
 ┌──────────┐         ┌──────────────┐       ┌──────────────┐        ┌─────────────────┐
 │gross_    │────────→│ Raw ingest   │──────→│ Date & price │──────→│ dim_gross_price  │
 │price.csv │         │              │       │ normalized   │       │ (merged w/parent)│
 └──────────┘         └──────────────┘       └──────────────┘       └─────────────────┘
 ┌──────────┐         ┌──────────────┐       ┌──────────────┐        ┌─────────────────┐
 │orders/   │────────→│ Append from  │──────→│ Validated    │──────→│ fact_orders      │
 │daily CSVs│         │ landing/     │       │ + joined     │       │ (monthly agg,    │
 └──────────┘         └──────────────┘       └──────────────┘       │  merged w/parent)│
                                                                     └─────────────────┘
```

---

## Project Structure

```
Data Engineering Project/
│
├── 0_data/
│   ├── 1_parent_company/
│   │   └── incremental_load/
│   │       └── incremental_data_parent_company_query.txt   # Parent company load query
│   │
│   └── 2_child_company/                                     # SportsBar raw data
│       ├── full_load/
│       │   ├── customers/customers.csv                      # 39 customer records
│       │   ├── products/products.csv                        # 20 product records
│       │   ├── gross_price/gross_price.csv                  # Pricing data
│       │   └── orders/landing/                              # 80+ daily order CSVs (Jul-Nov 2025)
│       │
│       └── incremental_load/
│           └── orders/                                      # 27 daily order CSVs (Dec 2025)
│
├── 1_codes/
│   ├── 1_setup/
│   │   ├── setup_catalog.ipynb                              # Creates fmcg catalog & schemas
│   │   ├── utilities.ipynb                                  # Shared schema variables
│   │   └── dim_date_table_creation.ipynb                    # Date dimension table
│   │
│   ├── 2_dimension_data_processing/
│   │   ├── 1_customers_data_processing.ipynb                # Customer dimension ETL
│   │   ├── 2_products_data_processing.ipynb                 # Product dimension ETL
│   │   └── 3_pricing_data_processing.ipynb                  # Pricing dimension ETL
│   │
│   └── 3_fact_data_processing/
│       ├── 1_full_load_fact.ipynb                           # Historical orders (Jul-Nov)
│       └── 2_incremental_load_fact.ipynb                    # Incremental orders (Dec)
│
├── CLAUDE.md
└── README.md
```

---

## Data Pipeline Details

### 1. Setup (`1_setup/`)

| Notebook | Purpose |
|----------|---------|
| `setup_catalog.ipynb` | Creates the `fmcg` Unity Catalog and initializes `bronze`, `silver`, `gold` schemas |
| `utilities.ipynb` | Defines shared schema variables imported by all downstream notebooks |
| `dim_date_table_creation.ipynb` | Generates a monthly date dimension table (2024-01 to 2025-12) with year, quarter, and month attributes |

### 2. Dimension Processing (`2_dimension_data_processing/`)

#### Customers
- **Input:** 39 raw customer records from S3
- **Key Fixes:** Removed 4 duplicates, corrected city misspellings ("Bengalore" → "Bengaluru", "Hyderbad" → "Hyderabad", "NewDelhi" → "New Delhi"), filled 4 missing cities using business-confirmed values, applied title-casing, trimmed whitespace
- **Output:** 35 clean records merged into `fmcg.gold.dim_customers` with standardized fields (market=India, platform=Sports Bar, channel=Acquisition)

#### Products
- **Input:** 20 raw product records
- **Key Fixes:** Removed 2 duplicates, fixed "Protien" → "Protein" spelling, mapped 6 categories to divisions (e.g., Energy Bars → Nutrition Bars), extracted product variants (e.g., "60g") from names, generated deterministic product codes via SHA-256 hash, validated product IDs
- **Output:** 18 clean records merged into `fmcg.gold.dim_products`

#### Pricing
- **Input:** Gross price data with inconsistent date formats and invalid values
- **Key Fixes:** Normalized 4 different date formats, converted negative prices to positive, replaced non-numeric prices with 0, joined with products to resolve product codes, selected best price per product per year using window ranking
- **Output:** Clean pricing merged into `fmcg.gold.dim_gross_price`

### 3. Fact Processing (`3_fact_data_processing/`)

#### Full Load
- **Input:** 80+ daily order CSV files (July-November 2025), ~51,810 rows
- **Key Fixes:** Filtered null quantities, validated customer IDs (non-numeric → fallback ID 999999), normalized date formats (removed weekday names), dropped exact duplicates, joined with product dimension
- **Output:** Daily orders aggregated to monthly granularity (3,060 rows) and merged into `fmcg.gold.fact_orders`
- **File Management:** Processed files moved from `landing/` to `processed/` directory

#### Incremental Load
- **Input:** 27 daily order CSV files (December 2025), ~8,834 rows
- **Processing:** Same transformations as full load, but only recalculates monthly aggregates for affected months (December 2025)
- **Output:** 6,947 cleaned rows merged into parent `fact_orders` via staging tables, which are dropped after merge

---

## Data Quality Summary

| Issue | Affected Table | Fix Applied |
|-------|---------------|-------------|
| Duplicate records | Customers, Products | `dropDuplicates()` on primary key |
| City misspellings | Customers | Explicit mapping for 3 known typos |
| Missing city values | Customers | Business-confirmed lookup for 4 records |
| Category spelling ("Protien") | Products | Case-insensitive regex replacement |
| Invalid product IDs (non-numeric) | Products, Orders | Regex validation, fallback to 999999 |
| Inconsistent date formats (4 types) | Pricing, Orders | `try_to_date()` with multiple format patterns |
| Negative prices | Pricing | Absolute value conversion |
| Non-numeric prices ("unknown") | Pricing | Replace with 0.0 |
| Dates with weekday names | Orders | Strip weekday prefix before parsing |
| Null order quantities | Orders | Filter out incomplete records |
| Non-numeric customer IDs | Orders | Regex validation, fallback to 999999 |

---

## Key Metrics

| Metric | Value |
|--------|-------|
| Customers (after cleaning) | 35 |
| Products (after cleaning) | 18 |
| Total order records processed | 47,758 |
| Full load records | 40,811 |
| Incremental load records | 6,947 |
| Date range | July 1, 2025 - December 31, 2025 |
| Daily order files processed | 107+ |
| Data quality rules applied | 15+ |

---

## Technology Stack

- **Platform:** Databricks (Apache Spark)
- **Storage Format:** Delta Lake (ACID transactions, change data feed)
- **Catalog:** Unity Catalog (`fmcg`)
- **Data Source:** AWS S3 (`s3://sportsbar-final/`)
- **Languages:** PySpark, SQL
- **Key Libraries:** `pyspark.sql.functions`, `delta.tables.DeltaTable`, `pyspark.sql.window.Window`

---

## Execution Order

Run the notebooks in this order (each depends on the previous steps):

```
1. setup_catalog.ipynb              → Creates catalog and schemas
2. utilities.ipynb                  → Defines shared variables (auto-imported)
3. dim_date_table_creation.ipynb    → Creates date dimension
4. 1_customers_data_processing.ipynb → Customers: bronze → silver → gold
5. 2_products_data_processing.ipynb  → Products: bronze → silver → gold
6. 3_pricing_data_processing.ipynb   → Pricing: bronze → silver → gold (depends on products)
7. 1_full_load_fact.ipynb            → Full historical orders (depends on products & customers)
8. 2_incremental_load_fact.ipynb     → Incremental order updates
```

---

## Gold Layer Schema

### dim_customers
| Column | Description |
|--------|-------------|
| customer_code | Primary key (customer ID) |
| customer_name | Cleaned customer name |
| city | Standardized city (Bengaluru, Hyderabad, New Delhi) |
| customer | Composite: customer_name + city |
| market | "India" |
| platform | "Sports Bar" |
| channel | "Acquisition" |

### dim_products
| Column | Description |
|--------|-------------|
| product_code | Primary key (SHA-256 hash of product name) |
| product_id | Original numeric product ID |
| division | Business division (e.g., Nutrition Bars) |
| category | Product category (e.g., Energy Bars) |
| product | Full product name |
| variant | Size/variant (e.g., 60g) |

### dim_gross_price
| Column | Description |
|--------|-------------|
| product_code | Foreign key to dim_products |
| price_inr | Price in INR |
| year | Pricing year |

### dim_date
| Column | Description |
|--------|-------------|
| date_key | YYYYMM format key |
| year | Calendar year |
| month_name | Full month name |
| month_short_name | Abbreviated month name |
| quarter | Quarter (Q1-Q4) |
| year_quarter | Year-quarter (e.g., 2025-Q3) |

### fact_orders
| Column | Description |
|--------|-------------|
| date | Month start date (monthly grain) |
| product_code | Foreign key to dim_products |
| customer_code | Foreign key to dim_customers |
| sold_quantity | Aggregated monthly quantity sold |

---

## Design Patterns

- **Medallion Architecture:** Raw → Bronze (audit trail) → Silver (cleaned) → Gold (business-ready, merged with parent)
- **Idempotent Operations:** `tableExists()` checks before creation; Delta Lake MERGE (upsert) for all writes
- **Incremental Processing:** Only recalculates affected months instead of full recomputation
- **Data Lineage:** File name, file size, and read timestamp tracked in bronze layer
- **Fallback IDs:** Invalid foreign keys default to 999999 to prevent join failures while flagging bad data
- **Parameterized Notebooks:** Widget parameters (`catalog`, `data_source`) allow reuse across environments
