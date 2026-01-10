# Olist E-Commerce Data Pipeline

A production-grade end-to-end data engineering pipeline for processing and analyzing Brazilian e-commerce data from Olist. Built with Apache Spark, Delta Lake, Apache Airflow, and PostgreSQL.

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.x-orange.svg)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-2.x-green.svg)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-red.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14+-blue.svg)

## 📋 Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [Data Flow](#data-flow)
- [Monitoring & Logging](#monitoring--logging)
- [Contributing](#contributing)

## 🎯 Overview

This project implements a scalable data pipeline for processing Brazilian e-commerce order and payment data. The pipeline handles:
- **Incremental data processing** from raw parquet files
- **Delta Lake tables** with Change Data Feed (CDF) for efficient tracking
- **Automated synchronization** to PostgreSQL for analytics
- **Full audit trail** with comprehensive logging
- **Orchestration** via Apache Airflow

## 🏗️ Architecture
```
┌─────────────────┐
│  Landing Zone   │  Raw Parquet Files
│  (Order/Payment)│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  LoadToSilver   │  PySpark Processing
│     Script      │  - Data Transformation
└────────┬────────┘  - Schema Validation
         │           - Audit Fields
         ▼
┌─────────────────┐
│   Delta Lake    │  Silver Layer
│  (Payment Fact) │  - Payment Facts
│ (Order Summary) │  - Order Summary
└────────┬────────┘  - CDF Enabled
         │
         ▼
┌─────────────────┐
│ SyncToPostgres  │  Incremental Sync
│     Script      │  - CDF-based
└────────┬────────┘  - UPSERT Logic
         │
         ▼
┌─────────────────┐
│   PostgreSQL    │  Analytics Layer
│  (OLIST Schema) │  - Payment Facts
└─────────────────┘  - Order Summary
                     - Sync Tracking
                     - Audit Logs
```

## ✨ Features

### Data Processing
- ✅ **Incremental Processing**: Only processes new/changed data
- ✅ **Delta Lake Integration**: ACID transactions with time-travel capabilities
- ✅ **Change Data Feed (CDF)**: Efficient tracking of data changes
- ✅ **Schema Evolution**: Handles schema changes gracefully
- ✅ **Data Deduplication**: Prevents duplicate records

### Data Quality
- ✅ **Audit Fields**: Tracks source files, job IDs, timestamps
- ✅ **Data Validation**: Type casting and null handling
- ✅ **Composite Keys**: Supports multi-column primary keys
- ✅ **UPSERT Logic**: Smart insert/update handling in PostgreSQL

### Operational Excellence
- ✅ **Comprehensive Logging**: Audit trail for all operations
- ✅ **Error Handling**: Graceful failure recovery
- ✅ **File Archiving**: Processed files moved to archive
- ✅ **Version Tracking**: Sync state management

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Processing** | Apache Spark (PySpark) | Distributed data processing |
| **Storage** | Delta Lake | ACID-compliant data lake |
| **Database** | PostgreSQL | Analytics and reporting |
| **Orchestration** | Apache Airflow | Workflow management |
| **Language** | Python 3.8+ | Primary programming language |

## 📁 Project Structure
```
Olist-e-commerce/
├── dags/                           # Airflow DAG definitions
│   ├── olist_file_processing.py    # File processing orchestration
│   └── olist_postgres_sync.py      # Postgres sync orchestration
│
├── gamma/Scripts/                  # Core processing scripts
│   ├── LoadToSilver.py            # Landing → Delta Lake
│   ├── SyncToPostgres.py          # Delta Lake → Postgres
│   │
│   └── dependencies/               # Utility modules
│       ├── spark.py               # Spark session management
│       ├── Add_log.py             # Audit logging
│       └── archieveFiles.py       # File archiving
│
├── data/Olist_e-commerce/         # Data directories
│   ├── Order_chunks/              # Raw order files
│   │   └── processed/             # Archived orders
│   ├── Payment_chunks/            # Raw payment files
│   │   └── processed/             # Archived payments
│   └── SilverLayer/               # Delta tables
│       ├── Payment_fact/          # Payment transactions
│       └── Order_summary/         # Order aggregates
│
├── sql/                           # Database schemas
│   ├── create_tables.sql          # PostgreSQL table definitions
│   └── indexes.sql                # Performance indexes
│
├── README.md                      # This file
└── requirements.txt               # Python dependencies
```

## 🚀 Setup Instructions

### Prerequisites
- Python 3.8 or higher
- Apache Spark 3.x
- PostgreSQL 14+
- Apache Airflow 2.x
- 8GB+ RAM recommended

### 1. Clone Repository
```bash
git clone https://github.com/yashreyansh/Olist-e-commerce.git
cd Olist-e-commerce
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

**requirements.txt:**
```txt
pyspark==3.4.0
delta-spark==2.4.0
psycopg2-binary==2.9.6
apache-airflow==2.6.0
pandas==2.0.0
```

### 3. Configure PostgreSQL
```bash
# Create database
psql -U postgres -c "CREATE DATABASE data_db;"

# Run schema setup
psql -U postgres -d data_db -f sql/create_tables.sql
```

**sql/create_tables.sql:**
```sql
-- Create schema
CREATE SCHEMA IF NOT EXISTS OLIST;

-- Payment Facts
CREATE TABLE OLIST.payment_facts (
    order_id VARCHAR NOT NULL,
    payment_sequential BIGINT NOT NULL,
    payment_installments INTEGER,
    payment_type VARCHAR,
    payment_value FLOAT,
    proc_run_id VARCHAR,
    source_payment_file VARCHAR,
    created_on TIMESTAMP,
    updated_on TIMESTAMP,
    PRIMARY KEY (order_id, payment_sequential)
);

-- Order Summary
CREATE TABLE OLIST.order_summary (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR,
    order_status VARCHAR,
    order_approved_at TIMESTAMP,
    order_purchase_timestamp TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    no_of_payments BIGINT,
    payment_installments INTEGER,
    payment_types_used VARCHAR,
    total_payment_value FLOAT,
    created_by_proc_run_id VARCHAR,
    updated_by_proc_run_id VARCHAR,
    source_order_file VARCHAR,
    source_payment_file VARCHAR,
    created_on TIMESTAMP,
    updated_on TIMESTAMP
);

-- Staging Tables
CREATE TABLE OLIST.payment_facts_staging (LIKE OLIST.payment_facts);
CREATE TABLE OLIST.order_summary_staging (LIKE OLIST.order_summary);

-- Sync Tracking
CREATE TABLE OLIST.sync_tracking (
    table_name VARCHAR PRIMARY KEY,
    last_version BIGINT,
    last_sync_time TIMESTAMP,
    run_id VARCHAR
);

-- Audit Log
CREATE TABLE OLIST.audit_log (
    audit_id SERIAL PRIMARY KEY,
    event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    run_id VARCHAR(50),
    event_type VARCHAR(100),
    status VARCHAR(20),
    resp_message VARCHAR(500),
    CONSTRAINT valid_status CHECK (status IN ('Started', 'In-Progress', 'Completed', 'Failed'))
);

-- Indexes
CREATE INDEX idx_audit_run_id ON OLIST.audit_log(run_id);
CREATE INDEX idx_audit_time ON OLIST.audit_log(event_time DESC);
CREATE INDEX idx_payment_order ON OLIST.payment_facts(order_id);
```

### 4. Configure Connection Settings

Update `SyncToPostgres.py`:
```python
postgres_config = {
    'host': 'localhost',     # Your Postgres host
    'port': 5432,
    'database': 'data_db',
    'user': 'your_user',
    'password': 'your_password'
}
```

### 5. Set Up Airflow (Optional)
```bash
# Initialize Airflow
export AIRFLOW_HOME=~/airflow
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Copy DAGs
cp dags/*.py $AIRFLOW_HOME/dags/

# Start services
airflow webserver --port 8080 &
airflow scheduler &
```

## 💻 Usage

### Manual Execution

#### 1. Process Files to Delta Lake
```bash
python gamma/Scripts/LoadToSilver.py
```

**What it does:**
- Reads parquet files from landing zone
- Transforms and validates data
- Merges into Delta Lake tables
- Archives processed files
- Adds audit metadata

#### 2. Sync Delta to PostgreSQL
```bash
python gamma/Scripts/SyncToPostgres.py
```

**What it does:**
- Reads changes from Delta (CDF)
- Writes to PostgreSQL staging
- Executes UPSERT operations
- Updates sync tracking

### Airflow Execution

Access Airflow UI: `http://localhost:8080`

**Available DAGs:**
- `olist_file_processing` - Runs LoadToSilver.py
- `olist_postgres_sync` - Runs SyncToPostgres.py

Trigger manually or set schedule:
```python
# In DAG file
schedule_interval='*/15 * * * *'  # Every 15 minutes
```

## 🔄 Data Flow

### Phase 1: Landing → Delta Lake
```python
# Example: Processing order file
Order File (10.parquet)
    ↓ Read & Transform
    ├─ Add proc_run_id: "scheduled_2026-01-08T04:40:00"
    ├─ Add source_order_file: "10.parquet"
    ├─ Add created_on: current_timestamp()
    ├─ Convert timestamps
    ↓ Merge to Delta
    └─ Payment_fact v2 (50 new records)
```

### Phase 2: Delta Lake → PostgreSQL
```python
# Example: Incremental sync
Delta Table (v2)
    ↓ Read CDF (v1 → v2)
    ├─ 50 changed records
    ├─ Filter: INSERT + UPDATE_POSTIMAGE
    ↓ Write to Staging
    ├─ TRUNCATE staging
    ├─ INSERT 50 records
    ↓ UPSERT to Main
    ├─ 30 INSERTs (new)
    ├─ 20 UPDATEs (existing)
    └─ Update sync_tracking: last_version = 2
```

## 📊 Monitoring & Logging

### Query Audit Logs
```sql
-- Recent sync jobs
SELECT run_id, event_type, status, resp_message, event_time
FROM OLIST.audit_log
WHERE event_time > NOW() - INTERVAL '1 day'
ORDER BY event_time DESC;

-- Failed operations
SELECT * FROM OLIST.audit_log
WHERE status = 'Failed'
ORDER BY event_time DESC;
```

### Check Sync Status
```sql
-- Sync tracking
SELECT table_name, last_version, last_sync_time
FROM OLIST.sync_tracking;

-- Record counts
SELECT 
    'payment_facts' as table_name,
    COUNT(*) as record_count
FROM OLIST.payment_facts
UNION ALL
SELECT 
    'order_summary',
    COUNT(*)
FROM OLIST.order_summary;
```

### Delta Lake Metrics
```python
from delta.tables import DeltaTable

# Check versions
delta_table = DeltaTable.forPath(spark, "/path/to/delta")
delta_table.history().show()

# View changes
changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .load("/path/to/delta")
changes.groupBy("_change_type").count().show()
```

## 🎯 Key Design Patterns

### 1. Change Data Feed (CDF)
- Only syncs changed records (not full table)
- Reduces network/processing overhead
- Enables time-travel queries

### 2. UPSERT Pattern
```sql
INSERT INTO target_table
SELECT * FROM staging_table
ON CONFLICT (primary_key)
DO UPDATE SET column = EXCLUDED.column;
```

### 3. Audit Trail
Every operation logged with:
- Unique run_id
- Source file names
- Processing timestamps
- Success/failure status

### 4. Idempotent Processing
- Can re-run safely
- Handles duplicates
- Version-based sync tracking

## 🐛 Troubleshooting

### Issue: "Change Data Feed not enabled"
**Solution:**
```python
# Enable CDF when creating Delta table
.option("delta.enableChangeDataFeed", "true")
```

### Issue: Duplicate records in Delta
**Solution:**
```python
# Add deduplication before merge
payment_df = payment_df.dropDuplicates(["order_id", "payment_sequential"])
```

### Issue: Version mismatch error
**Solution:**
```sql
-- Reset sync tracking
UPDATE OLIST.sync_tracking 
SET last_version = 0 
WHERE table_name = 'your_table';
```

## 📈 Performance Tips

1. **Partition Delta tables** for large datasets:
```python
.partitionBy("year", "month")
```

2. **Optimize Delta tables** regularly:
```python
delta_table.optimize().executeCompaction()
```

3. **Vacuum old versions** (after 7 days):
```python
delta_table.vacuum(168)  # hours
```

4. **Index PostgreSQL** frequently queried columns:
```sql
CREATE INDEX idx_order_date ON OLIST.order_summary(order_purchase_timestamp);
```

## 🤝 Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## 📝 License

This project is licensed under the MIT License.

## 👤 Author

**Yash Reyansh**
- GitHub: [@yashreyansh](https://github.com/yashreyansh)

## 🙏 Acknowledgments

- Olist for providing the Brazilian e-commerce dataset
- Apache Spark & Delta Lake communities
- Contributors and reviewers

---

**⭐ Star this repo if you find it helpful!**
