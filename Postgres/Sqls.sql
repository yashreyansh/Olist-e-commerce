create schema OLIST;
CREATE TABLE IF NOT EXISTS OLIST.audit_log (
    audit_id SERIAL PRIMARY KEY,  -- Auto-increment primary key
    event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    run_id VARCHAR(50),  -- Not primary key - same run can have multiple events
    event_type VARCHAR(100),
    status VARCHAR(20),
    resp_message VARCHAR(500),  -- Increased size for longer messages
    CONSTRAINT valid_status CHECK (status IN ('Started', 'In-Progress', 'Completed', 'Failed'))
);
CREATE INDEX idx_audit_run_id ON OLIST.audit_log(run_id);
CREATE INDEX idx_audit_event_time ON OLIST.audit_log(event_time DESC);



-- Main tables
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

-- Staging tables (same structure)
CREATE TABLE OLIST.payment_facts_staging (LIKE payment_facts);
select  order_id, payment_sequential , count(*) from OLIST.payment_facts_staging 
group by order_id, payment_sequential having count(*)>1;
select distinct source_payment_file From   OLIST.payment_facts;
selecT distinct source_order_file from OLIST.order_summary ;
delete from OLIST.order_summary;
delete from  OLIST.payment_facts;
delete from OLIST.sync_tracking;
select * from OLIST.sync_tracking;
select * from OLIST.audit_log;

CREATE TABLE OLIST.order_summary_staging (LIKE order_summary);

-- Sync tracking table
CREATE TABLE OLIST.sync_tracking (
    table_name VARCHAR PRIMARY KEY,
    last_version BIGINT,
    last_sync_time TIMESTAMP,
    run_id VARCHAR
);