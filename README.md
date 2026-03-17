# dwh-etl-interview-snippets

Oracle DWH ETL interview snippets: SQL DDL (partitioning, indexing, SCD7), PL/SQL package and Python ETL pipeline - anonymized for interview use

## Overview

This repository contains production-inspired examples of Data Warehouse and ETL architecture patterns used in enterprise banking/financial DWH environments.

### What's Included

- **dwh_etl_oracle.sql** - Oracle DDL and PL/SQL implementation of SCD Type 7 (Slowly Changing Dimension):
  - `DIM_LOAN_ATTR_HIST` - History table with temporal validity (SCD Type 2)
  - `DIM_LOAN_ATTR_CURR` - Current snapshot table (SCD Type 1)
  - Partitioned fact table with sub-partitioning strategy
  - PL/SQL package with hash generation and MERGE-based load procedures
  - Performance hints and indexing strategy

- **dwh_etl_pipeline.py** - Python ETL pipeline demonstrating:
  - Data transformation and deduplication
  - SHA-256 technical key generation (HUB/SAT model)
  - SCD7 pattern orchestration (history + current table split)
  - Change detection via SAT key comparison
  - Structured logging and error handling

## Key Concepts

### SCD Type 7 Pattern

The SCD7 implementation combines SCD1 (current snapshot) and SCD2 (full history):

1. **HISTORY table** - Maintains all versions with effective dating (X_EFFECTIVE_FROM_DT, X_EFFECTIVE_TO_DT)
2. **CURRENT table** - Always reflects the latest state (no historical tracking)
3. **Materialized View** - Optional bridge providing current view of history

This pattern optimizes for both analytical queries (history) and operational queries (current state) without duplicating data.

### Partitioning Strategy

- **HISTORY table**: Partitioned `RANGE` on `X_EFFECTIVE_FROM_DT` with `INTERVAL (MONTH)` for automatic partition creation
  - Enables partition pruning on temporal queries
  - Improves performance for historical analysis

- **CURRENT table**: No partitioning (small, frequently accessed, SCD1 semantics)

- **FACT table**: Composite `LIST` (REF_MONTH_ID) + `LIST` (SOURCE_SYSTEM_CD) sub-partitions
  - Enables parallel loads by source system
  - Supports monthly data lifecycle management

### Key Generation

- **X_HUB_KEY**: SHA256 hash of business key (CONTRACT_ID # BANK_ID)
- **X_SAT_KEY**: SHA256 hash of attributes (LOAN_STATUS_CD # OUTSTANDING_AMT # RATE_TYPE_CD)
  - Used for change detection: if SAT_KEY differs from current, record has changed

## Usage

The SQL can be executed directly in Oracle 19c+ environments. The Python pipeline requires:

```bash
pip install pandas
python dwh_etl_pipeline.py
```

## Notes

All examples are simplified and anonymized, representative of production patterns but without sensitive data or specific implementation details. This structure is typical for banking/lending DWH where temporal accuracy, audit trails, and performance are critical.
