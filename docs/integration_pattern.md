# Integration Pattern – Databricks → Snowflake

## Overview

This project implements a cross-platform data engineering integration pattern where:

- Databricks handles:
  - Raw ingestion (Bronze)
  - Cleansing, deduplication (Silver)
  - Business KPI transformations (Gold)
  - Incremental merge logic (Delta Lake MERGE)

- Snowflake serves as:
  - Analytical serving warehouse
  - Structured SQL-based consumption layer
  - BI-ready dataset provider

---

## Architectural Pattern Used

### Pattern Type:
**Parquet-based Batch Data Exchange via Internal Stages**

### Flow:

Databricks Gold (Delta)  
↓  
Export as Parquet  
↓  
Upload to Snowflake Internal Stage  
↓  
Snowflake External Table Read  
↓  
Structured Table Creation  

---

## Why This Pattern?

### 1️⃣ Decoupling of Compute Engines
Databricks and Snowflake remain independent.
No direct JDBC dependency between systems.

### 2️⃣ Storage-Level Interoperability
Parquet provides:
- Open format
- Schema evolution support
- Efficient compression
- Cross-engine compatibility

### 3️⃣ Clear Responsibility Split

| Layer | Responsibility |
|-------|---------------|
| Databricks | Transformations, data quality, incremental logic |
| Snowflake | Analytical serving, SQL querying, BI consumption |

---

## Alternative Integration Options (Not Used Here)

| Option | Reason Not Used |
|--------|----------------|
| Direct Spark → Snowflake Connector | Tighter coupling between engines |
| Snowpipe Streaming | Not required for batch KPI case |
| External S3 Stage | Adds unnecessary infrastructure complexity |

---

## When to Use This Pattern

This integration approach is ideal when:

- You want transformation and ML workloads in Databricks
- You want SQL-first consumption in Snowflake
- Teams are separated by platform ownership
- You require controlled batch data promotion to warehouse

---

## Production Considerations

In a real production environment, the following enhancements would be added:

- Orchestration (Airflow / Databricks Workflows)
- CI/CD deployment for SQL and notebooks
- Data validation before Snowflake load
- Monitoring on stage ingestion
- Schema evolution control
- Automated watermark-based incremental loads

---

## Key Takeaway

This case study demonstrates a production-style, decoupled, cross-platform data architecture using open formats and layered design principles.

