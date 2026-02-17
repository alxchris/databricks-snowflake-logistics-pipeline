# databricks-snowflake-logistics-pipeline
🚀 **Databricks → Snowflake Logistics Analytics Pipeline
Production-Style Cross-Platform Data Engineering Case Study**

This project demonstrates a production-style data engineering pipeline built using:
•	**Databricks (Delta Lake, Unity Catalog)
•	Snowflake (Cloud Data Warehouse)**
•	Parquet-based data exchange
•	Incremental upserts (MERGE)
•	Data quality enforcement

The pipeline simulates a logistics company processing shipment, customer, and operational event data to produce business-ready KPIs.
________________________________________
🏗 Architecture Overview

Raw Data Generation
        ↓
Databricks Bronze (Raw Delta)
        ↓
Databricks Silver (Cleaned + Deduplicated)
        ↓
Databricks Gold (Business KPIs)
        ↓
Export as Parquet
        ↓
Snowflake Internal Stages
        ↓
Structured Warehouse Tables

Databricks handles transformation and data quality logic.
Snowflake serves as the analytical warehouse layer.
________________________________________
🧱 Databricks Layering
**Bronze Layer**
  •	Raw ingestion
  •	Schema-on-read
  •	Stored in Delta format
**Silver Layer**
  •	Deduplication using Delta MERGE
  •	Business rule enforcement:
      o	Remove negative shipment weights
      o	Validate origin/destination ports
  •	Ensures 1 record per shipment_id
**Gold Layer**
Curated KPI tables:
  •	gold_customer_monthly_kpis
  •	gold_lane_kpis
  •	gold_latest_shipment_event_status

  🔁 Incremental Processing (MERGE Logic)
  Delta Lake MERGE used for idempotent upserts:

  Python:
  delta_target.alias("t") \
  .merge(
      ship_latest.alias("s"),
      "t.shipment_id = s.shipment_id"
  ) \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()
  
This simulates production-grade incremental processing.
________________________________________
📦 Snowflake Integration
Due to serverless compute constraints, Gold datasets were:
1.Exported as Parquet from Databricks
2.Uploaded to Snowflake internal stages
3.Loaded using CTAS with structured column casting

Example:
SQL:
CREATE OR REPLACE TABLE GOLD_LANE_KPIS AS
SELECT
  $1:origin_port::STRING AS origin_port,
  $1:dest_port::STRING AS dest_port,
  $1:shipments::NUMBER AS shipments
FROM @CASE02_LANE_STAGE
(FILE_FORMAT => 'CASE02_PARQUET');
________________________________________
📊**Warehouse Output Tables**
**GOLD_LANE_KPIS**
Lane performance metrics:
•	Shipments
•	Avg transit hours
•	On-time rate
•	Total spend

**GOLD_CUSTOMER_MONTHLY_KPIS**
Customer-level KPIs:
•	Monthly shipment volume
•	Average shipment cost
•	Delivery performance

**GOLD_LATEST_SHIPMENT_EVENT_STATUS**
Operational snapshot:
•	Latest shipment event
•	Event timestamp
•	Location
•	Notes
________________________________________
🛡 Data Quality Controls
•	Negative weights filtered out
•	Port dimension validation
•	Deduplication using grouping + MERGE
________________________________________
📈 Final Row Counts
| Table                             | Rows   |
| --------------------------------- | ------ |
| GOLD_LANE_KPIS                    | 26,922 |
| GOLD_CUSTOMER_MONTHLY_KPIS        | 23,328 |
| GOLD_LATEST_SHIPMENT_EVENT_STATUS | 73,438 |
________________________________________
🔧 Technologies Used
•	Apache Spark (PySpark)
•	Delta Lake
•	Unity Catalog
•	Snowflake
•	Parquet
•	SQL
•	Data Warehousing concepts
________________________________________
🎯 What This Demonstrates
•	Multi-layer medallion architecture
•	Delta Lake MERGE logic
•	Data quality enforcement
•	Cross-platform integration (Databricks → Snowflake)
•	Warehouse-ready structured modeling
________________________________________
📌 Author
Alexander Christodoulou
Senior Database Engineer transitioning into modern Data Engineering & Analytics.

