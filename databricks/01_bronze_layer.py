# ============================================
# 01_bronze_layer.py
# Bronze Layer - Raw Ingestion into Delta
# ============================================

from pyspark.sql import functions as F

# Step 1: Define target database (Unity Catalog assumed)
database_name = "workspace.default"

# Step 2: Read raw generated data (assuming already created DataFrames)
# Replace with your raw source if needed
ship_raw = spark.table(f"{database_name}.raw_shipments")
cust_raw = spark.table(f"{database_name}.raw_customers")
event_raw = spark.table(f"{database_name}.raw_events")

# Step 3: Write raw data as Delta Bronze tables
ship_raw.write.format("delta").mode("overwrite").saveAsTable(f"{database_name}.bronze_shipments")
cust_raw.write.format("delta").mode("overwrite").saveAsTable(f"{database_name}.bronze_customers")
event_raw.write.format("delta").mode("overwrite").saveAsTable(f"{database_name}.bronze_events")

print("Bronze layer tables created successfully.")

