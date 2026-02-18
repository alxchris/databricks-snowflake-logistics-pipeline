# ============================================
# 02_silver_layer.py
# Silver Layer - Cleaning & Deduplication
# ============================================

from pyspark.sql import functions as F

database_name = "workspace.default"

# Step 1: Load Bronze tables
bronze_ship = spark.table(f"{database_name}.bronze_shipments")
bronze_ports = spark.table(f"{database_name}.bronze_ports")

# Step 2: Remove negative shipment weights (Data Quality Rule #1)
ship_clean = bronze_ship.filter(F.col("weight_kg") > 0)

# Step 3: Validate origin/destination ports (Data Quality Rule #2)
valid_ports = bronze_ports.select("port_code").distinct()

ship_clean = (
    ship_clean
    .join(valid_ports.withColumnRenamed("port_code", "origin_port"), on="origin_port", how="inner")
    .join(valid_ports.withColumnRenamed("port_code", "dest_port"), on="dest_port", how="inner")
)

# Step 4: Deduplicate by shipment_id
ship_latest = (
    ship_clean
    .withColumn("row_num", F.row_number().over(
        Window.partitionBy("shipment_id").orderBy(F.col("event_ts").desc())
    ))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# Step 5: Save as Silver table
ship_latest.write.format("delta").mode("overwrite").saveAsTable(f"{database_name}.silver_shipments")

print("Silver layer created successfully.")

