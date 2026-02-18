# ============================================
# data_quality_rules.py
# Explicit Data Quality Controls
# ============================================

from pyspark.sql import functions as F

database_name = "workspace.default"
silver_ship = spark.table(f"{database_name}.silver_shipments")

# Rule 1: Ensure no negative weights
invalid_weights = silver_ship.filter(F.col("weight_kg") <= 0).count()
print("Invalid weight records:", invalid_weights)

# Rule 2: Check for duplicate shipment_ids
duplicates = (
    silver_ship
    .groupBy("shipment_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Duplicate shipment_ids:", duplicates)

print("Data quality checks completed.")

