# ============================================
# 04_merge_logic.py
# Incremental MERGE into Silver Layer
# ============================================

from delta.tables import DeltaTable

database_name = "workspace.default"
silver_table = f"{database_name}.silver_shipments"

# Step 1: Get Delta table reference
delta_target = DeltaTable.forName(spark, silver_table)

# Step 2: Merge new snapshot into existing Silver table
delta_target.alias("t") \
    .merge(
        ship_latest.alias("s"),
        "t.shipment_id = s.shipment_id"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

print("MERGE operation completed successfully.")

