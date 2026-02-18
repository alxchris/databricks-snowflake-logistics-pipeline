# ============================================
# 03_gold_layer.py
# Gold Layer - KPI Aggregations
# ============================================

from pyspark.sql import functions as F

database_name = "workspace.default"

silver_ship = spark.table(f"{database_name}.silver_shipments")
silver_cust = spark.table(f"{database_name}.silver_customers")

# 1️⃣ Lane KPIs
gold_lane = (
    silver_ship
    .groupBy("origin_port", "origin_region", "dest_port", "dest_region", "service_level")
    .agg(
        F.count("*").alias("shipments"),
        F.avg("transit_hours").alias("avg_transit_hours"),
        F.avg("on_time_flag").alias("on_time_rate"),
        F.sum("shipment_cost_usd").alias("total_spend_usd")
    )
)

gold_lane.write.format("delta").mode("overwrite").saveAsTable(f"{database_name}.gold_lane_kpis")

# 2️⃣ Customer Monthly KPIs
gold_customer = (
    silver_ship
    .withColumn("month", F.trunc("shipment_date", "month"))
    .groupBy("customer_id", "customer_name", "segment", "industry", "month")
    .agg(
        F.count("*").alias("shipments"),
        F.sum("shipment_cost_usd").alias("total_spend_usd"),
        F.avg("shipment_cost_usd").alias("avg_shipment_cost_usd"),
        F.avg("on_time_flag").alias("on_time_rate"),
        F.avg("delivered_flag").alias("delivered_rate")
    )
)

gold_customer.write.format("delta").mode("overwrite").saveAsTable(f"{database_name}.gold_customer_monthly_kpis")

# 3️⃣ Latest Shipment Event Status
gold_event = (
    silver_ship
    .groupBy("shipment_id")
    .agg(
        F.max("event_ts").alias("latest_event_ts"),
        F.last("event_type").alias("latest_event_type"),
        F.last("event_location_port").alias("latest_event_location_port"),
        F.last("event_notes").alias("latest_event_notes")
    )
)

gold_event.write.format("delta").mode("overwrite").saveAsTable(f"{database_name}.gold_latest_shipment_event_status")

print("Gold KPI tables created successfully.")

