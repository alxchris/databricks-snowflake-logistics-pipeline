# Databricks notebook: 00_generate_data.py
from pyspark.sql import functions as F
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

random.seed(42)

base_path = "dbfs:/FileStore/databricks_case02"  # easy for Community Edition
data_path = f"{base_path}/data"

# Clean old
dbutils.fs.rm(base_path, recurse=True)
dbutils.fs.mkdirs(data_path)

# -----------------------
# Parameters (adjustable)
# -----------------------
N_CUSTOMERS = 2000
N_PORTS = 120
N_SHIPMENTS = 80000      # safe-ish for CE; reduce if needed
N_EVENTS = 200000        # optional events size; reduce if slow

start_dt = datetime(2025, 1, 1)

countries = ["Greece","Netherlands","Singapore","USA","UAE","Germany","UK","Italy","Spain","Norway"]
regions = {"Greece":"EMEA","Netherlands":"EMEA","Germany":"EMEA","UK":"EMEA","Italy":"EMEA","Spain":"EMEA","Norway":"EMEA",
           "Singapore":"APAC","UAE":"EMEA","USA":"AMER"}

industries = ["Shipping","Energy","Retail","Manufacturing","Tech"]
segments = ["SMB","Enterprise"]
service_levels = ["Standard","Express"]
container_types = ["20GP","40GP","40HC"]
statuses = ["Planned","In Transit","Delivered","Delayed","Cancelled"]

# -----------------------
# Customers
# -----------------------
customers = []
for i in range(1, N_CUSTOMERS + 1):
    cid = f"C{i:06d}"
    country = random.choice(countries)
    customers.append((
        cid,
        f"Customer {i}",
        country,
        random.choice(industries),
        random.choice(segments),
        (start_dt + timedelta(days=random.randint(0, 365))).isoformat()
    ))

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), False),
    StructField("country", StringType(), False),
    StructField("industry", StringType(), False),
    StructField("segment", StringType(), False),
    StructField("created_at", StringType(), False),
])

df_customers = spark.createDataFrame(customers, schema=customers_schema) \
    .withColumn("created_at", F.to_timestamp("created_at"))

# -----------------------
# Ports
# -----------------------
# Create pseudo port codes like P001.. and map to countries/regions
ports = []
for i in range(1, N_PORTS + 1):
    code = f"P{i:03d}"
    country = random.choice(countries)
    ports.append((code, f"Port {code}", country, regions[country]))

ports_schema = StructType([
    StructField("port_code", StringType(), False),
    StructField("port_name", StringType(), False),
    StructField("country", StringType(), False),
    StructField("region", StringType(), False),
])
df_ports = spark.createDataFrame(ports, schema=ports_schema)

# -----------------------
# Shipments (main fact)
# -----------------------
# We will purposely create some duplicates with later updated_at for MERGE demo.
shipments = []
for i in range(1, N_SHIPMENTS + 1):
    sid = f"S{i:09d}"
    cid = f"C{random.randint(1, N_CUSTOMERS):06d}"
    o = f"P{random.randint(1, N_PORTS):03d}"
    d = f"P{random.randint(1, N_PORTS):03d}"
    svc = random.choice(service_levels)
    ctype = random.choice(container_types)

    weight = round(max(50.0, random.gauss(12000, 4000)), 2)
    # inject a few bad rows
    if random.random() < 0.002:
        weight = -abs(weight)

    distance = max(50, int(random.gauss(3500, 1200)))
    base_cost = round(distance * (0.55 if ctype == "20GP" else 0.75) * (1.2 if svc == "Express" else 1.0), 2)
    fuel = round(base_cost * random.uniform(0.05, 0.18), 2)
    total = round(base_cost + fuel, 2)

    planned_dep = start_dt + timedelta(days=random.randint(0, 365), hours=random.randint(0, 23))
    planned_arr = planned_dep + timedelta(days=random.randint(7, 30), hours=random.randint(0, 23))

    status = random.choices(statuses, weights=[0.15, 0.35, 0.35, 0.12, 0.03])[0]

    actual_dep = None
    actual_arr = None
    if status in ["In Transit","Delivered","Delayed"]:
        actual_dep = planned_dep + timedelta(hours=random.randint(-6, 24))
    if status in ["Delivered","Delayed"]:
        delay_hours = random.randint(0, 120) if status == "Delayed" else random.randint(-12, 24)
        actual_arr = planned_arr + timedelta(hours=delay_hours)

    updated_at = planned_dep + timedelta(days=random.randint(0, 10), hours=random.randint(0, 23))

    shipments.append((
        sid, cid, o, d, svc, ctype,
        float(weight), int(distance),
        float(base_cost), float(fuel), float(total),
        planned_dep.isoformat(),
        actual_dep.isoformat() if actual_dep else None,
        planned_arr.isoformat(),
        actual_arr.isoformat() if actual_arr else None,
        status,
        updated_at.isoformat()
    ))

    # duplicate update ~1.5%: later updated_at changes status or actual_arr
    if random.random() < 0.015:
        new_status = "Delivered" if status in ["In Transit","Delayed","Planned"] else status
        new_actual_arr = actual_arr
        if new_status == "Delivered" and new_actual_arr is None:
            new_actual_arr = planned_arr + timedelta(hours=random.randint(-6, 36))
        new_updated = updated_at + timedelta(days=random.randint(1, 4), hours=random.randint(0, 8))
        shipments.append((
            sid, cid, o, d, svc, ctype,
            float(weight), int(distance),
            float(base_cost), float(fuel), float(total),
            planned_dep.isoformat(),
            actual_dep.isoformat() if actual_dep else None,
            planned_arr.isoformat(),
            new_actual_arr.isoformat() if new_actual_arr else None,
            new_status,
            new_updated.isoformat()
        ))

shipments_schema = StructType([
    StructField("shipment_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("origin_port", StringType(), False),
    StructField("dest_port", StringType(), False),
    StructField("service_level", StringType(), False),
    StructField("container_type", StringType(), False),
    StructField("weight_kg", DoubleType(), False),
    StructField("distance_km", IntegerType(), False),
    StructField("base_cost_usd", DoubleType(), False),
    StructField("fuel_surcharge_usd", DoubleType(), False),
    StructField("total_cost_usd", DoubleType(), False),
    StructField("planned_departure_ts", StringType(), False),
    StructField("actual_departure_ts", StringType(), True),
    StructField("planned_arrival_ts", StringType(), False),
    StructField("actual_arrival_ts", StringType(), True),
    StructField("status", StringType(), False),
    StructField("updated_at", StringType(), False),
])
df_shipments = spark.createDataFrame(shipments, schema=shipments_schema) \
    .withColumn("planned_departure_ts", F.to_timestamp("planned_departure_ts")) \
    .withColumn("actual_departure_ts", F.to_timestamp("actual_departure_ts")) \
    .withColumn("planned_arrival_ts", F.to_timestamp("planned_arrival_ts")) \
    .withColumn("actual_arrival_ts", F.to_timestamp("actual_arrival_ts")) \
    .withColumn("updated_at", F.to_timestamp("updated_at"))

# Optional events
events = []
event_types = ["Booked","Loaded","Departed","Arrived","Delivered","Exception"]
for i in range(N_EVENTS):
    sid = f"S{random.randint(1, N_SHIPMENTS):09d}"
    et = random.choice(event_types)
    ets = start_dt + timedelta(days=random.randint(0, 365), hours=random.randint(0, 23), minutes=random.randint(0, 59))
    loc = f"P{random.randint(1, N_PORTS):03d}"
    notes = None if random.random() < 0.7 else f"Note {random.randint(1,9999)}"
    events.append((sid, ets.isoformat(), et, loc, notes))

events_schema = StructType([
    StructField("shipment_id", StringType(), False),
    StructField("event_ts", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_location_port", StringType(), False),
    StructField("event_notes", StringType(), True),
])
df_events = spark.createDataFrame(events, schema=events_schema) \
    .withColumn("event_ts", F.to_timestamp("event_ts"))

# Write CSVs
df_customers.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{data_path}/customers_csv")
df_ports.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{data_path}/ports_csv")
df_shipments.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{data_path}/shipments_csv")
df_events.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{data_path}/shipment_events_csv")

display(df_shipments.limit(5))
print("Data generated at:", data_path)

