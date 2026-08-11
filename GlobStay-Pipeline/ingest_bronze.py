# Databricks notebook source
# DBTITLE 1,Bronze Layer: Raw Booking Data Ingestion
# Bronze Layer: Raw Booking Data Ingestion
import random
from datetime import datetime, timedelta
from pyspark.sql import Row

# --- Config ---
catalog = "hospitality_lab"
schema  = "bronze"
table   = f"{catalog}.{schema}.globstay_bronze_bookings"

# --- Synthetic GlobStay raw data ---
random.seed(42)
hotels    = ["Grand Azure", "Sunrise Inn", "Harbor View", "City Loft", "Mountain Escape"]
statuses  = ["CONFIRMED", "CONFIRMED", "CONFIRMED", "PENDING", "CANCELLED"]
countries = ["US", "UK", "DE", "FR", "JP", "AU"]

def rnd_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

start, end = datetime(2024, 1, 1), datetime(2024, 12, 31)

rows = []
for i in range(1, 201):
    checkin = rnd_date(start, end)
    rows.append(Row(
        booking_id    = f"BKG-{i:04d}",
        guest_name    = f"Guest {i}" if i % 15 != 0 else None,  # ~7% nulls
        hotel         = random.choice(hotels),
        country       = random.choice(countries),
        checkin_date  = str(checkin.date()),
        checkout_date = str((checkin + timedelta(days=random.randint(1, 14))).date()),
        status        = random.choice(statuses),
        total_amount  = round(random.uniform(50, 2000), 2),
        ingested_at   = datetime.now().isoformat()
    ))

rows += rows[:5]  # inject 5 duplicate rows

df = spark.createDataFrame(rows)
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)

print(f"✅ Bronze ingestion complete — {spark.table(table).count()} rows → {table}")
