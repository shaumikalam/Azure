# Databricks notebook source
# DBTITLE 1,Silver Layer: Cleanse & Transform
# Silver Layer: Cleanse & Transform
from pyspark.sql.functions import col, trim, upper, to_date, row_number
from pyspark.sql.window import Window

try:
    layer = dbutils.widgets.get("layer")
except Exception:
    layer = "silver"
print(f"▶ Layer: {layer}")

catalog = "hospitality_lab"
src = f"{catalog}.bronze.globstay_bronze_bookings"
tgt = f"{catalog}.silver.globstay_silver_bookings"

df = spark.table(src)

# 1. Drop rows missing critical fields
df = df.dropna(subset=["booking_id", "guest_name", "hotel"])

# 2. Standardise text columns
df = (df
    .withColumn("status",  trim(upper(col("status"))))
    .withColumn("country", trim(upper(col("country"))))
    .withColumn("hotel",   trim(col("hotel"))))

# 3. Cast date strings to DateType
df = (df
    .withColumn("checkin_date",  to_date("checkin_date",  "yyyy-MM-dd"))
    .withColumn("checkout_date", to_date("checkout_date", "yyyy-MM-dd")))

# 4. Deduplicate — keep first occurrence per booking_id
w  = Window.partitionBy("booking_id").orderBy("ingested_at")
df = df.withColumn("rn", row_number().over(w)).filter("rn = 1").drop("rn")

# 5. Drop records where checkout ≤ checkin
df = df.filter(col("checkout_date") > col("checkin_date"))

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tgt)
print(f"✅ Silver cleanse complete — {spark.table(tgt).count()} rows → {tgt}")
