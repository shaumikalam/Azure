# Databricks notebook source
# DBTITLE 1,Gold Layer: Business Aggregations
# Gold Layer: Business Aggregations
from pyspark.sql.functions import count, countDistinct, sum, avg, round as spark_round

try:
    layer = dbutils.widgets.get("layer")
except Exception:
    layer = "gold"
print(f"▶ Layer: {layer}")

catalog = "hospitality_lab"
src = f"{catalog}.silver.globstay_silver_bookings"
tgt = f"{catalog}.gold.globstay_gold_booking_summary"

df = spark.table(src)

df_gold = (df
    .groupBy("hotel", "status", "country")
    .agg(
        count("booking_id")                  .alias("total_bookings"),
        countDistinct("guest_name")           .alias("unique_guests"),
        spark_round(sum("total_amount"), 2)   .alias("total_revenue"),
        spark_round(avg("total_amount"), 2)   .alias("avg_booking_value")
    )
    .orderBy("hotel", "status"))

df_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tgt)
print(f"✅ Gold aggregation complete — {spark.table(tgt).count()} rows → {tgt}")
df_gold.show(20, truncate=False)
