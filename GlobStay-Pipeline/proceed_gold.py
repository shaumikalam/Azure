# Databricks notebook source
# DBTITLE 1,Proceed: Quality Confirmed — Finalise Gold
# Proceed: Quality Confirmed — Stamp Gold Table as Approved
from pyspark.sql.functions import current_timestamp, lit

catalog = "hospitality_lab"
tgt = f"{catalog}.gold.globstay_gold_booking_summary"

print("✅ All quality gates passed — finalising Gold layer.")

df = (spark.table(tgt)
    .withColumn("quality_checked_at", current_timestamp())
    .withColumn("pipeline_status",    lit("APPROVED")))

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tgt)

print(f"✅ Gold table finalised — {spark.table(tgt).count()} rows in {tgt}")
df.show(5, truncate=False)
