# Databricks notebook source
# DBTITLE 1,Data Quality Checks
# Data Quality Checks — emits task values consumed by quality_gate tasks
from pyspark.sql.functions import col, isnull

catalog = "hospitality_lab"
table   = f"{catalog}.silver.globstay_silver_bookings"
df      = spark.table(table)
total   = df.count()

# 1. Null key fields
null_count  = df.filter(isnull("booking_id") | isnull("checkin_date") | isnull("total_amount")).count()

# 2. Invalid amounts
bad_amounts = df.filter(col("total_amount") <= 0).count()

# 3. Bad date order
bad_dates   = df.filter(col("checkout_date") <= col("checkin_date")).count()

# 4. Unrecognised status
bad_status  = df.filter(~col("status").isin("CONFIRMED", "PENDING", "CANCELLED")).count()

failed_checks = sum([
    1 if null_count  > 0 else 0,
    1 if bad_amounts > 0 else 0,
    1 if bad_dates   > 0 else 0,
    1 if bad_status  > 0 else 0,
])
has_issues = "true" if failed_checks > 0 else "false"

print(f"Total rows          : {total}")
print(f"Null key fields     : {null_count}")
print(f"Invalid amounts     : {bad_amounts}")
print(f"Bad date ranges     : {bad_dates}")
print(f"Unrecognised status : {bad_status}")
print(f"Failed checks       : {failed_checks}  |  has_issues: {has_issues}")

# Emit values consumed by quality_gate condition tasks
dbutils.jobs.taskValues.set("has_issues",    has_issues)
dbutils.jobs.taskValues.set("failed_checks", str(failed_checks))
