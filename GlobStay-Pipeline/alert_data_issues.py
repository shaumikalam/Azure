# Databricks notebook source
# DBTITLE 1,Alert: Data Quality Issues Detected
# Alert: Data Quality Issues Detected
has_issues    = dbutils.jobs.taskValues.get(taskKey="check_quality", key="has_issues",    default="false")
failed_checks = dbutils.jobs.taskValues.get(taskKey="check_quality", key="failed_checks", default="0")

print("=" * 55)
print("⚠️  DATA QUALITY ALERT — GlobStay Booking Pipeline")
print("=" * 55)
print(f"has_issues    : {has_issues}")
print(f"failed_checks : {failed_checks}")
print("Action        : Review globstay_silver_bookings before")
print("                promoting data to the Gold layer.")
print("=" * 55)

# Uncomment to hard-fail the task and trigger email notification:
# raise Exception(f"Data quality check failed ({failed_checks} issue(s) detected).")
