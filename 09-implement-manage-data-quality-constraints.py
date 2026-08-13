# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,claims_validated — data quality constraints
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.expect_or_drop("drop null claim_id", "claim_id IS NOT NULL")
@dp.expect_or_drop("drop null customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid claim_date", "claim_date IS NOT NULL")
@dp.expect("warn invalid status", "status IN ('OPEN', 'PENDING', 'CLOSED')")
@dp.expect_or_fail("fail nonpositive coverage_amount", "coverage_amount > 0")
@dp.table(name='silver.claims_validated')
def claims_validated():
    '''Silver: validated insurance claims with quality constraints applied.'''

    df = spark.readStream.table('bronze.claims_raw_autoloader')
    df = df.withColumns({
        'claim_date': F.col('claim_date').cast('date'),
        'coverage_amount': F.col('coverage_amount').cast('decimal(12,2)')
    })
    return df

# COMMAND ----------

# DBTITLE 1,Verification queries
# MAGIC %sql
# MAGIC -- How many claims made it through all validations?
# MAGIC SELECT COUNT(*) AS valid_claim_count
# MAGIC FROM insurance_lab.silver.claims_validated;
# MAGIC
# MAGIC -- What types and statuses appear in the validated silver layer?
# MAGIC SELECT claim_type, status, COUNT(*) AS count
# MAGIC FROM insurance_lab.silver.claims_validated
# MAGIC GROUP BY claim_type, status
# MAGIC ORDER BY claim_type, status;
# MAGIC
# MAGIC -- Review the gold summary
# MAGIC SELECT *
# MAGIC FROM insurance_lab.gold.claims_summary
# MAGIC ORDER BY claim_type, status;
# MAGIC
# MAGIC -- Did Auto Loader capture any rescued data?
# MAGIC SELECT claim_id, _rescued_data
# MAGIC FROM insurance_lab.silver.claims_rescued
# MAGIC WHERE _rescued_data IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP CATALOG IF EXISTS insurance_lab CASCADE;
