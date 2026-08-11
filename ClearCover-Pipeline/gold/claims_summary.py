from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name='gold.claims_summary',
    comment='Gold: aggregated claims summary by status — count, total coverage, and average coverage.'
)
def claims_summary():
    return (
        spark.read.table('silver.claims_validated')
        .groupBy('status')
        .agg(
            F.count('claim_id').alias('claim_count'),
            F.sum('coverage_amount').alias('total_coverage'),
            F.avg('coverage_amount').alias('avg_coverage')
        )
    )
