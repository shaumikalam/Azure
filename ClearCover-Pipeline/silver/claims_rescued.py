from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name='silver.claims_rescued',
    comment='Silver: rows rescued from bronze where Auto Loader could not parse fields into the inferred schema.'
)
def claims_rescued():
    return (
        spark.readStream.table('bronze.claims_raw_autoloader')
        .filter(F.col('_rescued_data').isNotNull())
    )
