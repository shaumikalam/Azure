from pyspark import pipelines as dp


@dp.table(
    name='bronze.claims_raw_autoloader',
    comment='Bronze: raw insurance claims ingested from CSV landing zone via Auto Loader.'
)
def claims_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("header", "true")
        .load("/Volumes/insurance_lab/bronze/raw_files/")
    )
