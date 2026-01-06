from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.


@dp.table
def sample_zones_dab_pipelines():
    # Read from the "sample_trips" table, then sum all the fares
    return (
        spark.read.table("sample_trips_dab_pipelines")
        .groupBy(F.col("pickup_zip"))
        .agg(sum("fare_amount").alias("total_fare"))
    )
