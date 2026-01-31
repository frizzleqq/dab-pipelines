"""Delta Live Table pipeline for silver layer - SCD Type 2 dimension and fact tables."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dab_pipelines import df_utils


@dp.table(
    name="silver.machine_dim_scd2",
    comment="Machine dimension with SCD Type 2 tracking historical changes",
    table_properties={"quality": "silver", "pipelines.autoOptimize.zOrderCols": "machine_id,is_current"},
)
@dp.expect_all_or_drop(
    {
        "valid_machine_id": "machine_id IS NOT NULL",
        "valid_effective_from": "effective_from IS NOT NULL",
    }
)
def machine_dim_scd2():
    """Create SCD Type 2 dimension table for machines.

    Tracks historical changes to machine attributes with:
    - effective_from: When this version became active
    - effective_to: When this version was superseded (NULL for current)
    - is_current: Boolean flag for current version
    - version: Sequential version number per machine

    Returns
    -------
    DataFrame
        Machine dimension with SCD Type 2 tracking.
    """
    # Read from raw table
    df = dp.read_stream("raw.machine_dim")
    df = df_utils.drop_technical_columns(df)

    # Add SCD Type 2 tracking columns
    df = (
        df.withColumn("effective_from", F.col("_file_modification_time"))
        .withColumn("effective_to", F.lit(None).cast(T.TimestampType()))
        .withColumn("is_current", F.lit(True))
        .withColumn(
            "version",
            F.row_number().over(F.window.partitionBy("machine_id").orderBy(F.col("_file_modification_time"))),
        )
        .withColumn("_loading_ts", F.current_timestamp())
    )

    return df


@dp.table(
    name="silver.sensor_facts",
    comment="Cleaned and enriched sensor readings fact table",
    table_properties={"quality": "silver", "pipelines.autoOptimize.zOrderCols": "machine_id,timestamp"},
)
@dp.expect_all_or_drop(
    {
        "valid_reading_id": "reading_id IS NOT NULL",
        "valid_machine_id": "machine_id IS NOT NULL",
        "valid_timestamp": "timestamp IS NOT NULL",
    }
)
@dp.expect_or_drop("reasonable_temperature", "temperature BETWEEN -100 AND 500")
@dp.expect_or_drop("reasonable_pressure", "pressure >= 0")
@dp.expect_or_drop("reasonable_vibration", "vibration >= 0")
@dp.expect_or_drop("reasonable_power", "power_consumption >= 0")
def sensor_facts():
    """Create cleaned sensor facts table with data quality checks.

    Applies data quality expectations to:
    - Require valid IDs and timestamps
    - Validate sensor readings are within reasonable ranges
    - Enrich with time-based attributes

    Returns
    -------
    DataFrame
        Cleaned and enriched sensor facts.
    """
    df = dp.read_stream("raw.sensor_facts")
    df = df_utils.drop_underscore_columns(df)

    # Add derived time attributes for analytics
    df = df.withColumns(
        {
            "reading_date": F.to_date(F.col("timestamp")),
            "reading_hour": F.hour(F.col("timestamp")),
            "reading_day_of_week": F.dayofweek(F.col("timestamp")),
            # Calculate if reading is outside normal operating range
            "is_high_temperature": F.when(F.col("temperature") > 80, True).otherwise(False),
            "is_high_pressure": F.when(F.col("pressure") > 100, True).otherwise(False),
            "is_high_vibration": F.when(F.col("vibration") > 10, True).otherwise(False),
            "_loading_ts": F.current_timestamp(),
        }
    )

    return df
