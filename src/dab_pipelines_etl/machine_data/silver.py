"""Spark Declarative Pipeline for silver layer - SCD dimension and fact tables."""

from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from dab_pipelines import df_utils
from dab_pipelines_etl.machine_data import pipeline_config as cfg


@dp.temporary_view()
def tmp_machine_dim_source():
    """Source view for machine dimension data.

    Returns
    -------
    DataFrame
        Cleaned machine dimension data ready for SCD Type 2 processing.
    """
    df = spark.readStream.table(f"{cfg.raw_schema}.machine_dim")
    # Use source timestamp as machine_timestamp for SCD sequencing
    df = df.withColumnRenamed("timestamp", "machine_timestamp")
    df = df_utils.drop_technical_columns(df)
    df = df.withColumnRenamed("location", "machine_location")
    df = df.withColumnRenamed("status", "machine_status")
    return df


# SCD Type 1 - current state (overwrites on change)
dp.create_streaming_table(
    name=f"{cfg.silver_schema}.dim_machine",
    comment="Machine dimension with SCD Type 1 (current state only)",
    table_properties={"quality": "silver"},
    cluster_by=["machine_id"],
)

dp.create_auto_cdc_flow(
    target=f"{cfg.silver_schema}.dim_machine",
    source="tmp_machine_dim_source",
    keys=["machine_id"],
    sequence_by="machine_timestamp",
    stored_as_scd_type=1,
)

# SCD Type 2 - full change history
dp.create_streaming_table(
    name=f"{cfg.silver_schema}.dim_machine_history",
    comment="Machine dimension with SCD Type 2 tracking historical changes",
    table_properties={"quality": "silver"},
    cluster_by=["machine_id"],
)

dp.create_auto_cdc_flow(
    target=f"{cfg.silver_schema}.dim_machine_history",
    source="tmp_machine_dim_source",
    keys=["machine_id"],
    sequence_by="machine_timestamp",
    stored_as_scd_type=2,
)


@dp.table(
    name=f"{cfg.silver_schema}.fact_sensor",
    comment="Cleaned and enriched sensor readings fact table",
    table_properties={"quality": "silver"},
    cluster_by=["machine_id", "machine_timestamp"],
)
@dp.expect_all_or_drop(
    {
        "valid_reading_id": "reading_id IS NOT NULL",
        "valid_machine_id": "machine_id IS NOT NULL",
        "valid_timestamp": "machine_timestamp IS NOT NULL",
    }
)
@dp.expect_or_drop("reasonable_temperature", "temperature BETWEEN -100 AND 500")
@dp.expect_or_drop("reasonable_pressure", "pressure >= 0")
@dp.expect_or_drop("reasonable_vibration", "vibration >= 0")
@dp.expect_or_drop("reasonable_power", "power_consumption >= 0")
def fact_sensor():
    """Create cleaned sensor facts table with data quality checks.

    Applies data quality expectations to:
    - Require valid IDs and timestamps
    - Validate sensor readings are within reasonable ranges

    Returns
    -------
    DataFrame
        Cleaned sensor facts.
    """
    df = spark.readStream.table(f"{cfg.raw_schema}.sensor_facts")
    df = df_utils.drop_technical_columns(df)

    df = df.withColumnRenamed("timestamp", "machine_timestamp")

    df = df.withColumns(
        {
            # Calculate if reading is outside normal operating range
            "is_high_temperature": F.when(F.col("temperature") > 80, True).otherwise(False),
            "is_high_pressure": F.when(F.col("pressure") > 100, True).otherwise(False),
            "is_high_vibration": F.when(F.col("vibration") > 10, True).otherwise(False),
            "_loading_ts": F.current_timestamp(),
        }
    )

    return df
