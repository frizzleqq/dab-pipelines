"""Delta Live Table pipeline for silver layer - SCD Type 2 dimension and fact tables."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from dab_pipelines import df_utils


@dp.temporary_view()
def tmp_machine_dim_source():
    """Source view for machine dimension data.

    Returns
    -------
    DataFrame
        Cleaned machine dimension data ready for SCD Type 2 processing.
    """
    df = dp.read_stream("raw.machine_dim")
    # Use source timestamp as machine_timestamp for SCD sequencing
    df = df.withColumnRenamed("timestamp", "machine_timestamp")
    df = df_utils.drop_technical_columns(df)
    df = df.withColumnRenamed("location", "machine_location")
    df = df.withColumnRenamed("status", "machine_status")
    return df


# Create SCD Type 2 dimension table using DLT's apply_changes
dp.create_streaming_table(
    name="silver.dim_machine",
    comment="Machine dimension with SCD Type 2 tracking historical changes",
    table_properties={"quality": "silver"},
)

dp.apply_changes(
    target="silver.dim_machine",
    source="tmp_machine_dim_source",
    keys=["machine_id"],
    sequence_by=F.col("machine_timestamp"),
    stored_as_scd_type="2",
    except_column_list=["machine_timestamp"],
)


@dp.table(
    name="silver.fact_sensor",
    comment="Cleaned and enriched sensor readings fact table",
    table_properties={"quality": "silver"},
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
    - Enrich with time-based attributes

    Returns
    -------
    DataFrame
        Cleaned and enriched sensor facts.
    """
    df = dp.read_stream("raw.sensor_facts")
    df = df_utils.drop_technical_columns(df)

    df = df.withColumnRenamed("timestamp", "machine_timestamp")

    # Add derived time attributes for analytics
    df = df.withColumns(
        {
            "machine_date": F.to_date(F.col("machine_timestamp")),
            "reading_hour": F.hour(F.col("machine_timestamp")),
            "reading_day_of_week": F.dayofweek(F.col("machine_timestamp")),
            # Calculate if reading is outside normal operating range
            "is_high_temperature": F.when(F.col("temperature") > 80, True).otherwise(False),
            "is_high_pressure": F.when(F.col("pressure") > 100, True).otherwise(False),
            "is_high_vibration": F.when(F.col("vibration") > 10, True).otherwise(False),
            "_loading_ts": F.current_timestamp(),
        }
    )

    return df
