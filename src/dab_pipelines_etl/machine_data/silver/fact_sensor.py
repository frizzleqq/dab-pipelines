"""Silver sensor facts table with data quality checks."""

from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from dab_pipelines import df_utils
from dab_pipelines_etl.machine_data import pipeline_config as cfg


@dp.temporary_view()
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
def tmp_fact_sensor_source():
    """Source view for sensor fact data with data quality checks applied.

    Returns
    -------
    DataFrame
        Cleaned sensor facts ready for ingestion into the silver table.
    """
    df = spark.readStream.table(f"{cfg.raw_schema}.sensor_facts")
    df = df_utils.drop_technical_columns(df)

    df = df.withColumnRenamed("timestamp", "machine_timestamp")

    df = df.withColumns(
        {
            "is_high_temperature": F.when(F.col("temperature") > 80, True).otherwise(False),
            "is_high_pressure": F.when(F.col("pressure") > 100, True).otherwise(False),
            "is_high_vibration": F.when(F.col("vibration") > 10, True).otherwise(False),
            "_loading_ts": F.current_timestamp(),
        }
    )

    return df


dp.create_streaming_table(
    name=f"{cfg.silver_schema}.fact_sensor",
    schema=f"""
        reading_id           STRING    NOT NULL,
        machine_id           STRING    NOT NULL,
        machine_timestamp    TIMESTAMP NOT NULL,
        temperature          DOUBLE,
        pressure             DOUBLE,
        vibration            DOUBLE,
        power_consumption    DOUBLE,
        error_code           STRING,
        is_anomaly           BOOLEAN,
        is_high_temperature  BOOLEAN,
        is_high_pressure     BOOLEAN,
        is_high_vibration    BOOLEAN,
        _loading_ts          TIMESTAMP,
        CONSTRAINT pk_fact_sensor PRIMARY KEY (reading_id),
        CONSTRAINT fk_fact_sensor_machine_id FOREIGN KEY (machine_id) REFERENCES {cfg.silver_schema}.dim_machine(machine_id)
    """,
    comment="Cleaned and enriched sensor readings fact table",
    table_properties={"quality": "silver"},
    cluster_by=["machine_id", "machine_timestamp"],
)

dp.create_auto_cdc_flow(
    target=f"{cfg.silver_schema}.fact_sensor",
    source="tmp_fact_sensor_source",
    keys=["reading_id"],
    sequence_by="machine_timestamp",
    stored_as_scd_type=1,
)
