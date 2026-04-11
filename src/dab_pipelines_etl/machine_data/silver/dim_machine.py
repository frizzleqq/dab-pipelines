"""Silver machine dimension - SCD Type 1 (current state) and SCD Type 2 (history)."""

from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from dab_pipelines import df_utils
from dab_pipelines_etl.machine_data import pipeline_config as cfg


@dp.temporary_view()
def tmp_machine_dim_source():
    """Source view for machine dimension data.

    Returns
    -------
    DataFrame
        Cleaned machine dimension data ready for SCD processing.
    """
    df = spark.readStream.table(f"{cfg.raw_schema}.machine_dim")
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
# Schema is explicitly defined to declare a PRIMARY KEY constraint.
dp.create_streaming_table(
    name=f"{cfg.silver_schema}.dim_machine_h",
    schema="""
        machine_sk        BIGINT    GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key for the machine history dimension',
        machine_id        STRING    NOT NULL COMMENT 'Unique identifier of the machine',
        machine_name      STRING             COMMENT 'Human-readable name of the machine',
        machine_location  STRING             COMMENT 'Physical location of the machine',
        machine_type      STRING             COMMENT 'Category or type of the machine',
        manufacturer      STRING             COMMENT 'Manufacturer of the machine',
        installation_date TIMESTAMP          COMMENT 'Date the machine was installed',
        machine_status    STRING             COMMENT 'Operational status of the machine',
        max_temperature   DOUBLE             COMMENT 'Maximum allowed temperature threshold',
        max_pressure      DOUBLE             COMMENT 'Maximum allowed pressure threshold',
        machine_timestamp TIMESTAMP          COMMENT 'Timestamp of the last source record',
        __START_AT        TIMESTAMP          COMMENT 'SCD2 start of validity for this version',
        __END_AT          TIMESTAMP          COMMENT 'SCD2 end of validity, null if current',
        CONSTRAINT pk_dim_machine_h PRIMARY KEY (machine_sk)
    """,
    comment="Machine dimension with SCD Type 2 tracking historical changes",
    table_properties={"quality": "silver"},
    cluster_by=["machine_id"],
)

dp.create_auto_cdc_flow(
    target=f"{cfg.silver_schema}.dim_machine_h",
    source="tmp_machine_dim_source",
    keys=["machine_id"],
    sequence_by="machine_timestamp",
    stored_as_scd_type=2,
)
