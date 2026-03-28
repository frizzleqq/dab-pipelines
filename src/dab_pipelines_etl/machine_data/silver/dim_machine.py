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
# Schema is explicitly defined to declare a PRIMARY KEY constraint.
# Note: specifying schema= locks schema evolution — new source columns won't
# propagate automatically and require a manual schema update here.
dp.create_streaming_table(
    name=f"{cfg.silver_schema}.dim_machine",
    schema="""
        machine_id        STRING    NOT NULL,
        machine_name      STRING,
        machine_location  STRING,
        machine_type      STRING,
        manufacturer      STRING,
        installation_date TIMESTAMP,
        machine_status    STRING,
        max_temperature   DOUBLE,
        max_pressure      DOUBLE,
        machine_timestamp TIMESTAMP,
        CONSTRAINT pk_dim_machine PRIMARY KEY (machine_id)
    """,
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
# Schema is explicitly defined to declare a composite PRIMARY KEY on
# (machine_id, __START_AT) — the SCD2-generated validity start column.
dp.create_streaming_table(
    name=f"{cfg.silver_schema}.dim_machine_history",
    schema="""
        machine_id        STRING    NOT NULL,
        machine_name      STRING,
        machine_location  STRING,
        machine_type      STRING,
        manufacturer      STRING,
        installation_date TIMESTAMP,
        machine_status    STRING,
        max_temperature   DOUBLE,
        max_pressure      DOUBLE,
        machine_timestamp TIMESTAMP,
        __START_AT        TIMESTAMP NOT NULL,
        __END_AT          TIMESTAMP,
        CONSTRAINT pk_dim_machine_history PRIMARY KEY (machine_id, __START_AT)
    """,
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
