"""Gold machine dimension active materialized view."""

from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from dab_pipelines_etl.machine_data import pipeline_config as cfg


@dp.table(
    name=f"{cfg.gold_schema}.dim_machine_a",
    schema="""
        machine_sk        BIGINT    COMMENT 'Surrogate key for the machine history dimension',
        machine_id        STRING    NOT NULL COMMENT 'Unique identifier of the machine',
        machine_name      STRING    COMMENT 'Human-readable name of the machine',
        machine_location  STRING    COMMENT 'Physical location of the machine',
        machine_type      STRING    COMMENT 'Category or type of the machine',
        manufacturer      STRING    COMMENT 'Manufacturer of the machine',
        installation_date TIMESTAMP COMMENT 'Date the machine was installed',
        machine_status    STRING    COMMENT 'Operational status of the machine',
        max_temperature   DOUBLE    COMMENT 'Maximum allowed temperature threshold',
        max_pressure      DOUBLE    COMMENT 'Maximum allowed pressure threshold',
        machine_timestamp TIMESTAMP COMMENT 'Timestamp of the last source record',
        __START_AT        TIMESTAMP COMMENT 'SCD2 validity start for the current version',
        CONSTRAINT pk_gold_dim_machine_a PRIMARY KEY (machine_id)
    """,
    comment="Current state of each machine — active SCD Type 2 record per machine_id",
    table_properties={"quality": "gold"},
    cluster_by=["machine_id"],
)
def dim_machine_a():
    """Active machine dimension — one row per machine showing the latest state.

    Filters the silver SCD Type 2 table to only the currently active records
    (where __END_AT is null).

    Returns
    -------
    DataFrame
        One row per machine_id representing its current state.
    """
    return spark.read.table(f"{cfg.silver_schema}.dim_machine_h").filter("__END_AT IS NULL").drop("__END_AT")
