"""Gold daily aggregated sensor metrics per machine - materialized view."""

from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from dab_pipelines_etl.machine_data import pipeline_config as cfg


@dp.table(
    name=f"{cfg.gold_schema}.fact_sensor_agg",
    schema=f"""
        machine_date            DATE    NOT NULL COMMENT 'Date of the aggregation period',
        machine_id              STRING  NOT NULL COMMENT 'Unique identifier of the machine',
        machine_sk              BIGINT           COMMENT 'Surrogate key from the machine daily dimension',
        machine_name            STRING           COMMENT 'Human-readable name of the machine',
        machine_location        STRING           COMMENT 'Physical location of the machine',
        machine_type            STRING           COMMENT 'Category or type of the machine',
        machine_status          STRING           COMMENT 'Operational status of the machine',
        total_readings          BIGINT           COMMENT 'Total number of sensor readings recorded',
        anomaly_count           BIGINT           COMMENT 'Number of readings flagged as anomalies',
        high_temp_count         BIGINT           COMMENT 'Number of readings with high temperature',
        high_pressure_count     BIGINT           COMMENT 'Number of readings with high pressure',
        high_vibration_count    BIGINT           COMMENT 'Number of readings with high vibration',
        avg_temperature         DOUBLE           COMMENT 'Average temperature over the period',
        min_temperature         DOUBLE           COMMENT 'Minimum temperature recorded',
        max_temperature         DOUBLE           COMMENT 'Maximum temperature recorded',
        avg_pressure            DOUBLE           COMMENT 'Average pressure over the period',
        min_pressure            DOUBLE           COMMENT 'Minimum pressure recorded',
        max_pressure            DOUBLE           COMMENT 'Maximum pressure recorded',
        avg_vibration           DOUBLE           COMMENT 'Average vibration over the period',
        min_vibration           DOUBLE           COMMENT 'Minimum vibration recorded',
        max_vibration           DOUBLE           COMMENT 'Maximum vibration recorded',
        avg_power_consumption   DOUBLE           COMMENT 'Average power consumption over the period',
        min_power_consumption   DOUBLE           COMMENT 'Minimum power consumption recorded',
        max_power_consumption   DOUBLE           COMMENT 'Maximum power consumption recorded',
        total_power_consumption DOUBLE           COMMENT 'Total power consumed over the period',
        CONSTRAINT pk_fact_sensor_agg PRIMARY KEY (machine_date, machine_id),
        CONSTRAINT fk_fact_sensor_agg_dim_machine_daily FOREIGN KEY (machine_id, machine_date) REFERENCES {cfg.gold_schema}.dim_machine_daily(machine_id, machine_date)
    """,
    comment="Daily aggregated sensor metrics per machine",
    table_properties={"quality": "gold"},
    cluster_by=["machine_date", "machine_id"],
)
def fact_sensor_agg():
    """Daily aggregation of sensor readings joined with machine dimension.

    Aggregates silver sensor facts by machine and date, joined with the
    gold machine daily dimension for enriched machine attributes.

    Returns
    -------
    DataFrame
        One row per (machine_id, machine_date) with aggregated sensor metrics.
    """
    f = spark.read.table(f"{cfg.silver_schema}.fact_sensor")
    m = spark.read.table(f"{cfg.gold_schema}.dim_machine_daily")

    f = f.withColumn("machine_date", F.col("machine_timestamp").cast("date"))

    return (
        f.join(m, on=["machine_id", "machine_date"], how="left")
        .groupBy(
            "machine_date",
            "machine_id",
            m.machine_sk,
            m.machine_name,
            m.machine_location,
            m.machine_type,
            m.machine_status,
        )
        .agg(
            F.count("*").alias("total_readings"),
            F.sum(F.col("is_anomaly").cast("int")).alias("anomaly_count"),
            F.sum(F.col("is_high_temperature").cast("int")).alias("high_temp_count"),
            F.sum(F.col("is_high_pressure").cast("int")).alias("high_pressure_count"),
            F.sum(F.col("is_high_vibration").cast("int")).alias("high_vibration_count"),
            F.round(F.avg("temperature"), 2).alias("avg_temperature"),
            F.round(F.min("temperature"), 2).alias("min_temperature"),
            F.round(F.max("temperature"), 2).alias("max_temperature"),
            F.round(F.avg("pressure"), 2).alias("avg_pressure"),
            F.round(F.min("pressure"), 2).alias("min_pressure"),
            F.round(F.max("pressure"), 2).alias("max_pressure"),
            F.round(F.avg("vibration"), 2).alias("avg_vibration"),
            F.round(F.min("vibration"), 2).alias("min_vibration"),
            F.round(F.max("vibration"), 2).alias("max_vibration"),
            F.round(F.avg("power_consumption"), 2).alias("avg_power_consumption"),
            F.round(F.min("power_consumption"), 2).alias("min_power_consumption"),
            F.round(F.max("power_consumption"), 2).alias("max_power_consumption"),
            F.round(F.sum("power_consumption"), 2).alias("total_power_consumption"),
        )
    )
