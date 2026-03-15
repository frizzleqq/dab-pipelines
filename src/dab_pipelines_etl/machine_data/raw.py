"""Spark Declarative Pipeline for loading machine data using autoloader."""

from typing import Callable

from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dab_pipelines import df_utils
from dab_pipelines_etl.machine_data.pipeline_config import autoloader_metadata_base, machine_uploads_base, raw_schema

# Configuration for machine data ingestion tables
MACHINE_DATA_CONFIG = {
    "machine_dim": {
        "table_name": f"{raw_schema}.machine_dim",
        "comment": "Machine dimension data loaded via autoloader from Unity Catalog Volume",
        "source_path": "machine_dim",
        "cluster_by": ["machine_id"],
        "schema": T.StructType(
            [
                T.StructField("machine_id", T.StringType(), nullable=False),
                T.StructField("machine_name", T.StringType(), nullable=True),
                T.StructField("location", T.StringType(), nullable=True),
                T.StructField("machine_type", T.StringType(), nullable=True),
                T.StructField("manufacturer", T.StringType(), nullable=True),
                T.StructField("installation_date", T.TimestampType(), nullable=True),
                T.StructField("timestamp", T.TimestampType(), nullable=True),
                T.StructField("status", T.StringType(), nullable=True),
                T.StructField("max_temperature", T.DoubleType(), nullable=True),
                T.StructField("max_pressure", T.DoubleType(), nullable=True),
            ]
        ),
    },
    "sensor_facts": {
        "table_name": f"{raw_schema}.sensor_facts",
        "comment": "Sensor facts data loaded via autoloader from Unity Catalog Volume",
        "source_path": "sensor_facts",
        "cluster_by": ["machine_id", "timestamp"],
        "schema": T.StructType(
            [
                T.StructField("reading_id", T.StringType(), nullable=False),
                T.StructField("machine_id", T.StringType(), nullable=False),
                T.StructField("timestamp", T.TimestampType(), nullable=True),
                T.StructField("temperature", T.DoubleType(), nullable=True),
                T.StructField("pressure", T.DoubleType(), nullable=True),
                T.StructField("vibration", T.DoubleType(), nullable=True),
                T.StructField("power_consumption", T.DoubleType(), nullable=True),
                T.StructField("error_code", T.StringType(), nullable=True),
                T.StructField("is_anomaly", T.BooleanType(), nullable=True),
            ]
        ),
    },
}


def create_autoloader_table(config_key: str) -> Callable:
    """Create a DLT table function for loading data via autoloader.

    Parameters
    ----------
    config_key : str
        Key in MACHINE_DATA_CONFIG to use for table configuration.

    Returns
    -------
    Callable[[dp.QueryFunction], None]
        Function that returns a streaming DataFrame configured for autoloader.
    """
    try:
        config = MACHINE_DATA_CONFIG[config_key]
    except KeyError:
        raise ValueError(f"Invalid config_key '{config_key}'. Valid keys: {list(MACHINE_DATA_CONFIG.keys())}")

    @dp.table(
        name=config["table_name"],
        comment=config["comment"],
        table_properties={"quality": "raw"},
        cluster_by=config["cluster_by"],
    )
    def _table_function():
        schema_hints = df_utils.schema_to_hints(config["schema"])

        # setup autoloader config
        df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("pathGlobFilter", "*.json")
            .option("cloudFiles.schemaLocation", f"{autoloader_metadata_base}/{config['source_path']}")
            .option("cloudFiles.schemaHints", schema_hints)
            .option("rescuedDataColumn", "_rescued")
            # Archive processed files after some time
            .option("cloudFiles.cleanSource", "MOVE")
            .option("cloudFiles.cleanSource.retentionDuration", "30 days")
            .option(
                "cloudFiles.cleanSource.moveDestination",
                f"{machine_uploads_base}/_archive/{config['source_path']}",
            )
            .load(f"{machine_uploads_base}/{config['source_path']}")
        )
        # add file metadata
        df = df.withColumns(
            {
                "_loading_ts": F.current_timestamp(),
                "_file_path": F.col("_metadata.file_path"),
                "_file_name": F.col("_metadata.file_name"),
                "_file_modification_time": F.col("_metadata.file_modification_time"),
                "_file_size": F.col("_metadata.file_size"),
            }
        )

        return df

    return _table_function


# Generate DLT tables from configuration
machine_dim = create_autoloader_table("machine_dim")
sensor_facts = create_autoloader_table("sensor_facts")
