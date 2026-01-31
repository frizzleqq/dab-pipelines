"""Delta Live Table pipeline for loading machine data using autoloader."""

from typing import Callable

from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dab_pipelines_etl.machine_data import df_utils

# Configuration for machine data ingestion tables
MACHINE_DATA_CONFIG = {
    "machine_dim": {
        "table_name": "raw.machine_dim",
        "comment": "Machine dimension data loaded via autoloader from Unity Catalog Volume",
        "source_path": "machine_dim",
        "schema": T.StructType(
            [
                T.StructField("machine_id", T.StringType(), nullable=False),
                T.StructField("machine_name", T.StringType(), nullable=True),
                T.StructField("location", T.StringType(), nullable=True),
                T.StructField("machine_type", T.StringType(), nullable=True),
                T.StructField("manufacturer", T.StringType(), nullable=True),
                T.StructField("installation_date", T.TimestampType(), nullable=True),
                T.StructField("status", T.StringType(), nullable=True),
                T.StructField("max_temperature", T.DoubleType(), nullable=True),
                T.StructField("max_pressure", T.DoubleType(), nullable=True),
            ]
        ),
    },
    "sensor_facts": {
        "table_name": "raw.sensor_facts",
        "comment": "Sensor facts data loaded via autoloader from Unity Catalog Volume",
        "source_path": "sensor_facts",
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


def create_autoloader_table(config_key: str) -> Callable[[dp.QueryFunction], None]:
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

    @dp.table(name=config["table_name"], comment=config["comment"], table_properties={"quality": "raw"})
    def _table_function():
        # Get catalog from pipeline configuration
        catalog = spark.conf.get("volume_catalog")
        base_path = f"/Volumes/{catalog}/landing/machine_uploads"
        schema_hints = df_utils.schema_to_hints(config["schema"])

        # setup autoloader config
        df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("pathGlobFilter", "*.json")
            .option("cloudFiles.schemaLocation", f"{base_path}/_schema_evolution/{config['source_path']}")
            .option("cloudFiles.schemaHints", schema_hints)
            .option("rescuedDataColumn", "_rescued")
            # Archive processed files after some time
            .option("cloudFiles.cleanSource", "MOVE")
            .option("cloudFiles.cleanSource.retentionDuration", "30 days")
            .option("cloudFiles.cleanSource.moveDestination", f"{base_path}/_archive/{config['source_path']}")
            .load(f"{base_path}/{config['source_path']}")
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
