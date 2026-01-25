"""Delta Live Table pipeline for loading machine data using autoloader."""

from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import types as T

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
                T.StructField("installation_date", T.StringType(), nullable=True),
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
                T.StructField("timestamp", T.StringType(), nullable=True),
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


def create_autoloader_table(config_key: str) -> callable:
    """Create a DLT table function for loading data via autoloader.

    Parameters
    ----------
    config_key : str
        Key in MACHINE_DATA_CONFIG to use for table configuration.

    Returns
    -------
    callable
        Function that returns a streaming DataFrame configured for autoloader.
    """
    try:
        config = MACHINE_DATA_CONFIG[config_key]
    except KeyError:
        raise ValueError(f"Invalid config_key '{config_key}'. Valid keys: {list(MACHINE_DATA_CONFIG.keys())}")

    @dp.table(name=config["table_name"], comment=config["comment"])
    def _table_function():
        catalog = spark.catalog.currentCatalog()
        base_path = f"/Volumes/{catalog}/landing/machine_uploads"

        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{base_path}/schema_evolution/{config['source_path']}")
            .option("cloudFiles.schemaHints", config["schema"].simpleString())
            .load(f"{base_path}/{config['source_path']}")
        )

    return _table_function


# Generate DLT tables from configuration
machine_dim = create_autoloader_table("machine_dim")
sensor_facts = create_autoloader_table("sensor_facts")
