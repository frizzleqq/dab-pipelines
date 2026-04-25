"""Autoloader factory for creating bronze streaming ingest tables."""

from typing import Callable

from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from dab_pipelines import df_utils
from dab_pipelines_etl.machine_data import pipeline_config as cfg


def create_autoloader_table(config: dict) -> Callable:
    """Create a DLT table function for loading data via autoloader.

    Parameters
    ----------
    config : dict
        Table configuration with keys: table_name, comment, source_path,
        cluster_by, schema.

    Returns
    -------
    Callable
        Function that returns a streaming DataFrame configured for autoloader.
    """

    @dp.table(
        name=config["table_name"],
        comment=config["comment"],
        table_properties={"quality": "bronze"},
        cluster_by=config["cluster_by"],
    )
    def _table_function():
        schema_hints = df_utils.schema_to_hints(config["schema"])

        df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("pathGlobFilter", "*.json")
            .option("cloudFiles.schemaLocation", f"{cfg.autoloader_metadata_base}/{config['source_path']}")
            .option("cloudFiles.schemaHints", schema_hints)
            .option("rescuedDataColumn", "_rescued")
            .option("cloudFiles.cleanSource", "MOVE")
            .option("cloudFiles.cleanSource.retentionDuration", "30 days")
            .option(
                "cloudFiles.cleanSource.moveDestination",
                f"{cfg.machine_uploads_base}/_archive/{config['source_path']}",
            )
            .load(f"{cfg.machine_uploads_base}/{config['source_path']}")
        )

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
