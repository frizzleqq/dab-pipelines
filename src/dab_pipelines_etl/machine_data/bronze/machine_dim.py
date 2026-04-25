"""Bronze machine metadata table loaded via autoloader."""

from pyspark.sql import types as T

from dab_pipelines_etl.machine_data import pipeline_config as cfg
from dab_pipelines_etl.machine_data.bronze._autoloader import create_autoloader_table

machine_metadata = create_autoloader_table(
    {
        "table_name": f"{cfg.bronze_schema}.machine_metadata",
        "comment": "Machine metadata loaded via autoloader from Unity Catalog Volume",
        "source_path": "machine_metadata",
        "cluster_by": ["_loading_ts", "machine_id"],
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
    }
)
