"""Raw sensor facts table loaded via autoloader."""

from pyspark.sql import types as T

from dab_pipelines_etl.machine_data import pipeline_config as cfg
from dab_pipelines_etl.machine_data.raw._autoloader import create_autoloader_table

sensor_facts = create_autoloader_table(
    {
        "table_name": f"{cfg.raw_schema}.sensor_facts",
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
    }
)
