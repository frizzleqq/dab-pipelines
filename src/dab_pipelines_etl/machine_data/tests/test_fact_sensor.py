"""Pipeline unit tests for the silver ``fact_sensor`` table (Beta feature).

Runs only inside the Databricks pipeline runtime (Lakeflow Pipelines Editor >
"Run tests"), not under local pytest. See "Pipeline unit tests" in README.md.
"""

from pyspark.pipelines.testing import TestPipeline, test_spark  # noqa: F401
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

from dab_pipelines_etl.machine_data import pipeline_config as cfg

test_pipeline = TestPipeline.active()

SENSOR_DATA = f"{cfg.catalog}.{cfg.bronze_schema}.sensor_data"
FACT_SENSOR = f"{cfg.catalog}.{cfg.silver_schema}.fact_sensor"


def mock_sensor_data(session: SparkSession) -> None:
    """Replace the bronze source with rows covering the quality expectations.

    - ``R-001``: valid reading with all values in the normal range.
    - ``R-002``: valid reading above every high-reading threshold.
    - ``R-003`` to ``R-005``: each violates one expectation (missing machine_id,
      temperature out of range, negative pressure) and must be dropped.

    Parameters
    ----------
    session : SparkSession
        The test session provided by the ``test_spark`` fixture.
    """
    session.sql(f"""
        CREATE OR REPLACE TABLE {SENSOR_DATA} AS
        SELECT * FROM VALUES
            ('R-001', 'M-001', TIMESTAMP'2024-05-01 10:00:00', CAST(75.0 AS DOUBLE),
             CAST(50.0 AS DOUBLE), CAST(2.0 AS DOUBLE), CAST(10.0 AS DOUBLE), NULL, false, '/lz/f1.json'),
            ('R-002', 'M-001', TIMESTAMP'2024-05-01 11:00:00', CAST(95.0 AS DOUBLE),
             CAST(150.0 AS DOUBLE), CAST(12.0 AS DOUBLE), CAST(20.0 AS DOUBLE), 'E42', true, '/lz/f1.json'),
            ('R-003', NULL, TIMESTAMP'2024-05-01 12:00:00', CAST(70.0 AS DOUBLE),
             CAST(50.0 AS DOUBLE), CAST(2.0 AS DOUBLE), CAST(10.0 AS DOUBLE), NULL, false, '/lz/f2.json'),
            ('R-004', 'M-002', TIMESTAMP'2024-05-01 13:00:00', CAST(600.0 AS DOUBLE),
             CAST(50.0 AS DOUBLE), CAST(2.0 AS DOUBLE), CAST(10.0 AS DOUBLE), NULL, false, '/lz/f2.json'),
            ('R-005', 'M-002', TIMESTAMP'2024-05-01 14:00:00', CAST(70.0 AS DOUBLE),
             CAST(-5.0 AS DOUBLE), CAST(2.0 AS DOUBLE), CAST(10.0 AS DOUBLE), NULL, false, '/lz/f2.json')
        AS t(reading_id, machine_id, timestamp, temperature, pressure, vibration,
             power_consumption, error_code, is_anomaly, _file_path)
    """)


def test_invalid_rows_are_dropped(test_spark: SparkSession) -> None:
    """Rows violating a quality expectation do not reach the silver table."""
    mock_sensor_data(test_spark)
    test_pipeline.run(test_spark, {FACT_SENSOR})

    result = test_spark.table(FACT_SENSOR).select("reading_id", "machine_id")
    expected = test_spark.createDataFrame(
        [
            ("R-001", "M-001"),
            ("R-002", "M-001"),
        ],
        schema="reading_id STRING, machine_id STRING",
    )

    assertDataFrameEqual(result, expected)


def test_high_reading_flags(test_spark: SparkSession) -> None:
    """Threshold flags are computed per reading."""
    mock_sensor_data(test_spark)
    test_pipeline.run(test_spark, {FACT_SENSOR})

    result = test_spark.table(FACT_SENSOR).select(
        "reading_id", "is_high_temperature", "is_high_pressure", "is_high_vibration"
    )
    expected = test_spark.createDataFrame(
        [
            ("R-001", False, False, False),
            ("R-002", True, True, True),
        ],
        schema="reading_id STRING, is_high_temperature BOOLEAN, is_high_pressure BOOLEAN, is_high_vibration BOOLEAN",
    )

    assertDataFrameEqual(result, expected)


def test_columns(test_spark: SparkSession) -> None:
    """``timestamp`` is renamed, bronze technical columns are replaced by ``_loading_ts``."""
    mock_sensor_data(test_spark)
    test_pipeline.run(test_spark, {FACT_SENSOR})

    columns = test_spark.table(FACT_SENSOR).columns

    assert "machine_timestamp" in columns
    assert "timestamp" not in columns
    assert "_file_path" not in columns
    assert "_loading_ts" in columns
