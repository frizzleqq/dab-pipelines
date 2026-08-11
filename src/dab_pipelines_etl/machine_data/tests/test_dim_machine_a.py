"""Pipeline unit tests for the gold ``dim_machine_a`` table (Beta feature).

These tests are executed by the Databricks pipeline runtime, not by local
``pytest``: ``pyspark.pipelines.testing`` is only available inside the pipeline
runtime, so this module cannot be imported by ``uv run pytest`` (which is
scoped to ``tests/`` via ``[tool.pytest.ini_options] testpaths``).

Run them from the Lakeflow Pipelines Editor ("Run tests") or by starting an
update of ``pipeline_machine_data`` in test mode.

``test_pipeline.run()`` executes only the requested subset of the pipeline
graph; every upstream table it reads must be mocked first. Mocked tables are
written to a redirected test catalog, so real data is never touched.
"""

from pyspark.pipelines.testing import TestPipeline, test_spark  # noqa: F401
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

from dab_pipelines_etl.machine_data import pipeline_config as cfg

test_pipeline = TestPipeline.active()

DIM_MACHINE_H = f"{cfg.catalog}.{cfg.silver_schema}.dim_machine_h"
DIM_MACHINE_A = f"{cfg.catalog}.{cfg.gold_schema}.dim_machine_a"


def mock_dim_machine_h(session: SparkSession) -> None:
    """Replace the silver SCD Type 2 source with a small deterministic fixture.

    The fixture covers the three cases the gold table has to get right:

    - ``M-001``: superseded version plus a current version.
    - ``M-002``: a single current version.
    - ``M-003``: only a closed version, so the machine is no longer active.

    Parameters
    ----------
    session : SparkSession
        The test session provided by the ``test_spark`` fixture.
    """
    session.sql(f"""
        CREATE OR REPLACE TABLE {DIM_MACHINE_H} AS
        SELECT * FROM VALUES
            (CAST(1 AS BIGINT), 'M-001', 'Press A', 'Hall 1', 'press', 'ACME',
             TIMESTAMP'2020-01-01', 'active', CAST(120.0 AS DOUBLE), CAST(8.0 AS DOUBLE),
             TIMESTAMP'2024-01-01', TIMESTAMP'2024-01-01', TIMESTAMP'2024-06-01'),
            (CAST(2 AS BIGINT), 'M-001', 'Press A', 'Hall 2', 'press', 'ACME',
             TIMESTAMP'2020-01-01', 'maintenance', CAST(120.0 AS DOUBLE), CAST(8.0 AS DOUBLE),
             TIMESTAMP'2024-06-01', TIMESTAMP'2024-06-01', NULL),
            (CAST(3 AS BIGINT), 'M-002', 'Lathe B', 'Hall 1', 'lathe', 'Globex',
             TIMESTAMP'2021-03-15', 'active', CAST(90.0 AS DOUBLE), CAST(5.5 AS DOUBLE),
             TIMESTAMP'2024-02-01', TIMESTAMP'2024-02-01', NULL),
            (CAST(4 AS BIGINT), 'M-003', 'Drill C', 'Hall 3', 'drill', 'Initech',
             TIMESTAMP'2019-07-01', 'retired', CAST(80.0 AS DOUBLE), CAST(4.0 AS DOUBLE),
             TIMESTAMP'2023-01-01', TIMESTAMP'2023-01-01', TIMESTAMP'2023-12-31')
        AS t(machine_sk, machine_id, machine_name, machine_location, machine_type, manufacturer,
             installation_date, machine_status, max_temperature, max_pressure, machine_timestamp,
             __START_AT, __END_AT)
    """)


def test_only_active_versions_are_kept(test_spark: SparkSession) -> None:
    """Only the current SCD2 version of each machine reaches the gold table."""
    mock_dim_machine_h(test_spark)
    test_pipeline.run(test_spark, {DIM_MACHINE_A})

    result = test_spark.table(DIM_MACHINE_A).select("machine_sk", "machine_id", "machine_location", "machine_status")
    expected = test_spark.createDataFrame(
        [
            (2, "M-001", "Hall 2", "maintenance"),
            (3, "M-002", "Hall 1", "active"),
        ],
        schema="machine_sk BIGINT, machine_id STRING, machine_location STRING, machine_status STRING",
    )

    assertDataFrameEqual(result, expected)


def test_decommissioned_machine_is_excluded(test_spark: SparkSession) -> None:
    """A machine whose only version is closed does not appear at all."""
    mock_dim_machine_h(test_spark)
    test_pipeline.run(test_spark, {DIM_MACHINE_A})

    result = test_spark.table(DIM_MACHINE_A)

    assert result.filter("machine_id = 'M-003'").count() == 0
    assert result.count() == 2


def test_end_at_column_is_dropped(test_spark: SparkSession) -> None:
    """``__END_AT`` is removed, ``__START_AT`` is kept for lineage."""
    mock_dim_machine_h(test_spark)
    test_pipeline.run(test_spark, {DIM_MACHINE_A})

    columns = test_spark.table(DIM_MACHINE_A).columns

    assert "__END_AT" not in columns
    assert "__START_AT" in columns
