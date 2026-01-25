"""Delta Live Table pipeline for loading machine data using autoloader."""

from databricks.sdk.runtime import spark
from pyspark import pipelines as dp


@dp.table(name="raw.machine_dim", comment="Machine dimension data loaded via autoloader from Unity Catalog Volume")
def machine_dim():
    """Load machine dimension data from landing zone using autoloader.

    Returns:
        DataFrame: Machine dimension data with all columns from source JSON files.
    """
    catalog = spark.conf.get("catalog")
    current_catalog = spark.catalog.currentCatalog()
    assert catalog == current_catalog, f"Expected catalog {catalog}, but got {current_catalog}"

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"/Volumes/{catalog}/landing/machine_uploads/machine_dim")
    )


@dp.table(name="raw.sensor_facts", comment="Sensor facts data loaded via autoloader from Unity Catalog Volume")
def sensor_facts():
    """Load sensor facts data from landing zone using autoloader.

    Returns:
        DataFrame: Sensor facts data with all columns from source JSON files.
    """
    catalog = spark.conf.get("catalog")
    current_catalog = spark.catalog.currentCatalog()
    assert catalog == current_catalog, f"Expected catalog {catalog}, but got {current_catalog}"

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"/Volumes/{catalog}/landing/machine_uploads/sensor_facts")
    )
