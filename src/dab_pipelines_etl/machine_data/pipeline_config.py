"""Pipeline configuration values sourced from Databricks pipeline settings."""

from databricks.sdk.runtime import spark

bronze_schema = spark.conf.get("bronze_schema")
silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")
machine_uploads_base = spark.conf.get("machine_uploads_base")
autoloader_metadata_base = spark.conf.get("autoloader_metadata_base")
