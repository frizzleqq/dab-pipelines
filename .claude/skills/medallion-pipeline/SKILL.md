---
name: medallion-pipeline
description: "Implements a medallion pipeline (bronze → silver → gold) for a Databricks Asset Bundle. Use when building a Spark Declarative Pipelines for a domain. Triggered by phrases like 'build medallion pipeline', 'review medallion pipeline code'."
---

# Medallion Pipeline

Implement a medallion pipeline (bronze → silver → gold) for a domain.

---

## Reference Guides

### Layers

| Layer | Description | Reference |
|-------|-------------|-----------|
| **Bronze** | Ingest source tables unchanged — append-only, no transforms | See [layer-bronze.md](layer-bronze.md) |
| **Silver** | CDC-based SCD dimensions and streaming fact tables | See [layer-silver.md](layer-silver.md) |
| **Gold** | Dimension views, fact materialized views | See [layer-gold.md](layer-gold.md) |

Execute each layer in order (bronze → silver → gold). Read the full instructions for each layer before writing code for it.

## Guidelines

### Pipeline building guidelines

Before generating any code, check:
  - `resources/*.yml` for existing bundle resources
  - Any existing pipeline under `src/transformations/` or `resources/pipeline.*.yml` as a style reference
  - Use SDP instead of DLT: `from pyspark import pipelines as dp`

### You must understand source data before implementing new pipeline tables

- Consider reading sample data and table-/column-comments of source-data and existing upstream bronze-/silver-tables before implementing new tables.

### Pipeline Setup

- The pipeline should use a resource `resources/pipeline.<domain>.yml` in the databricks bundle:

```yaml
# Pipeline for loading <domain> data

resources:
  pipelines:
    pipeline_<domain>:
      name: pipeline_<domain>
      catalog: ${var.catalog}
      schema: ${var.schema}
      serverless: true
      root_path: "../src/"

      libraries:
        - glob:
            include: ../src/transformations/<domain>/**

      configuration:
        bronze_schema: ${resources.schemas.schema_bronze.name}
        silver_schema: ${resources.schemas.schema_silver.name}
        gold_schema: ${resources.schemas.schema_gold.name}

      environment:
        dependencies:
          - --editable ${workspace.file_path}
```

- The pipeline should use a `src\transformations\<domain>\pipeline_config.py` module to access pipeline configuration values:
  - all pipeline tables and views must use the config values for schema names (e.g. `cfg.bronze_schema`) rather than hardcoding schema names

```python
"""Pipeline configuration values sourced from Databricks pipeline settings.
- import the module as `from transformations.tpch import pipeline_config as cfg`
"""

from databricks.sdk.runtime import spark

bronze_schema = spark.conf.get("bronze_schema")
silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")
# Add further keys as needed (e.g. volume paths for Auto Loader pipelines)
```

### Verify pipeline

After all layers and the pipeline resource are written:

1. Confirm `resources/pipeline.<domain>.yml` exists and its `libraries` glob matches the actual source path.
2. Dry run the pipeline to ensure it executes without errors.
3. Run the pipeline in a dev environment and perform data plausibility checks on the implemented transformations (e.g. record counts, null counts, value distributions) to confirm the logic is correct.

## Related Skills

- **[databricks-spark-declarative-pipelines](../databricks-spark-declarative-pipelines/SKILL.md)** — SDP syntax, CDC/SCD patterns, streaming vs materialized views
- **[databricks-bundles](../databricks-bundles/SKILL.md)** — DAB resource YAML and multi-environment deployment
- **[databricks-unity-catalog](../databricks-unity-catalog/SKILL.md)** — catalog/schema/volume management
