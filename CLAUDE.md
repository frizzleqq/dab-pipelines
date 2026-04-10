# CLAUDE.md

This repo deploys a Databricks Asset Bundle.

## Project Structure
- `databricks.yml`: Databricks Asset Bundle configuration file
- `src/`: Python source code
  - `src/dab_pipelines/`: Shared Python code for pipelines.
    - `synthetic_data_generator.py`: Generate configurable synthetic test data
  - `src/dab_pipelines_etl/`: Spark Declarative Pipelines, organised by domain and medallion layer
- `resources/`: Resource configurations (jobs, pipelines, UC schemas, volumes, alerts)
- `tests/`: Unit tests for the shared Python code.

## Setup commands
- Install deps: `uv sync --locked`
- Run code checks: `uv run ruff check --fix`
- Check code formatting: `uv run ruff format`
- Run tests: `uv run pytest -v`
- Deploy to dev: `databricks bundle deploy`
  - For target `dev` deployed jobs are prefixed with `[dev_${workspace.current_user.short_name}]`

## Code Style
- Google Python Style Guide
- Include type hints
- Keep imports at top of the file
- Prefer: `from pyspark.sql import functions as F, types as T`

## Data Structure
- Default Catalog: `lake_dev`
- Schemas for tables: `raw`, `silver`, `gold`
  - In dev target my personal schemas are prefixed with `dev_${workspace.current_user.short_name}`

## Naming conventions
- tables: lowercase letters, numbers and underscores
  - dimension tables: prefixed with `dim_`, suffixed with `_a` (SCD1) or `_h` (SCD2)
  - fact tables: prefixed with `fact_`
- columns:
  - lowercase letters, numbers and underscores
  - surrogate keys (identity): optional, suffixed with `_sk`
