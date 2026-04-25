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
- Schemas for tables: `bronze`, `silver`, `gold`
  - In dev target my personal schemas are prefixed with `dev_${workspace.current_user.short_name}`

  ## Naming Conventions

- Dimension tables: `dim_` prefix, `_a` suffix (SCD1/current) or `_h` suffix (SCD2/history)
- Fact tables: `fact_` prefix
- Surrogate keys: `_sk` suffix
