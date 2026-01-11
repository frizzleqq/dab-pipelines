# AGENTS.md

This repo deploys a Databricks Asset Bundle.

## Project Structure
- `databricks.yml`: Databricks Asset Bundle configuration file
* `src/`: Python source code
  * `src/dab_pipelines/`: Shared Python code for pipelines.
    * `synthetic_data_generator.py`: Generate configurable synthetic test data
  * `src/dab_pipelines_etl/`: Spark Declarative Pipelines (DLT)
* `resources/`:  Resource configurations (jobs, pipelines, etc.)
* `tests/`: Unit tests for the shared Python code.

 ## Setup commands
- Install deps: `uv sync --locked`
- Run code checks: `uv run ruff check --fix`
- Check code formatting: `uv run ruff format`
- Run tests: `uv run pytest -v`
- To run Python code prefix with: `uv run ...`
 
## Code Style
- Numpy docstring
- Google Python Style Guide
- Include type hints
- Prefer: `from pyspark.sql import functions as F, types as T`