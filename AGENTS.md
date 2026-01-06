# AGENTS.md

This repo deploys a Databricks Asset Bundle.

## Project Structure
- `databricks.yml`: Databricks Asset Bundle configuration file
* `src/`: Python source code
  * `src/dab_pipelines/`: Shared Python code for pipelines.
  * `src/dab_pipelines_etl/`: Spark Declarative Pipelines (DLT)
* `resources/`:  Resource configurations (jobs, pipelines, etc.)
* `tests/`: Unit tests for the shared Python code.

 ## Setup commands
- Install deps: `uv sync --locked --dev`
- Run code checks: `uv run ruff check`
- Check code formatting: `uv run ruff format --check`
- Run tests: `uv run pytest -v`
 
## Code Style
- Numpy docstring
- Include type hints
- Prefer: `from pyspark.sql import functions as F, types as T`