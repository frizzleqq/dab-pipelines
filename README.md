# dab_pipelines

The 'dab_pipelines' is a showcase of a Spark Declarative Pipeline using Databricks Free Edition.
The data is generated using `faker`.

* `src/`: Python source code for this project.
  * `src/dab_pipelines/`: Shared Python code that can be used by jobs and pipelines.
  * `src/dab_pipelines_etl/`: Spark Declarative pipelines (aka DLT).
* `resources/`:  Resource configurations (jobs, pipelines, etc.)
* `tests/`: Unit tests for the shared Python code.
* `fixtures/`: Fixtures for data sets (primarily used for testing).


## Data Generation

We generate synthetic IoT data for machines using `faker` and write them as JSONL files to a Databricks Volume.

See [src/dab_pipelines/README_SYNTHETIC_DATA.md](src/dab_pipelines/README_SYNTHETIC_DATA.md)

## Development

### Requirements

* uv: https://docs.astral.sh/uv/getting-started/installation/
* Databricks CLI: https://docs.databricks.com/aws/en/dev-tools/cli/install

### Getting started

Sync `uv` environment:
```bash
uv sync
```

### Checks

```bash
# Linting
uv run ruff check --fix
# Formatting
uv run ruff format
# Tests
uv run pytest -v
```


# Using this project using the CLI

The Databricks workspace and IDE extensions provide a graphical interface for working
with this project. It's also possible to interact with it directly using the CLI:

1. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

2. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```

3. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```

4. To run a job or pipeline, use the "run" command:
   ```
   $ databricks bundle run
   ```
