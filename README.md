# dab_pipelines

The 'dab_pipelines' is a showcase of a Spark Declarative Pipeline using Databricks Free Edition.
The data is generated using `faker`.

* `src/`: Python source code for this project.
  * `src/dab_pipelines/`: Shared Python code that can be used by jobs and pipelines.
  * `src/dab_pipelines_etl/`: Spark Declarative pipelines organised by domain and medallion layer.
* `resources/`: Resource configurations (jobs, pipelines, UC schemas, volumes, alerts)
* `tests/`: Unit tests for the shared Python code.
* `src/dab_pipelines_etl/<domain>/tests/`: Pipeline unit tests, run by the pipeline runtime (see below).


## Data Generation

We generate synthetic IoT data for machines using `faker` and write them as JSONL files to a Databricks Volume.

See [src/dab_pipelines/README_SYNTHETIC_DATA.md](src/dab_pipelines/README_SYNTHETIC_DATA.md)

## Databricks Workspace

For this example we use a Databricks Free Edition workspace https://www.databricks.com/learn/free-edition with all resources and identities managed in the Workspace.

This Databricks Asset Bundle expects pre-existing Catalogs, Groups and Service Principals to showcase providing permissions on resources such as catalogs or workflows.

* **Catalogs**: `lake_dev`, `lake_test` and `lake_prod`
* **Service principals**: `sp_etl_dev` (for dev and test) and `sp_etl_prod` (for prod)
  * Make sure the User used to deploy Workflows has `Service principal: User` on the used service principals
* **Groups**: `group_etl` and `group_reader`
  * These are only used to showcase applying grants using Asset Bundle resources

## Unity Catalog Resources

Schemas and volumes are managed by the bundle — see [`resources/schemas.yml`](resources/schemas.yml) and [`resources/volumes.yml`](resources/volumes.yml) for definitions and grants. In `dev`, names are prefixed with `dev_<username>_` for per-user isolation; in `test`, names are prefixed with `test_`; names are fixed in `prod`.

## Development

### Requirements

* uv: https://docs.astral.sh/uv/getting-started/installation/
* Databricks CLI: https://docs.databricks.com/aws/en/dev-tools/cli/install
* (Optional) Databricks AI skills for your coding agent (installable via Databricks CLI):
  ```bash
  databricks aitools install
  ```

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

### Pipeline unit tests (Beta)

Besides the local tests in `tests/`, the pipeline transformations themselves are tested
with the Databricks *pipeline unit testing* feature (Beta). These tests mock a pipeline's
source tables, run a subset of the pipeline graph against the mocks, and assert on the
resulting tables — so the actual `@dp.table` definitions are exercised, decorators,
expectations and declared schemas included.

Example: [`src/dab_pipelines_etl/machine_data/tests/test_dim_machine_a.py`](src/dab_pipelines_etl/machine_data/tests/test_dim_machine_a.py)
covers the gold `dim_machine_a` table, which filters the silver SCD Type 2 history down
to the currently active version of each machine.

```python
from pyspark.pipelines.testing import TestPipeline, test_spark

test_pipeline = TestPipeline.active()


def test_only_active_versions_are_kept(test_spark):
    mock_dim_machine_h(test_spark)                 # CREATE OR REPLACE TABLE <catalog>.<silver>.dim_machine_h ...
    test_pipeline.run(test_spark, {DIM_MACHINE_A}) # run only this table
    ...
```

How this is wired up:

* Test files live **inside the pipeline's source glob** (`libraries.glob` in
  [`resources/pipeline.machine_data.yml`](resources/pipeline.machine_data.yml)) so they are
  deployed with the pipeline. The runtime picks them up as tests — not as dataset
  definitions — via pytest naming conventions (`test_*.py`).
* Tables are referenced by their **fully qualified** name. The pipeline's catalog is
  therefore exposed to pipeline code as a `configuration` value and read in
  `pipeline_config.py`, next to the existing schema names.
* Mocked tables are written to a redirected test catalog, so running the tests never
  touches real data.

Running them:

* From the Lakeflow Pipelines Editor in the workspace ("Run tests").
* They are **not** run by `uv run pytest`: `pyspark.pipelines.testing` ships only in the
  Databricks pipeline runtime, not in `databricks-connect`. Local pytest is scoped to
  `tests/` via `testpaths` in `pyproject.toml`, so it skips them instead of failing on the
  import.

> The feature is in Beta and must be enabled for the workspace; the API may still change.


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
