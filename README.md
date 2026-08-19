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

Besides the local tests in `tests/`, the pipeline transformations are tested with the
Databricks *pipeline unit testing* feature (Beta): tests mock the pipeline's source
tables, run a subset of the pipeline graph, and assert on the resulting tables — so the
actual `@dp.table` definitions are exercised, expectations and declared schemas included.
Mocked tables are written to a redirected test catalog, so real data is never touched.
See [`src/dab_pipelines_etl/machine_data/tests/`](src/dab_pipelines_etl/machine_data/tests/)
for examples.

Wiring (see [`resources/pipeline.machine_data.yml`](resources/pipeline.machine_data.yml)):

* Test files live under the pipeline's `root_path` but are **excluded from the source
  globs** — the runtime rejects test files as pipeline source.
* The pipeline runs on the **PREVIEW channel**, required by the Beta.
* The catalog is exposed as a `configuration` value (read in `pipeline_config.py`)
  because tests reference tables by fully qualified name.

Run them from the Lakeflow Pipelines Editor ("Run tests") — the only supported way;
there is no CLI/API trigger. Local `uv run pytest` skips them (`testpaths = ["tests"]`
in `pyproject.toml`), since `pyspark.pipelines.testing` only exists in the pipeline runtime.


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
