# Bronze Layer

| Purpose | SDP pattern |
|---------|-------------|
| Mirror source data unchanged — append-only, no transforms | Streaming table via `spark.readStream.table()` or Auto Loader (`cloudFiles`) |

## Output Structure

```
src/transformations/<domain>/
  pipeline_config.py          # schema config read from Spark conf
  bronze/
    <source_table>.py         # one file per source table
```

## Step-by-Step Instructions

### 0. Gather context

Determine the source catalog, schema, and tables from the conversation or ask the user.

### 1. One file per source table

Choose the ingestion pattern based on where the source data lives:

**Pattern A — Source Delta table** (Unity Catalog table as source):
Use `spark.readStream.table("<source_catalog>.<source_schema>.<table>")`.
- Table name: `{cfg.bronze_schema}.<source_schema>_<table>` (schema prefix avoids collisions)
- Add `_loading_ts` column via `.withColumn("_loading_ts", F.current_ts())`
- `cluster_by` using `_loading_ts` and the most selective / natural key column
- No transformations — preserve source columns exactly

**Pattern B — Auto Loader from Volume** (files dropped into a UC Volume):
Use `spark.readStream.format("cloudFiles")` pointed at the volume path.
- Set `cloudFiles.format` to match the file format (e.g. `json`, `csv`, `parquet`)
- Set `cloudFiles.schemaLocation` to a dedicated metadata volume path
- Use `cloudFiles.schemaHints` to enforce known column types
- Set `rescuedDataColumn` to `_rescued` to capture schema mismatches
- Add `_loading_ts`, `_file_path`, `_file_name`, `_file_modification_time` from `_metadata`
- Add `pipeline_config.py` keys for the uploads volume path and the metadata volume path
