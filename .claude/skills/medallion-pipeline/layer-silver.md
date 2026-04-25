# Silver Layer

| Purpose | SDP pattern |
|---------|-------------|
| Transform, deduplicate, and historize source data into clean dimensions and facts | Streaming tables with CDC (`dp.create_auto_cdc_flow`) for dims; `@dp.table` for facts |

## Output Structure

```
src/transformations/<domain>/
  silver/
    <dim|fact_name>.py        # SCD dimensions and fact streaming tables
```

## Step-by-Step Instructions

### 0. Gather context

Determine dimensions and facts from existing bronze files in the domain folder, the conversation, or ask the user.

### 1. SCD2 Dimension — one file per dimension

File: `silver/<dim_name>_h.py`

**SCD type → tables to create:**

| `scd_type` | Silver tables |
|------------|--------------|
| `2` | `<dim_name>_h` (`stored_as_scd_type=2`) |

Pattern: `@dp.temporary_view` for the source join → `dp.create_streaming_table` + `dp.create_auto_cdc_flow` per target table (see SDP CDC reference).

Project-specific rules:
- Built the surrogate key (`<dim_prefix>_sk`) as part of the schema definition in `create_auto_cdc_flow` with `<dim_prefix>_sk BIGINT GENERATED ALWAYS AS IDENTITY`
- `sequence_by`: use the source update timestamp; fall back to `_loading_ts` if no timestamp exists in the source
- `cluster_by` the by natural business keys and the `<dim_prefix>_sk` column for load/read performance
- Lookup tables (nation, region, etc.) join as **static** reads (`spark.read.table(...)`) inside the temp view

### 2. Facts — one file per fact

File: `silver/<fact_name>.py`

Pattern: `@dp.table` + `@dp.expect_all_or_drop` (see SDP Python syntax reference).

Project-specific rules:
- Add `@dp.expect_all_or_drop` for every grain/PK column (`IS NOT NULL`)
- Add quality checks for value ranges or domain values described in the spec
- Enrichment joins (e.g. orders for lineitem) as **static** reads inside the function
- Derived/computed columns can be added here if the logic belongs in silver rather than gold
- Always include `_loading_ts`
