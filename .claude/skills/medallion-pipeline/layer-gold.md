# Gold Layer

| Purpose | SDP pattern |
|---------|-------------|
| Presentation layer — apply business logic, expose a clean star schema to consumers | Views for dimensions; materialized views for facts |

## Output Structure

```
src/transformations/<domain>/
  gold/
    <dim_name>_a.sql          # SCD1 dimension view  (_a = current state)
    <dim_name>_h.sql          # SCD2 dimension view  (_h = full history, optional)
    <fact_name>.sql           # fact views or materialized views
```

## Step-by-Step Instructions

### 0. Gather context

Drive the gold schema from existing silver files in the domain folder and the conversation. Column comments and business logic must then be inferred from context or confirmed with the user.

### 1. SCD2 Dimension views (SQL)

Gold SCD2 dimensions are **plain views** (`CREATE OR REPLACE VIEW`) — silver already persists the data.

Project-specific rules:
- Name of view should be `gold`.<dim_name>_h`
- Surrogate key `<dim_prefix>_sk` is pre-computed in silver
- gold dimension views can't have primary keys
- Rename `__START_AT` → `valid_from_ts`, `__END_AT` → `valid_to_ts` for `_h` views
- Add `(__END_AT IS NULL) AS is_current` for `_h` views
- Add column `COMMENT` for every column, take from gathered information
  - Note that a View must not specify the data type even if a comment is added

### 2. SCD1 Dimension Materialized Views (Python or SQL)

Gold SCD1 dimensions are **materialized views** using the silver SCD2 dimension as a source.

Project-specific rules:
- Name of table should be `gold`.<dim_name>_a`
- Use the silver SCD2 dimension as source and filter on current entries
  - `__END_AT IS NULL"`

### 3. Fact views and materialized views (Python or SQL)

Choose the pattern based on complexity:
- **Plain view** (`CREATE OR REPLACE VIEW`): for lightweight passthrough from silver, simple column derivations. No persistence cost.
- **Materialized view** (`CREATE OR REFRESH MATERIALIZED VIEW`): when aggregation, multi-table joins, or expensive business logic is required.

Project-specific rules:
- Resolve SCD2 surrogate keys at event time by joining the gold view:
    ```sql
    JOIN ${silver_schema}.<dim_name>_h AS d
      ON  f.<source_key> = d.<business_key_bk>
      AND f.<event_timestamp> >= d.__START_AT
      AND (d.__END_AT IS NULL OR f.event_timestamp < d.__END_AT)
    ```
- Apply `business_logic` expression
- Add column `COMMENT` for every column, take from description in the spec if available or from gathered information
  - Note that a Materialized View must specify the data type for every column if a comment is added
