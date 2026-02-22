-- Gold layer materialized views for machine data analytics.
-- Builds on the silver SCD Type 2 dimension and fact tables.

-- -----------------------------------------------------------------------------
-- dim_machine_daily: One row per machine per calendar day (2020-01-01 → today)
-- Uses the SCD2 validity window to resolve which version was active on each day.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold.dim_machine_daily
COMMENT "One row per machine per calendar day from 2020-01-01 to current date, showing the machine attributes that were valid on that day according to the SCD2 history"
TBLPROPERTIES ("quality" = "gold")
AS
WITH date_spine AS (
  SELECT EXPLODE(SEQUENCE(DATE '2020-01-01', CURRENT_DATE(), INTERVAL 1 DAY)) AS machine_date
),
ranked AS (
  SELECT
    d.machine_date,
    m.machine_id,
    m.machine_name,
    m.machine_location,
    m.machine_type,
    m.manufacturer,
    m.installation_date,
    m.machine_status,
    m.max_temperature,
    m.max_pressure,
    m.__START_AT  AS valid_from,
    m.__END_AT    AS valid_to,
    ROW_NUMBER() OVER (
      PARTITION BY m.machine_id, d.machine_date
      ORDER BY m.__START_AT DESC
    ) AS _rn
  FROM date_spine AS d
  JOIN silver.dim_machine AS m
    ON CAST(m.__START_AT AS DATE) <= d.machine_date
   AND (m.__END_AT IS NULL OR CAST(m.__END_AT AS DATE) > d.machine_date)
)
SELECT
  machine_id,
  machine_date,
  machine_name,
  machine_location,
  machine_type,
  manufacturer,
  installation_date,
  machine_status,
  max_temperature,
  max_pressure,
  valid_from,
  valid_to
FROM ranked
WHERE _rn = 1;


-- -----------------------------------------------------------------------------
-- dim_machine_curr: Active machine dimension records (open-ended SCD2 rows)
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold.dim_machine_curr
COMMENT "Current active machine dimension - latest attributes for every machine"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  machine_id,
  machine_name,
  machine_location,
  machine_type,
  manufacturer,
  installation_date,
  machine_status,
  max_temperature,
  max_pressure,
  __START_AT AS valid_from
FROM silver.dim_machine
WHERE __END_AT IS NULL;


-- -----------------------------------------------------------------------------
-- fact_sensor: Silver sensor readings with machine_date column
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold.fact_sensor
COMMENT "Sensor readings from silver with machine_date derived from timestamp"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  reading_id,
  machine_id,
  machine_timestamp,
  machine_date,
  reading_hour,
  reading_day_of_week,
  temperature,
  pressure,
  vibration,
  power_consumption,
  error_code,
  is_anomaly,
  is_high_temperature,
  is_high_pressure,
  is_high_vibration
FROM silver.fact_sensor;


-- -----------------------------------------------------------------------------
-- fact_sensor_agg: Daily aggregations of sensor readings per machine
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold.fact_sensor_agg
COMMENT "Daily aggregated sensor metrics per machine"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  f.machine_date,
  f.machine_id,
  m.machine_name,
  m.machine_location,
  m.machine_type,
  m.machine_status,
  -- Reading counts
  COUNT(*)                                      AS total_readings,
  SUM(CAST(f.is_anomaly AS INT))                AS anomaly_count,
  SUM(CAST(f.is_high_temperature AS INT))       AS high_temp_count,
  SUM(CAST(f.is_high_pressure    AS INT))       AS high_pressure_count,
  SUM(CAST(f.is_high_vibration   AS INT))       AS high_vibration_count,
  -- Temperature stats
  ROUND(AVG(f.temperature), 2)                  AS avg_temperature,
  ROUND(MIN(f.temperature), 2)                  AS min_temperature,
  ROUND(MAX(f.temperature), 2)                  AS max_temperature,
  -- Pressure stats
  ROUND(AVG(f.pressure), 2)                     AS avg_pressure,
  ROUND(MIN(f.pressure), 2)                     AS min_pressure,
  ROUND(MAX(f.pressure), 2)                     AS max_pressure,
  -- Vibration stats
  ROUND(AVG(f.vibration), 2)                    AS avg_vibration,
  ROUND(MIN(f.vibration), 2)                    AS min_vibration,
  ROUND(MAX(f.vibration), 2)                    AS max_vibration,
  -- Power consumption stats
  ROUND(AVG(f.power_consumption), 2)            AS avg_power_consumption,
  ROUND(MIN(f.power_consumption), 2)            AS min_power_consumption,
  ROUND(MAX(f.power_consumption), 2)            AS max_power_consumption,
  ROUND(SUM(f.power_consumption), 2)            AS total_power_consumption
FROM silver.fact_sensor         AS f
LEFT JOIN gold.dim_machine_daily AS m
  ON  f.machine_id   = m.machine_id
  AND f.machine_date = m.machine_date
GROUP BY
  f.machine_date,
  f.machine_id,
  m.machine_name,
  m.machine_location,
  m.machine_type,
  m.machine_status;
