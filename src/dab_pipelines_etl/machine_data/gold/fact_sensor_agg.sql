-- Daily aggregations of sensor readings per machine
CREATE OR REFRESH MATERIALIZED VIEW ${gold_schema}.fact_sensor_agg (
  machine_date            DATE    NOT NULL,
  machine_id              STRING  NOT NULL,
  machine_name            STRING,
  machine_location        STRING,
  machine_type            STRING,
  machine_status          STRING,
  total_readings          BIGINT,
  anomaly_count           BIGINT,
  high_temp_count         BIGINT,
  high_pressure_count     BIGINT,
  high_vibration_count    BIGINT,
  avg_temperature         DOUBLE,
  min_temperature         DOUBLE,
  max_temperature         DOUBLE,
  avg_pressure            DOUBLE,
  min_pressure            DOUBLE,
  max_pressure            DOUBLE,
  avg_vibration           DOUBLE,
  min_vibration           DOUBLE,
  max_vibration           DOUBLE,
  avg_power_consumption   DOUBLE,
  min_power_consumption   DOUBLE,
  max_power_consumption   DOUBLE,
  total_power_consumption DOUBLE,
  CONSTRAINT pk_fact_sensor_agg PRIMARY KEY (machine_date, machine_id),
  CONSTRAINT fk_fact_sensor_agg_dim_machine_daily FOREIGN KEY (machine_id, machine_date) REFERENCES ${gold_schema}.dim_machine_daily(machine_id, machine_date)
)
CLUSTER BY (machine_id, machine_date)
COMMENT "Daily aggregated sensor metrics per machine"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  CAST(f.machine_timestamp AS DATE) AS machine_date,
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
FROM ${silver_schema}.fact_sensor         AS f
LEFT JOIN ${gold_schema}.dim_machine_daily AS m
  ON  f.machine_id                       = m.machine_id
  AND CAST(f.machine_timestamp AS DATE)  = m.machine_date
GROUP BY
  CAST(f.machine_timestamp AS DATE),
  f.machine_id,
  m.machine_name,
  m.machine_location,
  m.machine_type,
  m.machine_status;
