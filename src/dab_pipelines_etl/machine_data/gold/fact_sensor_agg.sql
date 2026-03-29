-- Daily aggregations of sensor readings per machine
-- Schema is explicitly defined to declare PK and FK constraints.
-- Note: specifying a column list locks schema evolution — new columns in the
-- SELECT require a matching update here.
CREATE OR REFRESH MATERIALIZED VIEW ${gold_schema}.fact_sensor_agg (
  machine_date            DATE    NOT NULL COMMENT "Date of the aggregation period",
  machine_id              STRING  NOT NULL COMMENT "Unique identifier of the machine",
  machine_name            STRING           COMMENT "Human-readable name of the machine",
  machine_location        STRING           COMMENT "Physical location of the machine",
  machine_type            STRING           COMMENT "Category or type of the machine",
  machine_status          STRING           COMMENT "Operational status of the machine",
  total_readings          BIGINT           COMMENT "Total number of sensor readings recorded",
  anomaly_count           BIGINT           COMMENT "Number of readings flagged as anomalies",
  high_temp_count         BIGINT           COMMENT "Number of readings with high temperature",
  high_pressure_count     BIGINT           COMMENT "Number of readings with high pressure",
  high_vibration_count    BIGINT           COMMENT "Number of readings with high vibration",
  avg_temperature         DOUBLE           COMMENT "Average temperature over the period",
  min_temperature         DOUBLE           COMMENT "Minimum temperature recorded",
  max_temperature         DOUBLE           COMMENT "Maximum temperature recorded",
  avg_pressure            DOUBLE           COMMENT "Average pressure over the period",
  min_pressure            DOUBLE           COMMENT "Minimum pressure recorded",
  max_pressure            DOUBLE           COMMENT "Maximum pressure recorded",
  avg_vibration           DOUBLE           COMMENT "Average vibration over the period",
  min_vibration           DOUBLE           COMMENT "Minimum vibration recorded",
  max_vibration           DOUBLE           COMMENT "Maximum vibration recorded",
  avg_power_consumption   DOUBLE           COMMENT "Average power consumption over the period",
  min_power_consumption   DOUBLE           COMMENT "Minimum power consumption recorded",
  max_power_consumption   DOUBLE           COMMENT "Maximum power consumption recorded",
  total_power_consumption DOUBLE           COMMENT "Total power consumed over the period",
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
