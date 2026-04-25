-- Silver sensor readings with machine_date column and machine_sk from SCD2 history
CREATE OR REPLACE VIEW ${gold_schema}.fact_sensor (
  reading_id          COMMENT "Unique identifier of the sensor reading",
  machine_id          COMMENT "Unique identifier of the machine",
  machine_sk          COMMENT "Surrogate key of the machine version active at reading time",
  machine_timestamp   COMMENT "Timestamp of the sensor reading",
  machine_date        COMMENT "Calendar date derived from the reading timestamp",
  reading_hour        COMMENT "Hour of day the reading was taken (0–23)",
  reading_day_of_week COMMENT "Day of week the reading was taken (1=Sunday, 7=Saturday)",
  temperature         COMMENT "Temperature reading in degrees",
  pressure            COMMENT "Pressure reading",
  vibration           COMMENT "Vibration reading",
  power_consumption   COMMENT "Power consumption reading",
  error_code          COMMENT "Error code reported by the machine, if any",
  is_anomaly          COMMENT "Whether the reading was flagged as an anomaly",
  is_high_temperature COMMENT "Whether the temperature exceeded the high threshold",
  is_high_pressure    COMMENT "Whether the pressure exceeded the high threshold",
  is_high_vibration   COMMENT "Whether the vibration exceeded the high threshold"
)
COMMENT "Sensor readings from silver with machine_date derived from timestamp"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  f.reading_id,
  f.machine_id,
  m.machine_sk,
  f.machine_timestamp,
  CAST(f.machine_timestamp AS DATE) AS machine_date,
  HOUR(f.machine_timestamp)         AS reading_hour,
  DAYOFWEEK(f.machine_timestamp)    AS reading_day_of_week,
  f.temperature,
  f.pressure,
  f.vibration,
  f.power_consumption,
  f.error_code,
  f.is_anomaly,
  f.is_high_temperature,
  f.is_high_pressure,
  f.is_high_vibration
FROM ${silver_schema}.fact_sensor AS f
LEFT JOIN ${silver_schema}.dim_machine_h AS m
  ON  f.machine_id        = m.machine_id
  AND f.machine_timestamp >= m.__START_AT
  AND (m.__END_AT IS NULL OR f.machine_timestamp < m.__END_AT);
