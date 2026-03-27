-- Silver sensor readings with machine_date column
CREATE OR REPLACE VIEW ${gold_schema}.fact_sensor
COMMENT "Sensor readings from silver with machine_date derived from timestamp"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  reading_id,
  machine_id,
  machine_timestamp,
  CAST(machine_timestamp AS DATE) AS machine_date,
  HOUR(machine_timestamp)         AS reading_hour,
  DAYOFWEEK(machine_timestamp)    AS reading_day_of_week,
  temperature,
  pressure,
  vibration,
  power_consumption,
  error_code,
  is_anomaly,
  is_high_temperature,
  is_high_pressure,
  is_high_vibration
FROM ${silver_schema}.fact_sensor;
