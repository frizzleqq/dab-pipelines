-- Current machine attributes (view over silver SCD1)
CREATE OR REPLACE VIEW ${gold_schema}.dim_machine
COMMENT "Current machine dimension - latest attributes for every machine"
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
  machine_timestamp
FROM ${silver_schema}.dim_machine;
