-- Full history of machine dimension changes (SCD Type 2)
CREATE OR REPLACE VIEW ${gold_schema}.dim_machine_h (
  machine_sk        COMMENT "Surrogate key for the machine history dimension",
  machine_id        COMMENT "Unique identifier of the machine",
  machine_name      COMMENT "Human-readable name of the machine",
  machine_location  COMMENT "Physical location of the machine",
  machine_type      COMMENT "Category or type of the machine",
  manufacturer      COMMENT "Manufacturer of the machine",
  installation_date COMMENT "Date the machine was installed",
  machine_status    COMMENT "Operational status of the machine",
  max_temperature   COMMENT "Maximum allowed temperature threshold",
  max_pressure      COMMENT "Maximum allowed pressure threshold",
  machine_timestamp COMMENT "Timestamp of the last source record",
  valid_from_ts     COMMENT "SCD2 start of validity for this version",
  valid_to_ts       COMMENT "SCD2 end of validity, null if current",
  is_current        COMMENT "True if this is the currently active version"
)
COMMENT "Full history of machine dimension changes (SCD Type 2), sourced from silver"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  machine_sk,
  machine_id,
  machine_name,
  machine_location,
  machine_type,
  manufacturer,
  installation_date,
  machine_status,
  max_temperature,
  max_pressure,
  machine_timestamp,
  __START_AT               AS valid_from_ts,
  __END_AT                 AS valid_to_ts,
  (__END_AT IS NULL)       AS is_current
FROM ${silver_schema}.dim_machine_h;
