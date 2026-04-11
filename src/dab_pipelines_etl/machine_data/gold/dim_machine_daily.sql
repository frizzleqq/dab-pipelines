-- One row per machine per calendar day (2020-01-01 → today)
-- Uses the SCD2 validity window to resolve which version was active on each day.
-- Schema is explicitly defined to declare a PRIMARY KEY constraint.
CREATE OR REFRESH MATERIALIZED VIEW ${gold_schema}.dim_machine_daily (
  machine_sk        BIGINT    NOT NULL COMMENT "Surrogate key from the SCD2 history table",
  machine_id        STRING    NOT NULL COMMENT "Unique identifier of the machine",
  machine_date      DATE      NOT NULL COMMENT "Calendar date this row represents",
  machine_name      STRING             COMMENT "Human-readable name of the machine",
  machine_location  STRING             COMMENT "Physical location of the machine",
  machine_type      STRING             COMMENT "Category or type of the machine",
  manufacturer      STRING             COMMENT "Manufacturer of the machine",
  installation_date TIMESTAMP          COMMENT "Date the machine was installed",
  machine_status    STRING             COMMENT "Operational status of the machine",
  max_temperature   DOUBLE             COMMENT "Maximum allowed temperature threshold",
  max_pressure      DOUBLE             COMMENT "Maximum allowed pressure threshold",
  valid_from        TIMESTAMP NOT NULL COMMENT "SCD2 start of validity for this version",
  valid_to          TIMESTAMP          COMMENT "SCD2 end of validity, null if current",
  CONSTRAINT pk_dim_machine_daily PRIMARY KEY (machine_id, machine_date)
)
CLUSTER BY (machine_id, machine_date)
COMMENT "One row per machine per calendar day from 2020-01-01 to current date, showing the machine attributes that were valid on that day according to the SCD2 history"
TBLPROPERTIES ("quality" = "gold")
AS
WITH date_spine AS (
  SELECT EXPLODE(SEQUENCE(DATE '2020-01-01', CURRENT_DATE(), INTERVAL 1 DAY)) AS machine_date
),
ranked AS (
  SELECT
    d.machine_date,
    m.machine_sk,
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
  JOIN ${silver_schema}.dim_machine_h AS m
    ON CAST(m.__START_AT AS DATE) <= d.machine_date
   AND (m.__END_AT IS NULL OR CAST(m.__END_AT AS DATE) > d.machine_date)
)
SELECT
  machine_sk,
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
