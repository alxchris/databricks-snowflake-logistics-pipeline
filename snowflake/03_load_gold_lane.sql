CREATE OR REPLACE TABLE GOLD_LANE_KPIS AS
SELECT
  $1:origin_port::STRING AS origin_port,
  $1:origin_region::STRING AS origin_region,
  $1:dest_port::STRING AS dest_port,
  $1:dest_region::STRING AS dest_region,
  $1:service_level::STRING AS service_level,
  $1:shipments::NUMBER AS shipments,
  $1:avg_transit_hours::FLOAT AS avg_transit_hours,
  $1:on_time_rate::FLOAT AS on_time_rate,
  $1:total_spend_usd::FLOAT AS total_spend_usd
FROM @CASE02_LANE_STAGE
(FILE_FORMAT => 'CASE02_PARQUET');

