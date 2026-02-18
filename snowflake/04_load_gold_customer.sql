CREATE OR REPLACE TABLE GOLD_CUSTOMER_MONTHLY_KPIS AS
SELECT
  $1:customer_id::STRING AS customer_id,
  $1:customer_name::STRING AS customer_name,
  $1:segment::STRING AS segment,
  $1:industry::STRING AS industry,
  TO_DATE(TO_TIMESTAMP_NTZ($1:month::STRING)) AS month,
  $1:shipments::NUMBER AS shipments,
  $1:total_spend_usd::FLOAT AS total_spend_usd,
  $1:avg_shipment_cost_usd::FLOAT AS avg_shipment_cost_usd,
  $1:on_time_rate::FLOAT AS on_time_rate,
  $1:delivered_rate::FLOAT AS delivered_rate
FROM @CASE02_CUSTOMER_STAGE
(FILE_FORMAT => 'CASE02_PARQUET');

