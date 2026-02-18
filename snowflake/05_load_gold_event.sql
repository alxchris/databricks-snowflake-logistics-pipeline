CREATE OR REPLACE TABLE GOLD_LATEST_SHIPMENT_EVENT_STATUS AS
SELECT
  $1:shipment_id::STRING AS shipment_id,
  TRY_TO_TIMESTAMP_NTZ($1:latest_event_ts::STRING) AS latest_event_ts,
  $1:latest_event_type::STRING AS latest_event_type,
  $1:latest_event_location_port::STRING AS latest_event_location_port,
  $1:latest_event_notes::STRING AS latest_event_notes
FROM @CASE02_EVENT_STAGE
(FILE_FORMAT => 'CASE02_PARQUET');

