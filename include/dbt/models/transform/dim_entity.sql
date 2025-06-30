-- dim_entity.sql

WITH prepped_data AS (
  SELECT
    COALESCE(CAST(company AS STRING), 'Unknown') AS company_cleaned,
    COALESCE(CAST(agent AS STRING), 'Unknown') AS agent_cleaned
  FROM {{ source('bookings', 'raw_bookings') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key([
      "company_cleaned",
      "agent_cleaned"
    ]) }} AS entity_id,
  company_cleaned AS company, 
  agent_cleaned AS agent     
FROM prepped_data