
  
    

    create or replace table `hotel-bookings-de`.`bookings`.`dim_entity`
      
    
    

    OPTIONS()
    as (
      -- dim_entity.sql

WITH prepped_data AS (
  SELECT
    COALESCE(CAST(company AS STRING), 'Unknown') AS company_cleaned,
    COALESCE(CAST(agent AS STRING), 'Unknown') AS agent_cleaned
  FROM `hotel-bookings-de`.`bookings`.`raw_bookings`
)
SELECT
  to_hex(md5(cast(coalesce(cast(company_cleaned as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(agent_cleaned as string), '_dbt_utils_surrogate_key_null_') as string))) AS entity_id,
  company_cleaned AS company, 
  agent_cleaned AS agent     
FROM prepped_data
    );
  