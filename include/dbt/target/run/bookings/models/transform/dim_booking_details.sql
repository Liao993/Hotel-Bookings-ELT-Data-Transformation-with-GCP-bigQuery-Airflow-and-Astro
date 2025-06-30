
  
    

    create or replace table `hotel-bookings-de`.`bookings`.`dim_booking_details`
      
    
    

    OPTIONS()
    as (
      -- dim_booking_details.sql

WITH booking_details_cte AS (
  SELECT DISTINCT
    COALESCE(market_segment, 'Unknown') AS market_segment_cleaned,
    COALESCE(distribution_channel, 'Unknown') AS distribution_channel_cleaned,
    COALESCE(meal, 'Unknown') AS meal_cleaned,
    COALESCE(deposit_type, 'Unknown') AS deposit_type_cleaned,
    COALESCE(reservation_status, 'Unknown') AS reservation_status_cleaned
  FROM `hotel-bookings-de`.`bookings`.`raw_bookings`
),
booking_detail_keys AS (
  SELECT
    to_hex(md5(cast(coalesce(cast(market_segment_cleaned as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(distribution_channel_cleaned as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(meal_cleaned as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(deposit_type_cleaned as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(reservation_status_cleaned as string), '_dbt_utils_surrogate_key_null_') as string))) AS booking_detail_id,
    market_segment_cleaned AS market_segment,
    distribution_channel_cleaned AS distribution_channel,
    meal_cleaned AS meal,
    deposit_type_cleaned AS deposit_type,
    reservation_status_cleaned AS reservation_status
  FROM booking_details_cte
)

SELECT
  booking_detail_id,
  market_segment,
  distribution_channel,
  meal,
  deposit_type,
  reservation_status
FROM booking_detail_keys
    );
  