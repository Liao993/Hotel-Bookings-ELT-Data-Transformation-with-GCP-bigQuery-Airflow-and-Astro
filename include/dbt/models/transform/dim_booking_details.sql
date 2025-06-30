-- dim_booking_details.sql

WITH booking_details_cte AS (
  SELECT DISTINCT
    COALESCE(market_segment, 'Unknown') AS market_segment_cleaned,
    COALESCE(distribution_channel, 'Unknown') AS distribution_channel_cleaned,
    COALESCE(meal, 'Unknown') AS meal_cleaned,
    COALESCE(deposit_type, 'Unknown') AS deposit_type_cleaned,
    COALESCE(reservation_status, 'Unknown') AS reservation_status_cleaned
  FROM {{ source('bookings', 'raw_bookings') }}
),
booking_detail_keys AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key([
      'market_segment_cleaned',
      'distribution_channel_cleaned',
      'meal_cleaned',
      'deposit_type_cleaned',
      'reservation_status_cleaned'
    ]) }} AS booking_detail_id,
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
