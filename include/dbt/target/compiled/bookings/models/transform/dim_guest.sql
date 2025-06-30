WITH guest_cte AS (
  SELECT DISTINCT
    to_hex(md5(cast(coalesce(cast(COALESCE(country, 'Unknown') as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(COALESCE(CAST(is_repeated_guest AS STRING), 'Unknown') as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(COALESCE(customer_type, 'Unknown') as string), '_dbt_utils_surrogate_key_null_') as string))) AS guest_id,
    COALESCE(country, 'Unknown') AS country,
    is_repeated_guest,
    COALESCE(customer_type, 'Unknown') AS customer_type
  FROM `hotel-bookings-de`.`bookings`.`raw_bookings`
)
SELECT
  g.guest_id,
  COALESCE(c.nicename, 'Unknown') AS country,
  g.is_repeated_guest,
  g.customer_type
FROM guest_cte g
LEFT JOIN `hotel-bookings-de`.`bookings`.`country` c
  ON g.country = c.iso3