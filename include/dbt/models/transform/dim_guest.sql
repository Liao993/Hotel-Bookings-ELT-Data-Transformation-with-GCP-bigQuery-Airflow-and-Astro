WITH guest_cte AS (
  SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key([
      "COALESCE(country, 'Unknown')",
      "COALESCE(CAST(is_repeated_guest AS STRING), 'Unknown')",
      "COALESCE(customer_type, 'Unknown')"
    ]) }} AS guest_id,
    COALESCE(country, 'Unknown') AS country,
    is_repeated_guest,
    COALESCE(customer_type, 'Unknown') AS customer_type
  FROM {{ source('bookings', 'raw_bookings') }}
)
SELECT
  g.guest_id,
  COALESCE(c.nicename, 'Unknown') AS country,
  g.is_repeated_guest,
  g.customer_type
FROM guest_cte g
LEFT JOIN {{ source('bookings', 'country') }} c
  ON g.country = c.iso3
