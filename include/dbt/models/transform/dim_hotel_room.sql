-- dim_hotel_room.sql

WITH hotel_cte AS (
  SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['hotel', 'reserved_room_type']) }} AS hotel_room_id,
    hotel AS hotel_name,
    reserved_room_type AS room_type
  FROM {{ source('bookings', 'raw_bookings') }}
  WHERE hotel IS NOT NULL
    AND reserved_room_type IS NOT NULL
)

SELECT
  hotel_room_id,
  hotel_name,
  room_type
FROM hotel_cte
