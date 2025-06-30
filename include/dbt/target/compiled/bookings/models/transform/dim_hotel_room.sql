-- dim_hotel_room.sql

WITH hotel_cte AS (
  SELECT DISTINCT
    to_hex(md5(cast(coalesce(cast(hotel as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(reserved_room_type as string), '_dbt_utils_surrogate_key_null_') as string))) AS hotel_room_id,
    hotel AS hotel_name,
    reserved_room_type AS room_type
  FROM `hotel-bookings-de`.`bookings`.`raw_bookings`
  WHERE hotel IS NOT NULL
    AND reserved_room_type IS NOT NULL
)

SELECT
  hotel_room_id,
  hotel_name,
  room_type
FROM hotel_cte