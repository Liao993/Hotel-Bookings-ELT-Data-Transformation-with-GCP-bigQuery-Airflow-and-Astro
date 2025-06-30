
  
    

    create or replace table `hotel-bookings-de`.`bookings`.`fct_bookings`
      
    
    

    OPTIONS()
    as (
      WITH bookings_cte AS (
  SELECT DISTINCT

    -- Generate date_id
    to_hex(md5(cast(coalesce(cast('arrival_date_year' as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast('arrival_date_month' as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast('arrival_date_day_of_month' as string), '_dbt_utils_surrogate_key_null_') as string))) AS date_id,

    -- Generate hotel_id
    to_hex(md5(cast(coalesce(cast('hotel' as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast('reserved_room_type' as string), '_dbt_utils_surrogate_key_null_') as string))) AS hotel_id,

    -- Generate guest_id (casted boolean needs to be in string)
    to_hex(md5(cast(coalesce(cast(COALESCE(country, 'Unknown') as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(COALESCE(CAST(is_repeated_guest AS STRING), 'Unknown') as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(COALESCE(customer_type, 'Unknown') as string), '_dbt_utils_surrogate_key_null_') as string))) AS guest_id,

    -- Generate booking_details_id
    to_hex(md5(cast(coalesce(cast(COALESCE(market_segment, 'Unknown') as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(COALESCE(distribution_channel, 'Unknown') as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(COALESCE(meal, 'Unknown') as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(COALESCE(deposit_type, 'Unknown') as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(COALESCE(reservation_status, 'Unknown') as string), '_dbt_utils_surrogate_key_null_') as string))) AS booking_details_id,

    -- Generate entity_id
    to_hex(md5(cast(coalesce(cast(COALESCE(CAST(company AS STRING), 'Unknown') as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(COALESCE(CAST(agent AS STRING), 'Unknown') as string), '_dbt_utils_surrogate_key_null_') as string))) AS entity_id,

    -- Generate the final booking_id
    to_hex(md5(cast(coalesce(cast('date_id' as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast('hotel_id' as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast('guest_id' as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast('booking_details_id' as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast('entity_id' as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(CAST(is_canceled AS STRING) as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(CAST(lead_time AS STRING) as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(CAST(stays_in_weekend_nights AS STRING) as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(CAST(stays_in_week_nights AS STRING) as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(CAST(adults AS STRING) as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(CAST(children AS STRING) as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(CAST(babies AS STRING) as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(CAST(booking_changes AS STRING) as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(CAST(previous_cancellations AS STRING) as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(CAST(previous_bookings_not_canceled AS STRING) as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(CAST(days_in_waiting_list AS STRING) as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(CAST(adr AS STRING) as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(CAST(required_car_parking_spaces AS STRING) as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(CAST(total_of_special_requests AS STRING) as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(CAST(reservation_status_date AS STRING) as string), '_dbt_utils_surrogate_key_null_') as string))) AS booking_id,

    -- Measures
    is_canceled,
    lead_time,
    stays_in_weekend_nights,
    stays_in_week_nights,
    adults,
    children,
    babies,
    booking_changes,
    previous_cancellations,
    previous_bookings_not_canceled,
    days_in_waiting_list,
    adr,
    required_car_parking_spaces,
    total_of_special_requests,
    reservation_status_date

  FROM `hotel-bookings-de`.`bookings`.`raw_bookings`
)
SELECT
  booking_id,
  date_id,
  hotel_id,
  guest_id,
  booking_details_id,
  entity_id,
  is_canceled,
  lead_time,
  stays_in_weekend_nights,
  stays_in_week_nights,
  adults,
  children,
  babies,
  booking_changes,
  previous_cancellations,
  previous_bookings_not_canceled,
  days_in_waiting_list,
  adr,
  required_car_parking_spaces,
  total_of_special_requests,
  reservation_status_date
FROM bookings_cte
    );
  