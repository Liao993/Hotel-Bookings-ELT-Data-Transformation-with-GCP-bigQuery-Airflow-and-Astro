WITH bookings_cte AS (
  SELECT DISTINCT

    -- Generate date_id
    {{ dbt_utils.generate_surrogate_key([
      "'arrival_date_year'",
      "'arrival_date_month'",
      "'arrival_date_day_of_month'"
    ]) }} AS date_id,

    -- Generate hotel_id
    {{ dbt_utils.generate_surrogate_key([
      "'hotel'",
      "'reserved_room_type'"
    ]) }} AS hotel_id,

    -- Generate guest_id (casted boolean needs to be in string)
    {{ dbt_utils.generate_surrogate_key([
      "COALESCE(country, 'Unknown')",
      "COALESCE(CAST(is_repeated_guest AS STRING), 'Unknown')",
      "COALESCE(customer_type, 'Unknown')"
    ]) }} AS guest_id,

    -- Generate booking_details_id
    {{ dbt_utils.generate_surrogate_key([
      "COALESCE(market_segment, 'Unknown')",
      "COALESCE(distribution_channel, 'Unknown')",
      "COALESCE(meal, 'Unknown')",
      "COALESCE(deposit_type, 'Unknown')",
      "COALESCE(reservation_status, 'Unknown')"
    ]) }} AS booking_details_id,

    -- Generate entity_id
    {{ dbt_utils.generate_surrogate_key([
      "COALESCE(CAST(company AS STRING), 'Unknown')",
      "COALESCE(CAST(agent AS STRING), 'Unknown')"
    ]) }} AS entity_id,

    -- Generate the final booking_id
    {{ dbt_utils.generate_surrogate_key([
      "'date_id'",
      "'hotel_id'",
      "'guest_id'",
      "'booking_details_id'",
      "'entity_id'",
      "CAST(is_canceled AS STRING)",
      "CAST(lead_time AS STRING)",
      "CAST(stays_in_weekend_nights AS STRING)",
      "CAST(stays_in_week_nights AS STRING)",
      "CAST(adults AS STRING)",
      "CAST(children AS STRING)",
      "CAST(babies AS STRING)",
      "CAST(booking_changes AS STRING)",
      "CAST(previous_cancellations AS STRING)",
      "CAST(previous_bookings_not_canceled AS STRING)",
      "CAST(days_in_waiting_list AS STRING)",
      "CAST(adr AS STRING)",
      "CAST(required_car_parking_spaces AS STRING)",
      "CAST(total_of_special_requests AS STRING)",
      "CAST(reservation_status_date AS STRING)"
    ]) }} AS booking_id,

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

  FROM {{ source('bookings', 'raw_bookings') }}
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
