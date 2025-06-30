-- dim_date.sql

WITH date_raw AS (
  SELECT DISTINCT
    arrival_date_year,
    arrival_date_month,
    arrival_date_week_number,
    arrival_date_day_of_month,
    CAST(
      CONCAT(
        arrival_date_year,
        '-',
        LPAD(
          CAST(
            CASE
              WHEN arrival_date_month = 'January' THEN 1
              WHEN arrival_date_month = 'February' THEN 2
              WHEN arrival_date_month = 'March' THEN 3
              WHEN arrival_date_month = 'April' THEN 4
              WHEN arrival_date_month = 'May' THEN 5
              WHEN arrival_date_month = 'June' THEN 6
              WHEN arrival_date_month = 'July' THEN 7
              WHEN arrival_date_month = 'August' THEN 8
              WHEN arrival_date_month = 'September' THEN 9
              WHEN arrival_date_month = 'October' THEN 10
              WHEN arrival_date_month = 'November' THEN 11
              WHEN arrival_date_month = 'December' THEN 12
              ELSE NULL
            END AS STRING
          ),
          2,
          '0'
        ),
        '-',
        LPAD(CAST(arrival_date_day_of_month AS STRING), 2, '0')
      ) AS DATE
    ) AS joined_full_date
  FROM {{ source('bookings', 'raw_bookings') }}
  WHERE arrival_date_year IS NOT NULL
    AND arrival_date_month IS NOT NULL
    AND arrival_date_day_of_month IS NOT NULL
),
date_cte AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['joined_full_date']) }} AS date_id,
    *
  FROM date_raw
)

SELECT
  date_id,
  arrival_date_year,
  arrival_date_month,
  arrival_date_week_number,
  arrival_date_day_of_month,
  FORMAT_DATE('%y/%m/%d', joined_full_date) AS full_date,
  EXTRACT(QUARTER FROM joined_full_date) AS quarter,
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM joined_full_date) IN (1, 7) THEN TRUE
    ELSE FALSE
  END AS is_weekend
FROM date_cte
