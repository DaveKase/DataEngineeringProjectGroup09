
  
  create view "weather"."main"."weather_cleaned__dbt_tmp" as (
    with raw_data as (
    select
        Address as address,
        Date_x20time as date_time,
        Minimum_x20Temperature as minimum_temperature,
        Maximum_x20Temperature as maximum_temperature,
        Temperature as temperature,
        Dew_x20Point as dew_point,
        Relative_x20Humidity as relative_humidity,
        Wind_x20Speed as wind_speed,
        Wind_x20Direction as wind_direction,
        Precipitation as precipitation,
        Visibility as visibility,
        Cloud_x20Cover as cloud_cover,
        Sea_x20Level_x20Pressure as sea_level_pressure,
        Contributing_x20Stations as contributing_stations,
        Latitude as latitude,
        Longitude as longitude,
        Conditions as conditions,
        Country as country
    from "weather_estonia"  -- Directly reference the table in the DuckDB database
),

-- Cleaned data: removing the name column and renaming
cleaned_data as (
    select
        date_time,
        minimum_temperature,
        maximum_temperature,
        temperature,
        dew_point,
        relative_humidity,
        wind_speed,
        wind_direction,
        precipitation,
        visibility,
        cloud_cover,
        sea_level_pressure,
        contributing_stations,
        latitude,
        longitude,
        conditions,
        country
    from raw_data
)

-- Create a view that includes all cleaned data
select *
from cleaned_data
  );
