
  
  create view "weather"."main"."weather_cleaned__dbt_tmp" as (
    with formatted_data as (
    -- Format columns and ensure consistent naming
    select
        Country as country,
        EIC as eic,
        BZN as bzn,
        datetime as date_time, -- Standardized datetime column
        mint as minimum_temperature,
        maxt as maximum_temperature,
        temp as temperature,
        dew as dew_point,
        humidity as relative_humidity,
        wspd as wind_speed,
        wdir as wind_direction,
        precip as precipitation,
        visibility as visibility,
        solarenergy as solar_energy,
        solarradiation as solar_radiation,
        sealevelpressure as sea_level_pressure,
        stationinfo as station_info,
        stationContributions as contributing_stations,
        conditions
    from weather_data 
)

-- Final output: Select all formatted columns
select *
from formatted_data
  );
