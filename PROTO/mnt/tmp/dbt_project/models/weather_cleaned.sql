with raw_data as (
    -- Combine weather tables from multiple countries
    select
        'Estonia' as country, -- Add a country identifier
        datetimeStr as datetime_str,
        datetime as date_time,
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
        conditions,
        stationContributions as contributing_stations
    from weather_estonia

    union all

    select
        'Latvia' as country, -- Add a country identifier
        datetimeStr as datetime_str,
        datetime as date_time,
        mint as minimum_temperature,
        maxt as maximum_temperature,
        temp as temperature,
        dew as dew_point,
        humidity as relative_humidity,
        wdir as wind_direction,
        wspd as wind_speed,
        precip as precipitation,
        visibility as visibility,
        solarenergy as solar_energy,
        solarradiation as solar_radiation,
        sealevelpressure as sea_level_pressure,
        stationinfo as station_info,
        conditions,
        stationContributions as contributing_stations
    from weather_latvia
),

-- Cleaned data: Select and rename columns
cleaned_data as (
    select
        country,
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
        solar_energy,
        solar_radiation,
        sea_level_pressure,
        contributing_stations,
        conditions
    from raw_data
)

-- Final output: Select all cleaned data
select *
from cleaned_data
