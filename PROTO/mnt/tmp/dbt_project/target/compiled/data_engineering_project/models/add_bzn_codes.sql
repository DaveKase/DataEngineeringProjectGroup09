-- Add bzn to consumption_data
CREATE OR REPLACE TABLE consumption_cleaned AS
SELECT
    c.*,
    w.bzn
FROM
    consumption_data c
LEFT JOIN
    weather_cleaned w
ON
    c.eic_code = w.eic_code;

-- Add bzn to price_data
CREATE OR REPLACE TABLE price_cleaned AS
SELECT
    p.*,
    w.bzn
FROM
    price_data p
LEFT JOIN
    weather_cleaned w
ON
    p.eic_code = w.eic_code;

-- Add bzn to production_data
CREATE OR REPLACE TABLE production_cleaned AS
SELECT
    pr.*,
    w.bzn
FROM
    production_data pr
LEFT JOIN
    weather_cleaned w
ON
    pr.eic_code = w.eic_code;