SELECT
    pr.*,
    CASE 
        WHEN pr.eic_code = '10YPL-AREA-----S' THEN 'PL'
        ELSE w.bzn
    END AS bzn
FROM
    production_data pr
LEFT JOIN (
    SELECT DISTINCT eic_code, bzn FROM weather_cleaned
) w
ON
    pr.eic_code = w.eic_code
