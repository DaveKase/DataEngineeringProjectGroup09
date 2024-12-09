SELECT
    p.*,
    CASE 
        WHEN p.eic_code = '10YPL-AREA-----S' THEN 'PL'
        ELSE w.bzn
    END AS bzn
FROM
    price_data p
LEFT JOIN (
    SELECT DISTINCT eic_code, bzn FROM weather_cleaned
) w
ON
    p.eic_code = w.eic_code
