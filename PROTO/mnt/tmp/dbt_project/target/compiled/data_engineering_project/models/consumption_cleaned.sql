SELECT
    c.*,
    CASE 
        WHEN c.eic_code = '10YPL-AREA-----S' THEN 'PL'
        ELSE w.bzn
    END AS bzn
FROM
    consumption_data c
LEFT JOIN (
    SELECT DISTINCT eic_code, bzn FROM weather_cleaned
) w
ON
    c.eic_code = w.eic_code