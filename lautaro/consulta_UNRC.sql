SELECT univiersities AS university, 
    carrera AS career, 
    TO_DATE(inscription_dates, 'YY/MON/DD') AS inscription_date, 
    SPLIT_PART(names, '-', 1) AS first_name, 
    SPLIT_PART(names, '-', 2) AS last_name, 
    sexo AS gender, 
    (CURRENT_DATE - TO_DATE(fechas_nacimiento, 'YY/MON/DD')) / 365 AS age, 
    localidad AS location, 
    email
FROM rio_cuarto_interamericana
WHERE TO_DATE(inscription_dates, 'YY/MON/DD') >= '2020-09-01' 
    AND TO_DATE(inscription_dates, 'YY/MON/DD') <= '2021-02-01' 
    AND univiersities = 'Universidad-nacional-de-río-cuarto';