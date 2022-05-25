-- OT215 Sprint 1 group C
-- Sql from the University of Palermo
-- Data of the registered people
-- Between 09/01/2020 to 02/01/2021
-- The date is listed in ascending order

SELECT 
    REPLACE(universidad,'_', ' ') AS university,
    careers AS career,
    fecha_de_inscripcion AS inscription_date,
    SPLIT_PART(names, '_', 1) AS first_name,
    SPLIT_PART(names, '_', 2) AS last_name,
    sexo AS gender,
    birth_dates AS year_birth,
    date_part('year',age(CURRENT_DATE,date(birth_dates))) AS age,
    correos_electronicos AS email,
    direcciones AS location,
	codigo_postal AS postal_code                                  
FROM 
    palermo_tres_de_febrero
WHERE
    (date(fecha_de_inscripcion) BETWEEN '01/Sep/20' AND  '01/Feb/21')    
AND
    universidad = '_universidad_de_palermo'
    ORDER BY date(fecha_de_inscripcion) ASC