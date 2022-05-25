-- OT215 Sprint 1 C
-- Sql from the National University of Jujuy
-- Obtain the data of the registered people
-- Between 09/01/2020 to 02/01/2021
-- The date is listed in ascending order

SELECT 
    university,
    career,
    inscription_date,
    SPLIT_PART(nombre, ' ', 1) AS first_name,
    SPLIT_PART(nombre, ' ', 2) AS last_name,
    sexo AS gender,
    date_part('year',age(CURRENT_DATE,date(birth_date))) AS age,
    location,
    direccion AS postal_code,
    email
FROM
    jujuy_utn
WHERE
    (inscription_date BETWEEN '2020-09-01' AND '2021-02-01')    
AND
    university = 'universidad nacional de jujuy'
    ORDER BY inscription_date ASC