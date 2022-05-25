SELECT universidad AS university, carrerra AS career,
       fechaiscripccion AS inscription_date,
       split_part(nombrre,' ', 1) AS first_name,
       split_part(nombrre,' ', 2) AS last_name, 
       sexo AS gender, 
       date_part('year', age(to_date(nacimiento, 'DD/MM/YYYY')))::int as age,
       codgoposstal AS postal_code,
       eemail AS email
FROM moron_nacional_pampa
WHERE universidad='Universidad nacional de la pampa'
AND to_date(fechaiscripccion, 'DD/MM/YYYY') 
BETWEEN '2020-Sep-01'::date AND '2021-Feb-01'::date;
