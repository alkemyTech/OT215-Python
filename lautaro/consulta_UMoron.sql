SELECT universidad AS university, 
    carrerra AS career, 
    TO_DATE(fechaiscripccion, 'DD/MM/YYYY') AS inscription_date, 
    SPLIT_PART(nombrre, ' ', 1) AS first_name, 
    SPLIT_PART(nombrre, ' ', 2) AS last_name, 
    sexo AS gender, 
    (CURRENT_DATE - TO_DATE(nacimiento, 'DD/MM/YYYY')) / 365 AS age, 
    codgoposstal AS postal_code, 
    eemail AS email
FROM moron_nacional_pampa
WHERE TO_DATE(fechaiscripccion, 'DD/MM/YYYY') >= '2020-09-01' 
    AND TO_DATE(fechaiscripccion, 'DD/MM/YYYY') <= '2021-02-01' 
    AND universidad = 'Universidad de morón';
