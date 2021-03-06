SELECT 
	universidad AS university, 
	carrera AS career, 
	fecha_de_inscripcion AS inscription_date, 
	name AS full_name, 
	sexo AS gender, 
	date_part('year',age(CAST(fecha_nacimiento AS DATE))) AS age, 
	codigo_postal AS postal_code, 
	correo_electronico AS email 
FROM 
	flores_comahue 
WHERE 
	universidad='UNIVERSIDAD DE FLORES' 
	AND fecha_de_inscripcion BETWEEN '2020-09-01' AND '2021-02-01';
