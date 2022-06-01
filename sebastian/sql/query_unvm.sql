SELECT 
	universidad AS university, 
	carrera AS career, 
	to_date(fecha_de_inscripcion, 'DD-Mon-YY') AS inscription_date, 
	nombre AS full_name, 
	sexo AS gender, 
	CASE WHEN date_part('year',to_date(fecha_nacimiento,'DD-Mon-YY'))>2022 
	THEN 100+date_part('year',age(to_date(fecha_nacimiento,'DD-Mon-YY'))) 
	ELSE date_part('year',age(to_date(fecha_nacimiento,'DD-Mon-YY'))) END AS age, 
	localidad AS location, 
	email 
FROM 
	salvador_villa_maria 
WHERE 
	universidad='UNIVERSIDAD_NACIONAL_DE_VILLA_MARÍA' 
	AND to_date(fecha_de_inscripcion, 'DD-Mon-YY') BETWEEN '2020-09-01' AND '2021-02-01';