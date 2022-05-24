SELECT 
universidad as university,
carrera as career,
fecha_de_inscripcion as inscription_date,
nombre as first_name, 
sexo as gender,
fecha_nacimiento as age, 
localidad as location, 
email


FROM public.salvador_villa_maria
where public.salvador_villa_maria.universidad LIKE '%UNIVERSIDAD_DEL_SALVADOR%'
ORDER BY id ASC 
limit 5;

