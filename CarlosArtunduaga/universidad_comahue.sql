SELECT 
universidad as university,
carrera as career,
fecha_de_inscripcion as inscription_date,
split_part(name, ' ',1) as first_name, 
split_part(name, ' ', 2) as last_name, 
sexo as gender,
(CURRENT_DATE - TO_DATE(fecha_nacimiento, 'YYYY/MM/DD')) / 365 AS age,
codigo_postal as postal_code,
 
correo_electronico as email


FROM public.flores_comahue
where public.flores_comahue.fecha_de_inscripcion BETWEEN '2020-01-09' AND '2021-02-01' and public.flores_comahue.universidad LIKE '%COMAHUE%'  

;