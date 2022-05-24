SELECT 
universidad as university,
carrera as career,
fecha_de_inscripcion as inscription_date,
name as first_name, 
sexo as gender,
fecha_nacimiento as age,
codigo_postal as postal_code,
direccion as location, 
correo_electronico as email


FROM public.flores_comahue
where public.flores_comahue.fecha_de_inscripcion BETWEEN '2020-01-09 23:00:00' AND '2021-02-01 23:00:00' and public.flores_comahue.universidad LIKE '%COMAHUE%'  

limit 5;