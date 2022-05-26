SELECT 
universidad as university,
carrera as career,
to_date(fecha_de_inscripcion, 'DD:MOM:YY') as inscription_date,
nombre as first_name, 
sexo as gender,
fecha_nacimiento as age, 
localidad as location, 
email


FROM public.salvador_villa_maria
where  to_date(public.salvador_villa_maria.fecha_de_inscripcion, 'DD:MOM:YY' )>= '2020-01-09' 
AND to_date(public.salvador_villa_maria.fecha_de_inscripcion, 'DD:MOM:YY' )<= '2021-02-01'  
AND public.salvador_villa_maria.universidad   
LIKE '%UNIVERSIDAD_DEL_SALVADOR%'

;

