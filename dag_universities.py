from datetime import timedelta, datetime
from airflow import DAG

default_args = {
    'retries': 5,
}

with DAG(
    default_args=default_args,
    dag_id='analysis_uflo_unvm',
    description="""
         Data extraction with postgresql for UFLO and UNVM universities, 
         data processing with pandas, 
         data loaded to S3.
         """,
    start_date=datetime(2022, 5, 19),
    schedule_interval=timedelta(hours=1)
) as dag:
    pass

# En el fututuro se deberian utilizar los sigueintes operadores:
# 2 sql para realizar las consultas asignadas 
# 1 python para procesar con pandas los datos obtenidos
# 1 GoogleApiToS3Operator para subir los datos a la nube