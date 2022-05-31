from airflow import DAG
from datetime import  timedelta, datetime
from airflow.operators.dummy import DummyOperator
import logging

logger = logging.getLogger('logger') 
handlerConsola = logging.StreamHandler() 
logger.addHandler(handlerConsola) 
logger.setLevel(logging.DEBUG)
logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s', datefmt='%Y-%m-%d')
logger.info('Mensaje del log')

default_args_dag={
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'universidadesF_dags.py',
    description='Dag vacio sin consulta ni procesamiento',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 5, 25),
    default_args=default_args_dag
) as dag:
    conexion_BD = DummyOperator(task_id='ConexionBaseDatos')
    consultaSQL_UNMoron = DummyOperator(task_id='consultaSQL_UNMoron')
    consultaSQL_UNRC = DummyOperator(task_id='consultaSQL_UNRC')
    procesamientoPandas = DummyOperator(task_id='procesamientoPandas')
    cargaS3 = DummyOperator(task_id='cargaS3')

    conexion_BD >> [consultaSQL_UNMoron, consultaSQL_UNRC ] >> procesamientoPandas >> cargaS3

"""
Los 5 operadores del DAG posteriormente tendran las siguientes funciones:
conexion_BD: Operador para conectarse a la base de datos para realizar las consultas correspondientes
consultasSQL_UMoron y ConsultasSQL_UNRC: Operadores que ejecutaran los archivos SQL, uno para cada una de las dos universidades del grupo F
procesamientoPandas: Operador python que procese datos en Pandas
cargaS3: Operador que cargue los resultados en Amazon S3
"""

    

    


