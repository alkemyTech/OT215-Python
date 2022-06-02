''' OT215-89 Sprint 2 C
Tomar el .txt del repositorio base 
Buscar un operador creado por la comunidad que se adecue a los datos.
Configurar el S3 Operator para la Universidad de Palermo
Subir el archivo a S3
'''
from datetime import datetime
from datetime import timedelta
from pathlib import Path
import logging

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

#Logger configuration
logger = logging.getLogger('dag_logger')
logging.basicConfig(format='%(asctime)s %(logger)s %(message)s', datefmt='%Y-%m-%d', filename='log_dag.log', 
encoding='utf-8', level=logging.DEBUG)

#parent project path and files, sql directory
path = Path(__file__).resolve().parent.parent

# Retries configuration 
DEFAULT_ARGS = {
    'owner': 'university_dag',
    'email': ['university_dag@example.com'],
    'retries':1,
    'retry_delay': timedelta(minutes=5)
}

def upload_to_s3(filename, key, bucket_name):
    ''' 
    Configure and load 
    the data to S3 server
    S3Hook provided connection
    upload text file
    '''
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)
    logging.info(f"upload file {key} txt with success.")

with DAG(
    dag_id='S3_load_data_UNJ_dag',
    description='''
                Upload university
                text file to
                amazon s3 service
                ''',
    schedule_interval = timedelta(hours=1),
    start_date=datetime(year=2022, month=5, day=24),
    catchup=False
) as dag:
    # Upload text file UNJ to amazon S3
    load_S3_UNJ = PythonOperator(
        task_id='upload_UNJ_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            "filename":str(path) + '/dags/files/uni_jujuy.txt',
            'key': 'uni_jujuy.txt',
            'bucket_name': 'cohorte-mayo-2820e45d'
        }
    )
     # Upload text file UP to amazon S3
    load_S3_UP = PythonOperator(
        task_id='upload_UP_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            "filename":str(path) + '/dags/files/uni_palermo.txt',
            'key': 'uni_palermo.txt',
            'bucket_name': 'cohorte-mayo-2820e45d'
        }
    )
# the ETL dag is extraction_data >> transformation_data >> load_S3_UNJ >>[load_S3_UNJ,load_S3_UP]
[load_S3_UNJ,load_S3_UP]