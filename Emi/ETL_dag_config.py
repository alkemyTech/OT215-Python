"""
OT215 Sprint 1 C
Documentar los operators que se deberían utilizar a futuro, teniendo en cuenta que se va a hacer 
dos consultas SQL (una para cada universidad), se van a procesar los datos con pandas 
y se van a cargar los datos en S3.  El DAG se debe ejecutar cada 1 hora, todos los días.
"""
from datetime import datetime
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator

def extraction_data():
    """  
    extract queries for each university and
    requested data from the training Postgres database
    """
    pass

def transform_data():
    ''' 
    transform the data obtained
    from the universities into a dataframe using Pandas library
     '''
    pass

def load_data ():
    ''' 
    configure and load the data to S3 server
    '''
    pass

# DAG should be run every 1 hour
with DAG(
    dag_id='ETL_UNJ_UP_dag',
    description='''
                Data extraction with queries db 
                from UNJ and UP, 
                data Processing With Pandas,
                upload data to s3
                ''',
    schedule_interval = timedelta(hours=1),
    start_date=datetime(year=2022, month=5, day=24),
    catchup=False
) as dag:

    # task extraction data from postgres db
    extraction_task = PythonOperator(
        task_id='extraction_data',
        python_callable=extraction_data,
    )

    # task transform data 
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

     # task load data to S3
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

extraction_task >> transform_task >> load_task