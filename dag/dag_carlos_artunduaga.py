from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow import DAG
from sqlalchemy import desc
from airflow.operators.dummy import DummyOperator

with DAG(
    'universidades B',
    description='este es un utorial de dags',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022,5,22)
 ) as dag:
    tarea_1 = DummyOperator(task_id='tarea_1')
    tarea_2 = DummyOperator(task_id='tarea_2')
    tarea_3 = DummyOperator(task_id='tarea_3')

    tarea_1 >> [tarea_2 , tarea_3]
