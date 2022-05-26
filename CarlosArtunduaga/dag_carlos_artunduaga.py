from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator

with DAG(
    'universidades_B',
    description='Configurar un DAG, sin consultas, ni procesamiento (Documentar los operators y tareas)',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,5,22)
 ) as dag:
    tarea_1 = DummyOperator(task_id='consulta_sql1')
    tarea_2 = DummyOperator(task_id='consulta_sql2')
    tarea_3 = DummyOperator(task_id='proceso_pandas')
    tarea_4 = DummyOperator(task_id='cargar_datos_s3')

    [tarea_1, tarea_2] >>  tarea_3 >> tarea_4


