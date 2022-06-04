"""
OT215-60 Sprint 2 A
Configurar un Python Operators, para que extraiga información de la base de datos 
utilizando el .sql disponible en el repositorio base.
"""


from datetime import timedelta, datetime
import logging.config

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.data_extraction import data_extraction


# Logger configuration.
logging.config.fileConfig(f'airflow/dags/scripts/logging.conf')
logger = logging.getLogger('DAG')

# Initial setup.
DEFAULT_ARGS = {
	"retries": 5
}

with DAG(
	default_args=DEFAULT_ARGS,
	dag_id="university_data_extraction",
	description="""
		Data extraction with postgresql for UFLO and UNVM universities, 
		data processing with pandas, 
		data loaded to S3.
		""",
	start_date=datetime(2022, 5, 19),
	schedule_interval=timedelta(hours=1)
) as dag:
	# Python-sql operators configuration.
	task_university_query = PythonOperator(
		task_id="university_data_extraction",
		python_callable=data_extraction,
		op_args={"query_uflo.sql","query_unvm.sql"}
	)

	task_uflo_query