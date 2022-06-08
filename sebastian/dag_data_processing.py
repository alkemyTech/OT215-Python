"""
OT215-68 Sprint 2 A
Configurar el Python Operator para que ejecute las dos funciones que procese los datos.
"""


from datetime import timedelta, datetime
import logging.config

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.data_processing import data_processing


# Logger configuration.
logging.config.fileConfig(f'airflow/dags/scripts/logging.conf')
logger = logging.getLogger('DAG')

# Initial setup.
DEFAULT_ARGS = {
	"retries": 5
}

with DAG(
	default_args=DEFAULT_ARGS,
	dag_id="university_data_processing",
	description="""
		Data extraction with postgresql for UFLO and UNVM universities, 
		data processing with pandas, 
		data loaded to S3.
		""",
	start_date=datetime(2022, 5, 19),
	schedule_interval=timedelta(hours=1)
) as dag:
	# Python-sql operators configuration.
	task_uflo_processing = PythonOperator(
		task_id="uflo_data_processing",
		python_callable=data_processing,
		op_args={"uflo_data.csv","unvm_data.csv"}
	)

	task_uflo_processing