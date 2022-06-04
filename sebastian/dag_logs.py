"""
OT215-44 Sprint 2 A
Configurar logs para Universidad De Flores y Universidad Nacional De Villa María
utilizando la libreria Loggin de python (%Y-%m-%d - nombre_logger - mensaje).
"""

from datetime import timedelta, datetime
import logging.config

from airflow import DAG


# Logger configuration.
logging.config.fileConfig(f'airflow/dags/scripts/logging.conf')
logger = logging.getLogger('DAG')

# Initial setup.
DEFAULT_ARGS = {
	"retries": 5
}

with DAG(
	default_args=DEFAULT_ARGS,
	dag_id='logs_configuration',
	description="""
		Data extraction with postgresql for UFLO and UNVM universities, 
		data processing with pandas, 
		data loaded to S3.
		""",
	start_date=datetime(2022, 5, 19),
	schedule_interval=timedelta(hours=1)
) as dag:
	pass