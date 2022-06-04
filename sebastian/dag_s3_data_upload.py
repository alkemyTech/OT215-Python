"""
OT215-84 y 85 Sprint 2 A
Configurar el S3 Operator para poder subir el txt creado por el operador de Python.
"""


from datetime import timedelta, datetime
import logging.config

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.data_loaded_s3 import connection_to_s3


# Logger configuration.
logging.config.fileConfig(f'airflow/dags/scripts/logging.conf')
logger = logging.getLogger('DAG')

# Initial setup.
DEFAULT_ARGS = {
	"retries": 5
}

with DAG(
	default_args=DEFAULT_ARGS,
	dag_id="upload_university_data_to_s3",
	description="""
		Data extraction with postgresql for UFLO and UNVM universities, 
		data processing with pandas, 
		data loaded to S3.
		""",
	start_date=datetime(2022, 5, 19),
	schedule_interval=timedelta(hours=1)
) as dag:
	# Python-sql operators configuration.
	task_upload_uflo_to_s3 = PythonOperator(
		task_id="upload_uflo_to_s3",
		python_callable=connection_to_s3,
		op_kwargs={"filename": "airflow/dags/files/uflo_normalized.txt",
					"key": "uflo_normalized.txt",
					"bucket_name": "cohorte-mayo-2820e45d"
		}
	)
	task_upload_unvm_to_s3 = PythonOperator(
		task_id="upload_unvm_to_s3",
		python_callable=connection_to_s3,
		op_kwargs={"filename": "airflow/dags/files/unvm_normalized.txt",
					"key": "unvm_normalized.txt",
					"bucket_name": "cohorte-mayo-2820e45d"
		}
	)

	[task_upload_uflo_to_s3,task_upload_unvm_to_s3]