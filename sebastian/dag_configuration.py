"""
OT215-28 y 36 Sprint 1 A
Configurar un DAG, sin operators y asignar 5 retries.
"""

from datetime import timedelta, datetime
import logging.config

from airflow import DAG

# Logger configuration.
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('DAG')
logger.info("Starting logs.")

# Retries configuration.
DEFAULT_ARGS = {
    "retries": 5,
}

with DAG(
	default_args=DEFAULT_ARGS,
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