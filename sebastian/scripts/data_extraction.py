import logging.config
import os

from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd


# University data extraction.
def data_extraction(*args):
	logger = logging.getLogger("DAG")
	try:
		logger.info("Establishing connection to database.")
		pg_hook = PostgresHook(postgres_conn_id="postgre_sql")
		pg_engine = pg_hook.get_sqlalchemy_engine()
		connection = pg_engine.connect()
	except psycopg2.OperationalError:
		logger.error("Wrong connection data.")
	else:
		logger.info("Successful connection.")
		os.makedirs("airflow/dags/files", exist_ok=True)
		for index, file in enumerate(args):
			name_u = file.strip(".sql").split("_")
			with open(f"airflow/dags/sql/{file}", 'r',encoding="utf8") as query:
				df = pd.read_sql_query(query.read(), connection)
				df.to_csv(f"airflow/dags/files/{name_u[1]}_data.csv")
				logger = logging.getLogger(f"{name_u[1].upper()}")
				logger.info(f"{name_u[1].upper()} data extracted successfully.")
