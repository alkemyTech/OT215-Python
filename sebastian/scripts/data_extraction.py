import os

from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd


# University data extraction.
def data_extraction(*args):
	pg_hook = PostgresHook(postgres_conn_id="postgre_sql")
	pg_engine = pg_hook.get_sqlalchemy_engine()
	connection = pg_engine.connect()

	os.makedirs("airflow/dags/files", exist_ok=True)
	for index, file in enumerate(args):
		name_u = file.strip(".sql").split("_")
		with open(f"airflow/dags/sql/{file}", 'r',encoding="utf8") as query:
			df = pd.read_sql_query(query.read(), connection)
			df.to_csv(f"airflow/dags/files/{name_u[1]}_data.csv")
