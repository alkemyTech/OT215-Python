import os

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Dado un .sql y una conexion previamente configurada desde la UI de
# Airflow, nos conectamos a la base de datos, se ejecuta la query
# y el resultado se guarda en .csv en el directorio files
# (Subdirectorio del directorio donde se levanta el server).
def data_extract(sql: str):
    pg_hook = PostgresHook(postgres_conn_id="unlpam_uai")
    pg_engine = pg_hook.get_sqlalchemy_engine()
    conn = pg_engine.connect()
    with open('scripts/' + sql, 'r') as query:
        df = pd.read_sql_query(query.read(), conn)
        os.makedirs('files', exist_ok=True)
        df.to_csv('files/' + sql.split('.')[0] + '.csv')
