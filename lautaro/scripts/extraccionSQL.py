from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas
import os

def sqlUMoron():
    pg_hook = PostgresHook(postgre_conn_id='postgre_sql_univ')
    connection = pg_hook.get_conn()

    with open ('files/sql/consultaSQL_UMoron.sql', 'r') as myfile: 
        dataConsulta = pandas.read_sql_query(myfile.read(), connection)
        os.makedirs('files', exist_ok=True)
        dataConsulta.to_csv('/files/resultQueryUMoron.csv')


def sqlUNRC():
    pg_hook = PostgresHook(postgre_conn_id='postgre_sql_univ')
    connection = pg_hook.get_conn()

    with open ('/sql/consultaSQL_UNRC.sql', 'r') as myfile: 
        dataConsulta = pandas.read_sql_query(myfile.read(), connection)
        os.makedirs('files', exist_ok=True)
        dataConsulta.to_csv('/files/resultQueryUNRC.csv')