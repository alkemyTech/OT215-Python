from airflow.hooks.postgres_hook import PostgresHook


pg_hook = PostgresHook(postgres_conn_id="postgre_sql")
pg_engine = pg_hook.get_sqlalchemy_engine()
conn = pg_engine.connect()