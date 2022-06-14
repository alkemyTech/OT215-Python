import dagfactory
from os.path import abspath
from airflow import DAG

cfg_path = abspath('dag_configs/uai_unlpam.yml')

dag_factory = dagfactory.DagFactory(cfg_path)

dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())
