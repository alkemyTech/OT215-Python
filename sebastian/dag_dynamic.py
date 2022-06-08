"""
OT215-100 Sprint 3 A
Arreglar un Dag dinamico para el grupo de universidades A con DAG factory.
"""


from os.path import abspath
import logging.config

import dagfactory


# Logger configuration.
logging.config.fileConfig("airflow/dags/scripts/logging.conf")

cfg_path = abspath("airflow/dags/scripts/config_file.yml")
dag_factory = dagfactory.DagFactory(cfg_path)

dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())