'''
OT215-110 Sprint 3 C
Arreglar un Dag dinamico para el grupo de universidades C
Se debe arreglar el DAG factory
'''
from airflow import DAG
import dagfactory
from pathlib import Path
import logging
from scripts.logs_dag import log_dag

#parent project path 
path = Path(__file__).resolve().parent.parent

#init logs
log_dag ()

# Config file
config_file = str(path) + '/dags/dynamic_dag.yml'
load_plan = dagfactory.DagFactory(config_file)
logging.info(f"config dag facetory succesfull.")

# Dependencies
load_plan.clean_dags(globals())
load_plan.generate_dags(globals())
logging.info(f"load dag plan succesfull.")
