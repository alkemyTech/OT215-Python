import os

import pandas as pd

from scripts.connection_configuration import *


# university data extraction
def data_extraction(file):
	name_u = file.strip(".sql").split("_")
	with open(f"airflow/dags/sql/{file}", 'r',encoding="utf8") as query:
		df = pd.read_sql_query(query.read(), conn)
		os.makedirs("airflow/dags/files", exist_ok=True)
		df.to_csv(f"airflow/dags/files/{name_u[1]}_data.csv")
