from pathlib import Path
import os
import logging
import pandas as pd 
from logs_dag import log_dag

from airflow.hooks.postgres_hook import PostgresHook

#parent project path
path = Path(__file__).resolve().parent.parent
#init logs
log_dag ()

# Extraction data for universities
def extract_data(query1, query2): 
    """  
    config and connection postgres DB training  
    PostgresHook provided connection
    save fie into csv
    """
    queries= [query1, query2]

    try:
        logging.info(f"connection to postgrest database.")
        pg_hook = PostgresHook(
            postgres_conn_id='postgrest',
            schema='training'
        )
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        logging.info(f"connect to db succesfull.")

    except Exception as e:
        logging.error(f"Error connection to database." + e)

    for query in queries:
        #open sql file
        fd = open(query)
        sql_uni_file = fd.read()
        fd.close()
        #execute queries
        cursor.execute(sql_uni_file)
        sql_query = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(data=sql_query, columns=col_names)

        #create directory if not exists
        if not os.path.exists(str(path) + 'dags/files' ):
           os.makedirs(str(path) + 'dags/files/')
        #save file university name
        file_name= str(query).replace( str(path) +"/sql/","").replace(".sql","")
        df.to_csv(os.path.join(path,r'files/'+ file_name+'.csv'))
        logging.info(f"Cvs {file_name} created with success.")

   

