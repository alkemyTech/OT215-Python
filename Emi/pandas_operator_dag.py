"""OT215-78 Sprint 2 C
Crear funcion python con pandas
devuelva txt para cada universidad
"""
from datetime import datetime
from datetime import timedelta
import logging
from logging import raiseExceptions
import pandas as pd 
from pathlib import Path
import os

from airflow.models import DAG
from airflow.operators.python import PythonOperator

#Logger configuration
logger = logging.getLogger('dag_logger')
logging.basicConfig(format='%(asctime)s %(logger)s %(message)s', datefmt='%Y-%m-%d', filename='log_dag.log', 
encoding='utf-8', level=logging.DEBUG)

#parent project path and files, sql directory
path = Path(__file__).resolve().parent.parent

# Retries configuration 
DEFAULT_ARGS = {
    'owner': 'university_dag',
    'email': ['university_dag@example.com'],
    'retries':1,
    'retry_delay': timedelta(minutes=5)
}

# Transformation and save data for each university csv
def transform_data(file_csv1, file_csv2):
    """  
    Transform the data obtained
    from the universities into a dataframe using Pandas library
    save dataframe in text file
    """
    # Get files and read csv codigos_postales
    files =[file_csv1, file_csv2]
    df_codigos_postales= pd.read_csv(str(path) + '/dags/files/codigos_postales.csv')
    logging.info(f"Read codigos_postales csv with success.")
    # Change columns names
    df_codigos_postales.columns = ['postal_code','location']
    # Drop duplicate postal_code rows
    df_codigos_postales= df_codigos_postales.drop_duplicates('postal_code')
    df_codigos_postales.reset_index(inplace=True)

    def clean_data(df_university):
        '''
        Clean generic data no lowercase, no extra spaces and symbols
        format date in year-month-day
        convert age to integer and postal_code to str
        '''
        columns_generic =['university','career', 'first_name', 'last_name' , 'location']
        # format only generic columns repeat  no lowercase, no extra spaces and symbols
        for columns_clean in columns_generic:
            df_university[columns_clean] = df_university[columns_clean].str.strip().str.lower().str.replace("-"," ").str.replace("_"," ")
        
        # clean first name "mr" and "dr" substring 
        df_university['first_name'] = df_university['first_name'].str.replace("mr."," ").str.replace("dr."," ")
        # format email lowercase
        df_university['email'] = df_university['email'].str.strip().str.lower()
        # format gender to male, female
        df_university['gender'] = df_university['gender'].str.replace("f","fem").str.replace("m","male")
        # format inscription_date in year-month-day
        df_university['inscription_date'] = pd.to_datetime(df_university['inscription_date'])
        # format age in integer
        df_university['age'] = pd.to_numeric(df_university['age'], downcast='integer')
        # format postal_code as string
        df_university['postal_code'] = df_university['postal_code'].apply(str)
        logging.info(f"Clean dataframe complete.")

        return df_university

    for university in files:
        df_university = pd.read_csv(university)
        # join location column in university dataframe
        if 'postal_code' in df_university.columns:
            df_university = pd.merge(df_university, df_codigos_postales, on='postal_code')
            clean_data(df_university)
        else:
         # join postal_code column in university dataframe
            df_university['location'] = df_university['location'].str.upper()
            df_university = pd.merge(df_university, df_codigos_postales, on='location')
            clean_data(df_university)
        #save data in txt file
        file_name= str(''.join(list(university))).replace(str(path) + "/dags/files","").replace(".csv","")
        # convert csv in text file
        df_university.to_csv(r'dags/files/' + file_name + '.txt' , header=None, index=False, sep='\t', mode='a')
        logging.info(f"save {university} txt with success.")

 
with DAG(
    dag_id='tranform_txt_universities_dag',
    description='''
                Transform csv data  
                clean dataframes and
                save files in txt
                ''',
    default_args=DEFAULT_ARGS,
    schedule_interval = timedelta(hours=1),
    start_date=datetime(year=2022, month=5, day=24),
    catchup=False
) as dag:
    #transform data config python operator
    transformation_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_kwargs={"file_csv1":str(path) + '/dags/files/uni_jujuy.csv',
                   "file_csv2":str(path) + '/dags/files/uni_palermo.csv'}
    )

#  the complete process ETL dag is extraction_data >> transformation_data
transformation_data