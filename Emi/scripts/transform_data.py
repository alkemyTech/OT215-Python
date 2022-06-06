from pathlib import Path
import os
import logging
from logs_dag import log_dag
import pandas as pd 

#parent project path and files, sql directory
path = Path(__file__).resolve().parent.parent
#init logs
log_dag ()

# Transformation and save data for each university csv
def transform_data(file_csv1, file_csv2):
    """  
    Transform the data obtained
    from the universities into a dataframe using Pandas library
    save dataframe in text file
    """
    # Get files and read csv codigos_postales
    files =[file_csv1, file_csv2]
    df_codigos_postales= pd.read_csv(str(path) + '/files/codigos_postales.csv')
    logging.info(f"Read codigos_postales csv with success.")
    # Change columns names
    df_codigos_postales.columns = ['postal_code','location']
    # Drop duplicate repeat postal_code rows
    df_codigos_postales= df_codigos_postales.drop_duplicates('location')
    df_codigos_postales.reset_index(inplace=True)

    for university in files:
        df_university = pd.read_csv(university, index_col=0)
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
        file_name= str(university).replace( str(path) +"/dags/files","").replace(".csv","")
        # convert csv in text file
        df_university.to_csv(os.path.join(path, file_name+'.txt'), index=True, sep='\t', mode='a')
        logging.info(f"save {file_name}.txt with success.")


def clean_data(df_university):
        '''
        Clean generic data no lowercase, no extra spaces and symbols
        format date in year-month-day
        convert age to integer and postal_code to str
        '''
        # format only strings columns repeat  no lowercase, no extra spaces and symbols
        columns_strings =['university','career', 'first_name', 'last_name' , 'location']
        df_university = process_string_columns(columns_strings, df_university)

        # replace first name "mr" and "dr" substring 
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

def process_string_columns(columns_strings,df_university):
     for columns_clean in columns_strings:
            df_university[columns_clean] = df_university[columns_clean].str.strip().str.lower().str.replace("-"," ").str.replace("_"," ")
            return df_university