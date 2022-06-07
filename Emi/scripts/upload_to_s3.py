import logging
from logs_dag import log_dag

from airflow.hooks.S3_hook import S3Hook

#init logs
log_dag ()

def upload_to_s3(filename, key, bucket_name):
    ''' 
    Configure and load 
    the data to S3 server
    S3Hook provided connection
    upload text file
    '''
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)
    logging.info(f"upload file {key} txt with success.")
