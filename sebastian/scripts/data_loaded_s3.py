import logging.config

from airflow.hooks.S3_hook import S3Hook


# Data upload to s3.
def connection_to_s3(filename, key, bucket_name):
	logger = logging.getLogger("DAG")
	logger.info("Establishing connection to the cloud.")
	hook = S3Hook("aws_s3")
	logger.info("Successful connection.")
	hook.load_file(filename=filename, key=key, bucket_name=bucket_name)
	logger.info(f"{key} upload successfully.")