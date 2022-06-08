from airflow.hooks.S3_hook import S3Hook


def connection_to_s3(filename, key, bucket_name):
	hook = S3Hook("aws_s3")
	hook.load_file(filename=filename, key=key, bucket_name=bucket_name)
