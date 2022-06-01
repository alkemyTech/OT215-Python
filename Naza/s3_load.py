from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def file_to_s3(filename, key, bucket_name):
    s3_hook = S3Hook('universities_bucket')
    s3_hook.load_file(filename=filename, key=key, bucket_name=bucket_name)
