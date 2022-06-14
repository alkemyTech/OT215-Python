from airflow.hooks.S3_hook import S3Hook

def s3CargaUMoron(filename: str, key: str, bucket_name: str) -> None:
    hook=S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

def s3CargaUNRC(filename: str, key: str, bucket_name: str) -> None:
    hook=S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)