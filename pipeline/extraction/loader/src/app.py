import os
import boto3
import botocore
import httpx
from threading import Thread

s3 = boto3.client("s3")
bucket = os.environ["S3_BUCKET"]

index_folder = "file_index/new/"
processed_folder = "file_index/processed/"
files_folder = "files/new/"
data_dir = "/tmp/data/"
if not os.path.exists(data_dir):
    os.mkdir(data_dir)
headers = {
    "Host": "archive.sensor.community",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/109.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

def job(func, data):
    for item in data:
        func(item)


def process_in_threads(data: list, func: callable, n_threads: int) -> list:
    if len(data) > n_threads:
        chunk_size = round(len(data)/n_threads)
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    else:
        chunks = [data]
    n_threads = min(n_threads, len(chunks))
    threads = [None for i in range(n_threads)]

    for i in range(n_threads):
        threads[i] = Thread(target=job, args=(func, chunks[i]))
        threads[i].start()

    for i in range(n_threads):
        threads[i].join()

    return [item for sublist in chunks for item in sublist]


def handler(event, context):
    process_in_threads(event["Items"], load_file, 8)


def validate_data(data: list) -> bool:
    """Validate data (check for columns)"""
    columns = ['sensor_id', 'timestamp', 'lat', 'lon', 'location', 'timestamp', 'sensor_type']
    header = data.split("\n")[0]
    header = header.split(";")
    for col in columns:
        if col not in header:
            print(f"Invalid column: {col}")
            return False
    return True


def load_file(link: str):
    """
    link like: https://archive.sensor.community/2015-10-01/2015-10-01_ppd42ns_sensor_27.csv
    """
    resp = httpx.get(link, headers=headers, timeout=120)
    data = resp.content.decode()
    if not validate_data(data):
        print(f"Invalid data: {link}")
        print(data)
        return
    path = data_dir + link.split("/")[-1]
    data_object = files_folder + link.split("/")[-1]
    with open(path, 'w') as f:
        f.write(data)
    try:
        s3.upload_file(path, bucket, data_object)
        return data_object        
    except botocore.exceptions.ClientError as e:
        print(e)
        raise e
    finally:
        os.remove(path)